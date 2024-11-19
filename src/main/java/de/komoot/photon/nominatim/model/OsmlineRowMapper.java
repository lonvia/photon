package de.komoot.photon.nominatim.model;

import de.komoot.photon.PhotonDoc;
import de.komoot.photon.nominatim.DBDataAdapter;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class OsmlineRowMapper implements RowMapper<PhotonDoc> {
    @Override
    public PhotonDoc mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new PhotonDoc(
                rs.getLong("place_id"),
                "W", rs.getLong("osm_id"),
                "place", "house_number")
                .parentPlaceId(rs.getLong("parent_place_id"))
                .countryCode(rs.getString("country_code"))
                .postcode(rs.getString("postcode"));
    }

    public String getBaseQuery(DBDataAdapter dbutils, boolean hasNewStyleInterpolation) {
        return "SELECT p.place_id, p.osm_id, p.parent_place_id, p.startnumber, p.endnumber, p.postcode, p.country_code, p.linegeo," +
                        (hasNewStyleInterpolation ? " p.step," : " p.interpolationtype,") +
                        "       parent.class as parent_class, parent.type as parent_type," +
                        "       parent.rank_address as parent_rank_address, parent.name as parent_name, " +
                        dbutils.jsonArrayFromSelect(
                                "address_place_id",
                                "FROM place_addressline pa " +
                                        " WHERE pa.place_id IN (p.place_id, coalesce(p.parent_place_id, p.place_id)) AND isaddress" +
                                        " ORDER BY cached_rank_address DESC, pa.place_id = p.place_id DESC") + " as addresslines" +
                        " FROM location_property_osmline p LEFT JOIN placex parent ON p.parent_place_id = parent.place_id" +
                        " WHERE startnumber is not null";
    }
}