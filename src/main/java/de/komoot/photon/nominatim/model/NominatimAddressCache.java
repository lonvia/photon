package de.komoot.photon.nominatim.model;

import de.komoot.photon.nominatim.DBDataAdapter;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.util.*;

/**
 * Container for caching information about address parts.
 */
public class NominatimAddressCache {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(NominatimAddressCache.class);

    private static final String SQL_SELECT =
            "SELECT place_id, name, class, type, rank_address FROM placex";
    private static final String SQL_COUNTRY_WHERE =
            " WHERE rank_address between 5 and 25 AND linked_place_id is null";

    private final Map<Long, AddressRow> addresses = new HashMap<>();
    private final RowCallbackHandler rowMapper;

    public NominatimAddressCache(DBDataAdapter dbutils, String[] languages) {
        this.rowMapper = rs ->
                addresses.put(
                        rs.getLong("place_id"),
                        AddressRow.makeRow(
                                dbutils.getMap(rs, "name"),
                                rs.getString("class"),
                                rs.getString("type"),
                                rs.getInt("rank_address"),
                                languages
                        ));
    }


    public void loadCountryAddresses(JdbcTemplate template, String countryCode) {
        if ("".equals(countryCode)) {
            template.query(SQL_SELECT + SQL_COUNTRY_WHERE + " AND country_code is null", rowMapper);
        } else {
            template.query(SQL_SELECT + SQL_COUNTRY_WHERE + " AND country_code = ?", rowMapper, countryCode);
        }

        if (addresses.size() > 0) {
            LOGGER.info("Loaded {} address places for country {}", addresses.size(), countryCode);
        }
    }

    public AddressRowList getAddressList(String addressline) {
        if (addressline == null || addressline.isBlank()) {
            return new AddressRowList();
        }

        return makeAddressList(new JSONArray(addressline));
    }

    public AddressRowList getOrLoadAddressList(String addressline, JdbcTemplate template) {
        if (addressline == null || addressline.isBlank()) {
            return new AddressRowList();
        }

        final JSONArray addressPlaces = new JSONArray(addressline);

        // Find any missing places.
        final List<Long> missing = new ArrayList<>();
        for (int i = 0; i < addressPlaces.length(); ++i) {
            final long placeId = addressPlaces.optLong(i);
            if (placeId > 0 && !addresses.containsKey(placeId)) {
                missing.add(placeId);
            }
        }

        if (!missing.isEmpty()) {
            template.query(SQL_SELECT + " WHERE place_id = ANY(?)",
                           rowMapper, (Object) missing.toArray(new Long[0]));
        }

        return makeAddressList(addressPlaces);
    }

    private AddressRowList makeAddressList(JSONArray addressPlaces) {
        final AddressRowList outlist = new AddressRowList();

        for (int i = 0; i < addressPlaces.length(); ++i) {
            final long placeId = addressPlaces.optLong(i);
            if (placeId > 0) {
                final AddressRow row = addresses.get(placeId);
                if (row != null) {
                    outlist.set(row);
                }
            }
        }

        return outlist;
    }
}
