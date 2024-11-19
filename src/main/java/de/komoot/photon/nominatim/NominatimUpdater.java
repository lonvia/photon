package de.komoot.photon.nominatim;

import de.komoot.photon.PhotonDoc;
import de.komoot.photon.Updater;
import de.komoot.photon.nominatim.model.*;
import org.locationtech.jts.geom.Geometry;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Importer for updates from a Nominatim database.
 */
public class NominatimUpdater extends NominatimConnector {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(NominatimUpdater.class);

    private static final String TRIGGER_SQL =
            "DROP TABLE IF EXISTS photon_updates;"
            + "CREATE TABLE photon_updates (rel TEXT, place_id BIGINT,"
            + "                             operation TEXT,"
            + "                             indexed_date TIMESTAMP WITH TIME ZONE);"
            + "CREATE OR REPLACE FUNCTION photon_update_func()\n"
            + " RETURNS TRIGGER AS $$\n"
            + "BEGIN\n"
            + "  INSERT INTO photon_updates("
            + "     VALUES (TG_TABLE_NAME, OLD.place_id, TG_OP, statement_timestamp()));"
            + "  RETURN NEW;"
            + "END; $$ LANGUAGE plpgsql;"
            + "CREATE OR REPLACE TRIGGER photon_trigger_update_placex"
            + "   AFTER UPDATE ON placex FOR EACH ROW"
            + "   WHEN (OLD.indexed_status > 0 AND NEW.indexed_status = 0)"
            + "   EXECUTE FUNCTION photon_update_func();"
            + "CREATE OR REPLACE TRIGGER photon_trigger_delete_placex"
            + "   AFTER DELETE ON placex FOR EACH ROW"
            + "   EXECUTE FUNCTION photon_update_func();"
            + "CREATE OR REPLACE TRIGGER photon_trigger_update_interpolation "
            + "   AFTER UPDATE ON location_property_osmline FOR EACH ROW"
            + "   WHEN (OLD.indexed_status > 0 AND NEW.indexed_status = 0)"
            + "   EXECUTE FUNCTION photon_update_func();"
            + "CREATE OR REPLACE TRIGGER photon_trigger_delete_interpolation"
            + "   AFTER DELETE ON location_property_osmline FOR EACH ROW"
            + "   EXECUTE FUNCTION photon_update_func()";

    private Updater updater;

    private final RowMapper<NominatimResult> osmlineToNominatimResult;
    private final String osmlineRowSql;
    private final RowMapper<NominatimResult> placeToNominatimResult;


    /**
     * Lock to prevent thread from updating concurrently.
     */
    private final ReentrantLock updateLock = new ReentrantLock();


    private final NominatimAddressCache addressCache;


    public NominatimUpdater(String host, int port, String database, String username, String password) {
        this(host, port, database, username, password, new PostgisDataAdapter());
    }

    public NominatimUpdater(String host, int port, String database, String username, String password, DBDataAdapter dataAdapter) {
        super(host, port, database, username, password, dataAdapter);
        addressCache = new NominatimAddressCache(dataAdapter);

        final var placeRowMapper = new PlaceRowMapper(dbutils);
        placeToNominatimResult = (rs, rowNum) -> {
            PhotonDoc doc = placeRowMapper.mapRow(rs, rowNum);
            final Map<String, String> address = dbutils.getMap(rs, "address");

            assert (doc != null);

            final var addressPlaces = addressCache.getOrLoadAddressList(rs.getString("addresslines"), template);
            if (rs.getInt("rank_search") == 30 && rs.getString("parent_class") != null) {
                addressPlaces.add(0, new AddressRow(
                        dbutils.getMap(rs, "parent_name"),
                        rs.getString("parent_class"),
                        rs.getString("parent_type"),
                        rs.getInt("parent_rank_address")));
            }
            doc.completePlace(addressPlaces);
            doc.address(address); // take precedence over computed address
            doc.setCountry(countryNames.get(rs.getString("country_code")));

            return NominatimResult.fromAddress(doc, address);
        };

        // Setup handling of interpolation table. There are two different formats depending on the Nominatim version.
        // new-style interpolations
        final OsmlineRowMapper osmlineRowMapper = new OsmlineRowMapper();
        osmlineRowSql = osmlineRowMapper.getBaseQuery(dataAdapter, hasNewStyleInterpolation);
        osmlineToNominatimResult = (rs, rownum) -> {
            final PhotonDoc doc = osmlineRowMapper.mapRow(rs, 0);

            final var addressPlaces = addressCache.getOrLoadAddressList(rs.getString("addresslines"), template);
            if (rs.getString("parent_class") != null) {
                addressPlaces.add(0, new AddressRow(
                        dbutils.getMap(rs, "parent_name"),
                        rs.getString("parent_class"),
                        rs.getString("parent_type"),
                        rs.getInt("parent_rank_address")));
            }
            doc.completePlace(addressPlaces);

            doc.setCountry(countryNames.get(rs.getString("country_code")));

            final Geometry geometry = dbutils.extractGeometry(rs, "linegeo");
            if (hasNewStyleInterpolation) {
                return NominatimResult.fromInterpolation(
                        doc, rs.getLong("startnumber"), rs.getLong("endnumber"),
                        rs.getLong("step"), geometry);
            }

            return NominatimResult.fromInterpolation(
                    doc, rs.getLong("startnumber"), rs.getLong("endnumber"),
                    rs.getString("interpolationtype"), geometry);
        };
    }



    public boolean isBusy() {
        return updateLock.isLocked();
    }

    public boolean isSetUpForUpdates() {
        Integer result = template.queryForObject("SELECT count(*) FROM pg_tables WHERE tablename = 'photon_updates'", Integer.class);
        return (result != null) && (result > 0);
    }

    public void setUpdater(Updater updater) {
        this.updater = updater;
    }

    public void initUpdates(String updateUser) {
        LOGGER.info("Creating tracking tables");
        txTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                template.execute(TRIGGER_SQL);
                template.execute("GRANT SELECT, DELETE ON photon_updates TO \"" + updateUser + '"');
            }
        });
    }

    public void update() {
        if (updateLock.tryLock()) {
            try {
                loadCountryNames();
                updateFromPlacex();
                updateFromInterpolations();
                updater.finish();
                LOGGER.info("Finished updating");
            } finally {
                updateLock.unlock();
            }
        } else {
            LOGGER.info("Update already in progress");
        }
    }

    private void updateFromPlacex() {
        LOGGER.info("Starting place updates");
        int updatedPlaces = 0;
        int deletedPlaces = 0;
        for (UpdateRow place : getPlaces("placex")) {
            long placeId = place.getPlaceId();
            int objectId = -1;
            boolean checkForMultidoc = true;

            if (!place.isToDelete()) {
                final List<PhotonDoc> updatedDocs = getByPlaceId(placeId);
                if (updatedDocs != null && !updatedDocs.isEmpty() && updatedDocs.get(0).isUsefulForIndex()) {
                    checkForMultidoc = updatedDocs.get(0).getRankAddress() == 30;
                    ++updatedPlaces;
                    for (PhotonDoc updatedDoc : updatedDocs) {
                            updater.create(updatedDoc, ++objectId);
                    }
                }
            }

            if (objectId < 0) {
                ++deletedPlaces;
                updater.delete(placeId, 0);
                objectId = 0;
            }

            if (checkForMultidoc) {
                while (updater.exists(placeId, ++objectId)) {
                    updater.delete(placeId, objectId);
                }
            }
        }

        LOGGER.info("{} places created or updated, {} deleted", updatedPlaces, deletedPlaces);
    }

    /**
     * Update documents generated from address interpolations.
     */
    private void updateFromInterpolations() {
        // .isUsefulForIndex() should always return true for documents
        // created from interpolations so no need to check them
        LOGGER.info("Starting interpolations");
        int updatedInterpolations = 0;
        int deletedInterpolations = 0;
        for (UpdateRow place : getPlaces("location_property_osmline")) {
            long placeId = place.getPlaceId();
            int objectId = -1;

            if (!place.isToDelete()) {
                final List<PhotonDoc> updatedDocs = getInterpolationsByPlaceId(placeId);
                if (updatedDocs != null) {
                    ++updatedInterpolations;
                    for (PhotonDoc updatedDoc : updatedDocs) {
                        updater.create(updatedDoc, ++objectId);
                    }
                }
            }

            if (objectId < 0) {
                ++deletedInterpolations;
            }

            while (updater.exists(placeId, ++objectId)) {
                updater.delete(placeId, objectId);
            }
        }

        LOGGER.info("{} interpolations created or updated, {} deleted", updatedInterpolations, deletedInterpolations);
    }

    private List<UpdateRow> getPlaces(String table) {
        return txTemplate.execute(status -> {
            List<UpdateRow> results = template.query(dbutils.deleteReturning(
                            "DELETE FROM photon_updates WHERE rel = ?", "place_id, operation, indexed_date"),
                    (rs, rowNum) -> {
                        boolean isDelete = "DELETE".equals(rs.getString("operation"));
                        return new UpdateRow(rs.getLong("place_id"), isDelete, rs.getTimestamp("indexed_date"));
                    }, table);

            // For each place only keep the newest item.
            // Order doesn't really matter because updates of each place are independent now.
            results.sort(Comparator.comparing(UpdateRow::getPlaceId).thenComparing(
                    Comparator.comparing(UpdateRow::getUpdateDate).reversed()));

            ArrayList<UpdateRow> todo = new ArrayList<>();
            long prevId = -1;
            for (UpdateRow row : results) {
                if (row.getPlaceId() != prevId) {
                    prevId = row.getPlaceId();
                    todo.add(row);
                }
            }

            return todo;
        });
    }


    public List<PhotonDoc> getByPlaceId(long placeId) {
        List<NominatimResult> result = template.query(
                PlaceRowMapper.SQL_SELECT +
                        "       parent.class as parent_class, parent.type as parent_type," +
                        "       parent.rank_address as parent_rank_address, parent.name as parent_name, " +
                        dbutils.jsonArrayFromSelect(
                                "address_place_id",
                                "FROM place_addressline pa " +
                                        " WHERE pa.place_id IN (p.place_id, CASE WHEN p.rank_search = 30 THEN coalesce(p.parent_place_id, p.place_id) ELSE p.place_id END) AND isaddress" +
                                        " ORDER BY cached_rank_address DESC, pa.place_id = p.place_id DESC") + " as addresslines" +
                        " FROM placex p LEFT JOIN placex parent ON p.parent_place_id = parent.place_id" +
                        " WHERE p.place_id = ? and p.indexed_status = 0",
                placeToNominatimResult, placeId);

        return result.isEmpty() ? null : result.get(0).getDocsWithHousenumber();
    }


    public List<PhotonDoc> getInterpolationsByPlaceId(long placeId) {
        List<NominatimResult> result = template.query(
                osmlineRowSql + " AND p.place_id = ? and p.indexed_status = 0",
                osmlineToNominatimResult, placeId);

        return result.isEmpty() ? null : result.get(0).getDocsWithHousenumber();
    }
}
