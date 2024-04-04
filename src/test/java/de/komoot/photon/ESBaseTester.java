package de.komoot.photon;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.PrecisionModel;
import de.komoot.photon.solr.SolrTestServer;
import de.komoot.photon.searcher.PhotonResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.TimeZone;

/**
 * Start an ES server with some test data that then can be queried in tests that extend this class
 */
public class ESBaseTester {
    @TempDir
    protected Path dataDirectory;

    public static final String TEST_CLUSTER_NAME = "photon-test";
    protected static GeometryFactory FACTORY = new GeometryFactory(new PrecisionModel(), 4326);

    private SolrTestServer server;

    protected PhotonDoc createDoc(double lon, double lat, int id, int osmId, String key, String value) {
        Point location = FACTORY.createPoint(new Coordinate(lon, lat));
        return new PhotonDoc(id, "W", osmId, key, value).names(Collections.singletonMap("name", "berlin")).centroid(location);
    }

    protected PhotonResult getById(String id) {
        return getById(Integer.parseInt(id));
    }

    protected PhotonResult getById(int id) {
        return server.getById(id);
    }


    @AfterEach
    public void tearDown() {
        shutdownES();
    }

    public void setUpES() throws IOException {
        setUpES(dataDirectory, "en");
    }
    /**
     * Setup the ES server
     *
     * @throws IOException
     */
    public void setUpES(Path test_directory, String... languages) throws IOException {
        server = new SolrTestServer(test_directory.resolve("photon_test_data").toString());
        server.start(TEST_CLUSTER_NAME, new String[]{});
        server.recreateIndex(languages, new Calendar.Builder()
                                            .setDate(2020, 11,4)
                                            .setTimeOfDay(4, 13, 9)
                                            .setTimeZone(TimeZone.getTimeZone("UTC")).build().getTime());
        refresh();
    }

    protected Importer makeImporter() {
        return server.createImporter(new String[]{"en"}, new String[]{});
    }

    protected Importer makeImporterWithExtra(String... extraTags) {
        return server.createImporter(new String[]{"en"}, extraTags);
    }

    protected Importer makeImporterWithLanguages(String... languages) {
        return server.createImporter(languages, new String[]{});
    }

    protected Updater makeUpdater() {
        return server.createUpdater(new String[]{"en"}, new String[]{});
    }

    protected Updater makeUpdaterWithExtra(String... extraTags) {
        return server.createUpdater(new String[]{"en"}, extraTags);
    }

    protected SolrTestServer getServer() {
        if (server == null) {
            throw new RuntimeException("call setUpES before using getClient");
        }

        return server;
    }

    protected void refresh() {
        server.refresh();
    }

    /**
     * Shutdown the ES node
     */
    public void shutdownES() {
        if (server != null)
            server.shutdown();
    }
}
