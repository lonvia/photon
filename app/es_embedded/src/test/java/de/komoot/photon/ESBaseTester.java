package de.komoot.photon;

import de.komoot.photon.nominatim.model.NameMap;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.PrecisionModel;
import de.komoot.photon.searcher.PhotonResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Start an ES server with some test data that then can be queried in tests that extend this class
 */
public class ESBaseTester {
    @TempDir
    protected Path dataDirectory;

    public static final String TEST_CLUSTER_NAME = "photon-test";
    protected static final GeometryFactory FACTORY = new GeometryFactory(new PrecisionModel(), 4326);

    protected String[] serverLanguages = new String[]{"de", "en", "fr", "es"};
    private ElasticTestServer server;

    protected NameMap makeName(String... kv) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put(kv[i], kv[i + 1]);
        }
        return NameMap.makePlaceNames(map, serverLanguages);
    }

    protected PhotonDoc createDoc(double lon, double lat, int id, int osmId, String key, String value) {
        return new PhotonDoc(id, "W", osmId, key, value)
                .names(makeName("name", "berlin"))
                .centroid(FACTORY.createPoint(new Coordinate(lon, lat)));
    }

    protected PhotonResult getById(int id) {
        return getById(String.valueOf(id));
    }

    protected PhotonResult getById(String id) {
        return server.getById(id);
    }


    @AfterEach
    public void tearDown() throws IOException {
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
    public void setUpES(Path testDirectory, String... languages) throws IOException {
        server = new ElasticTestServer(testDirectory.toString());
        server.start(TEST_CLUSTER_NAME, new String[]{});
        server.recreateIndex(languages, new Date(), false);
        this.serverLanguages = languages;
        refresh();
    }

    protected Importer makeImporter() {
        return server.createImporter(new String[]{});
    }

    protected Importer makeImporterWithExtra(String... extraTags) {
        return server.createImporter(extraTags);
    }

    protected Updater makeUpdater() {
        return server.createUpdater(new String[]{});
    }

    protected Updater makeUpdaterWithExtra(String... extraTags) {
        return server.createUpdater(extraTags);
    }

    protected ElasticTestServer getServer() {
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
