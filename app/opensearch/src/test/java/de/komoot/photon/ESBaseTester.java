package de.komoot.photon;

import de.komoot.photon.nominatim.model.NameMap;
import de.komoot.photon.opensearch.OpenSearchTestServer;
import de.komoot.photon.searcher.PhotonResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ESBaseTester {
    public static final String TEST_CLUSTER_NAME = "photon-test";
    protected static final GeometryFactory FACTORY = new GeometryFactory(new PrecisionModel(), 4326);
    protected String[] serverLanguages = new String[]{"de", "en", "fr", "es"};

    @TempDir
    protected Path dataDirectory;

    private OpenSearchTestServer server;

    protected NameMap makeName(String... kv) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put(kv[i], kv[i + 1]);
        }
        return NameMap.makePlaceNames(map, serverLanguages);
    }

    protected PhotonDoc createDoc(double lon, double lat, int id, int osmId, String key, String value) {
        final var location = FACTORY.createPoint(new Coordinate(lon, lat));
        return new PhotonDoc(id, "W", osmId, key, value)
                .names(makeName("name", "berlin"))
                .centroid(location);
    }

    @AfterEach
    public void tearDown() throws IOException {
        shutdownES();
    }

    protected PhotonResult getById(int id) {
        return getById(Integer.toString(id));
    }

    protected PhotonResult getById(String id) {
        return server.getByID(id);
    }

    public void setUpES() throws IOException {
        setUpES(dataDirectory, "en");
    }

    public void setUpES(Path testDirectory, String... languages) throws IOException {
        this.serverLanguages = languages;
        server = new OpenSearchTestServer(testDirectory.toString());
        server.startTestServer(TEST_CLUSTER_NAME);
        server.recreateIndex(languages, new Date(), true);
        server.refreshIndexes();
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

    protected Server getServer() {
        assert server != null;

        return server;
    }

    protected void refresh() throws IOException {
        server.refreshIndexes();
    }

    /**
     * Shutdown the ES node
     */
    public void shutdownES() throws IOException {
        if (server != null) {
            server.stopTestServer();
        }
    }

}
