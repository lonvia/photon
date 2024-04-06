package de.komoot.photon;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static spark.Spark.*;

/**
 * These test connect photon to an already running ES node (setup in ESBaseTester) so that we can directly test the API
 */
class ApiIntegrationTest extends ESBaseTester {
    private static final int LISTEN_PORT = 30234;

    @BeforeEach
    void setUp() throws Exception {
        setUpES();
        Importer instance = makeImporter();
        instance.add(createDoc(13.38886, 52.51704, 1000, 1000, "place", "city").importance(0.6), 0);
        instance.add(createDoc(13.39026, 52.54714, 1001, 1001, "place", "town").importance(0.3), 1);
        instance.finish();
        refresh();

        tearDown();
    }

    @AfterEach
    void shutdown() {
        stop();
        awaitStop();
    }

    void startApp(String... extraArgs) throws Exception {
        String[] baseArgs = new String[]{"-cluster", TEST_CLUSTER_NAME,
                "-listen-port", Integer.toString(LISTEN_PORT),
                "-transport-addresses", "127.0.0.1",
                "-data-dir", dataDirectory.resolve("photon_test_data").toString()};
        App.main(Stream.concat(Arrays.stream(baseArgs), Arrays.stream(extraArgs)).toArray(String[]::new));
        awaitInitialization();
    }

    private HttpURLConnection getURL(String file) throws MalformedURLException, IOException {
        return (HttpURLConnection) new URL("http", "127.0.0.1", port(), file).openConnection();

    }

    /**
     * Test that the Access-Control-Allow-Origin header is not set
     */
    @Test
    void testNoCors() throws Exception {
        startApp();

        HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:" + port() + "/api?q=berlin").openConnection();
        assertNull(connection.getHeaderField("Access-Control-Allow-Origin"));
    }

    /**
     * Test that the Access-Control-Allow-Origin header is set to *
     */
    @Test
    void testCorsAny() throws Exception {
        startApp("-cors-any");

        HttpURLConnection connection = getURL("/api?q=berlin");
        assertEquals("*", connection.getHeaderField("Access-Control-Allow-Origin"));
    }

    /**
     * Test that the Access-Control-Allow-Origin header is set to a specific domain
     */
    @Test
    void testCorsOriginIsSetToSpecificDomain() throws Exception {
        startApp("-cors-origin", "www.poole.ch");

        HttpURLConnection connection = getURL("/api?q=berlin");
        assertEquals("www.poole.ch", connection.getHeaderField("Access-Control-Allow-Origin"));
    }

    @Test
    void testSearchForBerlin() throws Exception {
        startApp();

        var connection = getURL("/api?q=berlin&limit=1");
        assertEquals(200, connection.getResponseCode());

        JSONObject json = new JSONObject(
                new BufferedReader(new InputStreamReader(connection.getInputStream())).lines().collect(Collectors.joining("\n")));
        JSONArray features = json.getJSONArray("features");
        assertEquals(1, features.length());
        JSONObject feature = features.getJSONObject(0);
        JSONObject properties = feature.getJSONObject("properties");
        assertEquals("W", properties.getString("osm_type"));
        assertEquals("place", properties.getString("osm_key"));
        assertEquals("city", properties.getString("osm_value"));
        assertEquals("berlin", properties.getString("name"));
    }

    /**
     * Search with location bias (this should give the last generated object which is roughly 2km away from the first)
     */
    @Test
    void testApiWithLocationBias() throws Exception {
        startApp();

        HttpURLConnection connection = getURL("/api?q=berlin&limit=1&lat=52.54714&lon=13.39026&zoom=16");
        assertEquals(200, connection.getResponseCode());

        JSONObject json = new JSONObject(
                new BufferedReader(new InputStreamReader(connection.getInputStream())).lines().collect(Collectors.joining("\n")));
        JSONArray features = json.getJSONArray("features");
        assertEquals(1, features.length());
        JSONObject feature = features.getJSONObject(0);
        JSONObject properties = feature.getJSONObject("properties");
        assertEquals("W", properties.getString("osm_type"));
        assertEquals("place", properties.getString("osm_key"));
        assertEquals("town", properties.getString("osm_value"));
        assertEquals("berlin", properties.getString("name"));
    }

    /**
     * Search with large location bias
     */
    @Test
    void testApiWithLargerLocationBias() throws Exception {
        startApp();

        HttpURLConnection connection = getURL("/api?q=berlin&limit=1&lat=52.54714&lon=13.39026&zoom=12&location_bias_scale=0.6");
        assertEquals(200, connection.getResponseCode());

        JSONObject json = new JSONObject(
                new BufferedReader(new InputStreamReader(connection.getInputStream())).lines().collect(Collectors.joining("\n")));
        JSONArray features = json.getJSONArray("features");
        assertEquals(1, features.length());
        JSONObject feature = features.getJSONObject(0);
        JSONObject properties = feature.getJSONObject("properties");
        assertEquals("W", properties.getString("osm_type"));
        assertEquals("place", properties.getString("osm_key"));
        assertEquals("city", properties.getString("osm_value"));
        assertEquals("berlin", properties.getString("name"));
    }

    /**
     * Reverse geocode test
     */
    @Test
    void testApiReverse() throws Exception {
        startApp();

        HttpURLConnection connection = getURL("/reverse/?lon=13.38886&lat=52.51704");
        assertEquals(200, connection.getResponseCode());

        JSONObject json = new JSONObject(
                new BufferedReader(new InputStreamReader(connection.getInputStream())).lines().collect(Collectors.joining("\n")));
        JSONArray features = json.getJSONArray("features");
        assertEquals(1, features.length());

        JSONObject feature = features.getJSONObject(0);
        JSONObject properties = feature.getJSONObject("properties");
        assertEquals("W", properties.getString("osm_type"));
        assertEquals("place", properties.getString("osm_key"));
        assertEquals("city", properties.getString("osm_value"));
        assertEquals("berlin", properties.getString("name"));
    }

    @Test
    void testApiStatus() throws Exception {
        startApp();

        HttpURLConnection connection = getURL("/status");
        JSONObject json = new JSONObject(
                new BufferedReader(new InputStreamReader(connection.getInputStream())).lines().collect(Collectors.joining("\n")));
        assertEquals("Ok", json.getString("status"));
        assertEquals("2020-12-04T04:13:09Z", json.getString("import_date"));
    }
}
