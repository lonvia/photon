package de.komoot.photon.elasticsearch;

import de.komoot.photon.CommandLineArgs;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Helper class to start/stop elasticsearch node and get elasticsearch clients
 *
 * @author felix
 */
@Slf4j
public class Server {
    private RestClient lowLevelRestClient;
    private RestHighLevelClient esClient;

    private String clusterName;

    private File esDirectory;

    private final String[] languages;

    private String transportAddresses;

    private Integer shards = null;

    protected static class MyNode extends Node {
        public MyNode(Settings preparedSettings, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(InternalSettingsPreparer.prepareEnvironment(preparedSettings, null), classpathPlugins);
        }
    }

    public Server(CommandLineArgs args) {
        this(args.getCluster(), args.getDataDirectory(), args.getLanguages(), args.getTransportAddresses());
    }

    public Server(String clusterName, String mainDirectory, String languages, String transportAddresses) {
        try {
            if (SystemUtils.IS_OS_WINDOWS) {
                setupDirectories(new URL("file:///" + mainDirectory));
            } else {
                setupDirectories(new URL("file://" + mainDirectory));
            }
        } catch (Exception e) {
            throw new RuntimeException("Can't create directories: " + mainDirectory, e);
        }
        this.clusterName = clusterName;
        this.languages = languages.split(",");
        this.transportAddresses = transportAddresses;
    }

    public Server start() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("admin", "admin"));

        lowLevelRestClient = RestClient.builder(
                new HttpHost("localhost", 9200))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder
                                .setDefaultCredentialsProvider(credentialsProvider);
                    }
                }).build();
        esClient = new RestHighLevelClient(lowLevelRestClient);

        return this;
    }

    /**
     * stops the elasticsearch node
     */
    public void shutdown() {
        try {
            lowLevelRestClient.close();
        } catch (IOException e) {
            throw new RuntimeException("Error during elasticsearch server shutdown", e);
        }
    }

    /**
     * returns an elasticsearch client
     */
    public RestHighLevelClient getClient() {
        return esClient;
    }

    private void setupDirectories(URL directoryName) throws IOException, URISyntaxException {
        final File mainDirectory = new File(directoryName.toURI());
        final File photonDirectory = new File(mainDirectory, "photon_data");
        this.esDirectory = new File(photonDirectory, "elasticsearch");
        final File pluginDirectory = new File(esDirectory, "plugins");
        final File scriptsDirectory = new File(esDirectory, "config/scripts");
        final File painlessDirectory = new File(esDirectory, "modules/lang-painless");

        for (File directory : new File[]{photonDirectory, esDirectory, pluginDirectory, scriptsDirectory,
                painlessDirectory}) {
            directory.mkdirs();
        }

        // copy script directory to elastic search directory
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();

        Files.copy(loader.getResourceAsStream("modules/lang-painless/antlr4-runtime.jar"),
                new File(painlessDirectory, "antlr4-runtime.jar").toPath(),
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(loader.getResourceAsStream("modules/lang-painless/asm-debug-all.jar"),
                new File(painlessDirectory, "asm-debug-all.jar").toPath(), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(loader.getResourceAsStream("modules/lang-painless/lang-painless.jar"),
                new File(painlessDirectory, "lang-painless.jar").toPath(), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(loader.getResourceAsStream("modules/lang-painless/plugin-descriptor.properties"),
                new File(painlessDirectory, "plugin-descriptor.properties").toPath(),
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(loader.getResourceAsStream("modules/lang-painless/plugin-security.policy"),
                new File(painlessDirectory, "plugin-security.policy").toPath(), StandardCopyOption.REPLACE_EXISTING);

    }

    public void recreateIndex() throws IOException {
        deleteIndex();

        final InputStream indexSettings = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("index_settings.json");
        final Charset utf8Charset = Charset.forName("utf-8");

        JSONObject settings = new JSONObject(IOUtils.toString(indexSettings, utf8Charset));
        if (shards != null) {
            settings.put("index", new JSONObject("{ \"number_of_shards\":" + shards + " }"));
        }

        final InputStream mappings = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("mappings.json");

        String mappingsString = IOUtils.toString(mappings, utf8Charset);
        JSONObject mappingsJSON = new JSONObject(mappingsString);

        // add all langs to the mapping
        mappingsJSON = addLangsToMapping(mappingsJSON);
        log.warn(mappingsJSON.toString(2));

        JSONObject payload = new JSONObject();
        payload.put("settings", settings);
        payload.put("mappings", mappingsJSON.get("place"));

        lowLevelRestClient.performRequest("PUT", PhotonIndex.NAME, Collections.emptyMap(),
                new NStringEntity(payload.toString(), ContentType.APPLICATION_JSON));

        log.info("Mapping created for {}.", PhotonIndex.NAME);
    }

    public void deleteIndex() {
        DeleteRequest request = new DeleteRequest(PhotonIndex.NAME, PhotonIndex.TYPE, "id");

        try {
            esClient.delete(request);
        } catch (IOException e) {
            // ignore
        } catch (ElasticsearchStatusException e) {
            if (e.status().getStatus() != 404) {
                throw e;
            }
        }
    }

    public void waitForReady() throws IOException {
        Map<String, String> parameters = Collections.singletonMap("wait_for_status", "yellow");
        lowLevelRestClient.performRequest("GET", "/_cluster/health", parameters);
    }

    private JSONObject addLangsToMapping(JSONObject mappingsObject) {
        // define collector json strings
        String copyToCollectorString = "{\"type\":\"text\",\"index\":false,\"copy_to\":[\"collector.{lang}\"]}";
        String nameToCollectorString = "{\"type\":\"text\",\"index\":false,\"fields\":{\"ngrams\":{\"type\":\"text\",\"analyzer\":\"index_ngram\"},\"raw\":{\"type\":\"text\",\"analyzer\":\"index_raw\"}},\"copy_to\":[\"collector.{lang}\"]}";
        String collectorString = "{\"type\":\"text\",\"index\":false,\"fields\":{\"ngrams\":{\"type\":\"text\",\"analyzer\":\"index_ngram\"},\"raw\":{\"type\":\"text\",\"analyzer\":\"index_raw\"}},\"copy_to\":[\"collector.{lang}\"]}}},\"street\":{\"type\":\"object\",\"properties\":{\"default\":{\"text\":false,\"type\":\"text\",\"copy_to\":[\"collector.default\"]}";

        JSONObject placeObject = mappingsObject.optJSONObject("place");
        JSONObject propertiesObject = placeObject == null ? null : placeObject.optJSONObject("properties");

        if (propertiesObject != null) {
            for (String lang : languages) {
                // create lang-specific json objects
                JSONObject copyToCollectorObject = new JSONObject(copyToCollectorString.replace("{lang}", lang));
                JSONObject nameToCollectorObject = new JSONObject(nameToCollectorString.replace("{lang}", lang));
                JSONObject collectorObject = new JSONObject(collectorString.replace("{lang}", lang));

                // add language specific tags to the collector
                propertiesObject = addToCollector("city", propertiesObject, copyToCollectorObject, lang);
                propertiesObject = addToCollector("context", propertiesObject, copyToCollectorObject, lang);
                propertiesObject = addToCollector("country", propertiesObject, copyToCollectorObject, lang);
                propertiesObject = addToCollector("state", propertiesObject, copyToCollectorObject, lang);
                propertiesObject = addToCollector("street", propertiesObject, copyToCollectorObject, lang);
                propertiesObject = addToCollector("district", propertiesObject, copyToCollectorObject, lang);
                propertiesObject = addToCollector("locality", propertiesObject, copyToCollectorObject, lang);
                propertiesObject = addToCollector("name", propertiesObject, nameToCollectorObject, lang);

                // add language specific collector to default for name
                JSONObject name = propertiesObject.optJSONObject("name");
                JSONObject nameProperties = name == null ? null : name.optJSONObject("properties");
                if (nameProperties != null) {
                    JSONObject defaultObject = nameProperties.optJSONObject("default");
                    JSONArray copyToArray = defaultObject.optJSONArray("copy_to");
                    copyToArray.put("name." + lang);

                    defaultObject.put("copy_to", copyToArray);
                    nameProperties.put("default", defaultObject);
                    name.put("properties", nameProperties);
                    propertiesObject.put("name", name);
                }

                // add language specific collector
                propertiesObject = addToCollector("collector", propertiesObject, collectorObject, lang);
            }
            placeObject.put("properties", propertiesObject);
            return mappingsObject.put("place", placeObject);
        }

        log.error("cannot add languages to mapping.json, please double-check the mappings.json or the language values supplied");
        return null;
    }

    private JSONObject addToCollector(String key, JSONObject properties, JSONObject collectorObject, String lang) {
        JSONObject keyObject = properties.optJSONObject(key);
        JSONObject keyProperties = keyObject == null ? null : keyObject.optJSONObject("properties");
        if (keyProperties != null) {
            if (!keyProperties.has(lang)) {
                keyProperties.put(lang, collectorObject);
            }
            keyObject.put("properties", keyProperties);
            return properties.put(key, keyObject);
        }
        return properties;
    }

    /**
     * Set the maximum number of shards for the embedded node
     * This typically only makes sense for testing
     *
     * @param shards the maximum number of shards
     * @return this Server instance for chaining
     */
    public Server setMaxShards(int shards) {
        this.shards = shards;
        return this;
    }
}
