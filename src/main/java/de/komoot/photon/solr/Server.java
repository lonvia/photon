package de.komoot.photon.solr;

import de.komoot.photon.DatabaseProperties;
import de.komoot.photon.Importer;
import de.komoot.photon.Updater;
import de.komoot.photon.searcher.ReverseHandler;
import de.komoot.photon.searcher.SearchHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
public class Server {
        /**
     * Database version created by new imports with the current code.
     *
     * Format must be: major.minor.patch-dev
     *
     * Increase to next to be released version when the database layout
     * changes in an incompatible way. If it is alredy at the next released
     * version, increase the dev version.
     */
    private static final String DATABASE_VERSION = "1.0.0-0";
    public static final String PROPERTY_DOCUMENT_ID = "DATABASE_PROPERTIES";

    private static final String BASE_FIELD = "document_properties";
    private static final String FIELD_VERSION = "database_version";
    private static final String FIELD_LANGUAGES = "indexed_languages";

    private final Path dataDirectory;
    private String coreName;
    private String[] transportAddresses;

    private SolrClient client;

    public Server(String mainDirectory) {
        dataDirectory = Paths.get(mainDirectory).toAbsolutePath();
    }

    public Server start(String coreName, String[] transportAddresses) {
        this.coreName = coreName;
        this.transportAddresses = transportAddresses;

        return this;
    }

    public void waitForReady() {
        // TODO
    }

    public void shutdown() {
        if (client != null) {
            try {
                client.commit();
                client.close();
            } catch (SolrServerException e) {
                log.warn("Error while closing client: " + e.getMessage());
            } catch (IOException e) {
                log.warn("Error while closing client: " + e.getMessage());
            }
        }
    }

    public DatabaseProperties recreateIndex(String[] languages) throws IOException {
        final Path coreDirectory = dataDirectory.resolve(coreName);

        final File dataAsFile = dataDirectory.toFile();
        if (dataAsFile.exists()) {
            throw new RuntimeException(String.format("Database directory '%s' already exists. Cannot create new database.", dataAsFile));
        }
        dataAsFile.mkdirs();

        copyResourceToFile("solr.xml", dataDirectory.resolve("solr.xml"));

        coreDirectory.toFile().mkdirs();
        copyResourceToFile("photon_core.properties", coreDirectory.resolve("core.properties"));
        copyResourceToFile("photon_solrconfig.xml", coreDirectory.resolve("solrconfig.xml"));
        copyResourceToFile("schema.xml", coreDirectory.resolve("schema.xml"));

        DatabaseProperties dbProperties = new DatabaseProperties().setLanguages(languages);
        saveToDatabase(dbProperties);

        return dbProperties;
    }

    public void updateIndexSettings(String synonymFile) throws IOException {
        // TODO: that will be fun
    }

    public Server setMaxShards(Integer shards) {
        // nothing, we don't care about shards
        return this;
    }

    public void saveToDatabase(DatabaseProperties dbProperties) throws IOException  {
        // TODO: needs doing
    }

    public void loadFromDatabase(DatabaseProperties dbProperties) {
        // TODO: need the table stuff first
    }

    public Importer createImporter(String[] languages, String[] extraTags) {
        return new SolrImporter(getSolrClient(), languages, extraTags);
    }

    public Updater createUpdater(String[] languages, String[] extraTags) {
        return new SolrUpdater();
    }

    public SearchHandler createSearchHandler(String[] languages) {
        return new SolrSearchHandler();
    }

    public ReverseHandler createReverseHandler() {
        return new SolrReverseHandler();
    }

    private void copyResourceToFile(String resource, Path output) throws IOException {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(resource);
        Files.copy(is, output);
    }

    private SolrClient getSolrClient() {
        return new EmbeddedSolrServer(dataDirectory, coreName);
    }
}
