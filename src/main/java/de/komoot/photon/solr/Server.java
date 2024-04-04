package de.komoot.photon.solr;

import de.komoot.photon.DatabaseProperties;
import de.komoot.photon.Importer;
import de.komoot.photon.Updater;
import de.komoot.photon.searcher.ReverseHandler;
import de.komoot.photon.searcher.SearchHandler;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrDocument;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

public class Server {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(Server.class);
        /**
     * Database version created by new imports with the current code.
     *
     * Format must be: major.minor.patch-dev
     *
     * Increase to next to be released version when the database layout
     * changes in an incompatible way. If it is alredy at the next released
     * version, increase the dev version.
     */
    private static final String DATABASE_VERSION = "2.0.0-0";
    public static final String PROPERTY_DOCUMENT_ID = "DATABASE_PROPERTIES";

    private static final String BASE_FIELD = "prop";
    private static final String FIELD_VERSION = "database_version";
    private static final String FIELD_LANGUAGES = "indexed_languages";
    private static final String FIELD_IMPORT_DATE = "import_date";

    private final Path dataDirectory;
    private String coreName;
    private String[] transportAddresses;

    private SolrClient client = null;

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
                client.close();
            } catch (IOException e) {
                LOGGER.error("Closing failed: ", e);
            }
        }
    }

    public DatabaseProperties recreateIndex(String[] languages, Date importDate) throws IOException {
        final Path coreDirectory = dataDirectory.resolve(coreName);

        final File dataAsFile = dataDirectory.toFile();
        if (dataAsFile.exists()) {
            throw new RuntimeException(String.format("Database directory '%s' already exists. Cannot create new database.", dataAsFile));
        }
        dataAsFile.mkdirs();

        copyResourceToFile("solr.xml", dataDirectory.resolve("solr.xml"));

        coreDirectory.resolve("conf").toFile().mkdirs();

        FileWriter propertyWriter = new FileWriter(coreDirectory.resolve("core.properties").toFile());
        propertyWriter.write("name=" + coreName + "\n");
        propertyWriter.close();

        copyResourceToFile("photon_solrconfig.xml", coreDirectory.resolve("solrconfig.xml"));
        copyResourceToFile("schema.xml", coreDirectory.resolve("schema.xml"));



        DatabaseProperties dbProperties = new DatabaseProperties().setLanguages(languages);
        dbProperties.setImportDate(importDate);
        saveToDatabase(dbProperties);

        return dbProperties;
    }

    public void updateIndexSettings(String synonymFile) throws IOException {
        // TODO: that will be fun
    }

    public void saveToDatabase(DatabaseProperties dbProperties) throws IOException  {
        DocumentBuilder builder = new DocumentBuilder()
                .add("id", PROPERTY_DOCUMENT_ID)
                .add(BASE_FIELD, FIELD_VERSION, DATABASE_VERSION)
                .add(BASE_FIELD, FIELD_LANGUAGES, String.join(",", dbProperties.getLanguages()));

        if (dbProperties.getImportDate() != null) {
            builder.add(BASE_FIELD, FIELD_IMPORT_DATE, dbProperties.getImportDate().toInstant().toString());
        }

        try {
            getSolrClient().add(builder.build());
            getSolrClient().commit();
        } catch (SolrServerException e) {
            throw new RuntimeException("Cannot write properties to database.");
        }
    }

    public void loadFromDatabase(DatabaseProperties dbProperties) {
        try {
            SolrDocument document = getSolrClient().getById(PROPERTY_DOCUMENT_ID);

            if (document == null) {
                throw new RuntimeException("Cannot find database properties. Database too old or corrupt?");
            }

            Map<String, String> properties = new SolrResult(document).getMap(BASE_FIELD);

            String version = properties.getOrDefault(FIELD_VERSION, "");
            if (!DATABASE_VERSION.equals(version)) {
                LOGGER.error("Database has incompatible version '{}'. Expected: {}", version, DATABASE_VERSION);
                throw new RuntimeException("Incompatible database.");
            }

            String langString = properties.get(FIELD_LANGUAGES);
            dbProperties.setLanguages(langString == null ? null : langString.split(","));

            String importDateString = properties.getOrDefault(FIELD_IMPORT_DATE, null);
            dbProperties.setImportDate(importDateString == null ? null : Date.from(Instant.parse(importDateString)));
        } catch (SolrServerException e) {
            throw new RuntimeException("Cannot find database properties. Database too old or corrupt?");
        } catch (IOException e) {
            throw new RuntimeException("Cannot access database properties. Database too old or corrupt?");
        }
    }

    public Importer createImporter(String[] languages, String[] extraTags) {
        return new SolrImporter(getSolrClient(), languages, extraTags);
    }

    public Updater createUpdater(String[] languages, String[] extraTags) {
        return new SolrUpdater();
    }

    public SearchHandler createSearchHandler(String[] languages, int queryTimeoutSec) {
        return new SolrSearchHandler(getSolrClient());
    }

    public ReverseHandler createReverseHandler(int queryTimeoutSec) {
        return new SolrReverseHandler(getSolrClient());
    }

    private void copyResourceToFile(String resource, Path output) throws IOException {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(resource);
        Files.copy(is, output);
    }

    protected SolrClient getSolrClient() {
        if (client == null) {
            client = new EmbeddedSolrServer(dataDirectory, coreName);
        }

        return client;
    }
}
