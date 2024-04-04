package de.komoot.photon.solr;

import de.komoot.photon.DatabaseProperties;
import de.komoot.photon.Importer;
import de.komoot.photon.Updater;
import de.komoot.photon.Utils;
import de.komoot.photon.searcher.ReverseHandler;
import de.komoot.photon.searcher.SearchHandler;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CoreAdminParams;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;

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

        // Empty synonyms and classification lists. This will be filled on update.
        new FileWriter(coreDirectory.resolve("synonyms.txt").toFile()).close();
        var fw = new FileWriter(coreDirectory.resolve("classification.txt").toFile());
        fw.close();

        DatabaseProperties dbProperties = new DatabaseProperties().setLanguages(languages);
        dbProperties.setImportDate(importDate);
        saveToDatabase(dbProperties);

        return dbProperties;
    }

    public void updateIndexSettings(String synonymFile) throws IOException {
        var synFile = new FileWriter(dataDirectory.resolve(coreName).resolve("synonyms.txt").toFile(), false);
        var classFile = new FileWriter(dataDirectory.resolve(coreName).resolve("classification.txt").toFile(), false);

        try {
            if (synonymFile == null) {
                return;
            }

            var synonymConfig = new JSONObject(new JSONTokener(new FileReader(synonymFile)));

            JSONArray synonyms = synonymConfig.optJSONArray("search_synonyms");
            if (synonyms != null) {
                for (int i = 0; i < synonyms.length(); ++i) {
                    synFile.write(synonyms.optString(i, "#"));
                    synFile.write('\n');
                }
            }

            JSONArray classifications = synonymConfig.optJSONArray("classification_terms");
            // Collect for each term in the list the possible classification expansions.
            Map<String, Set<String>> collector = new HashMap<>();
            for (int i = 0; i < classifications.length(); i++) {
                JSONObject descr = classifications.getJSONObject(i);

                String classString = Utils.buildClassificationString(descr.getString("key"), descr.getString("value")).toLowerCase();

                if (classString != null) {
                    JSONArray jsonTerms = descr.getJSONArray("terms");
                    for (int j = 0; j < jsonTerms.length(); j++) {
                        String term = jsonTerms.getString(j).toLowerCase().trim();
                        if (term.indexOf(' ') >= 0) {
                            throw new RuntimeException("Syntax error in synonym file: only single word classification terms allowed.");
                        }

                        if (term.length() > 1) {
                            collector.computeIfAbsent(term, k -> new HashSet<>()).add(classString);
                        }
                    }
                }
            }

            // Create the final list of synonyms. A term can expand to any classificator or not at all.
            for (Map.Entry<String, Set<String>> entry : collector.entrySet()) {
                String term = entry.getKey();
                Set<String> classificators = entry.getValue();
                classFile.write(term + " => " + term + "," + String.join(",", classificators));
                classFile.write('\n');
            }
        } finally {
            synFile.close();
            classFile.close();
        }

        try {
            CoreAdminRequest.reloadCore(coreName, getSolrClient());
        } catch (SolrServerException e) {
            throw new RuntimeException("Cannot reload Solr core.");
        }
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
