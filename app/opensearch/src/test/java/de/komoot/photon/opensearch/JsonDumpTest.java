package de.komoot.photon.opensearch;

import de.komoot.photon.Constants;
import de.komoot.photon.ESBaseTester;
import de.komoot.photon.JsonDumper;
import de.komoot.photon.ReflectionTestUtil;
import de.komoot.photon.nominatim.ImportThread;
import de.komoot.photon.nominatim.NominatimConnector;
import de.komoot.photon.nominatim.NominatimImporter;
import de.komoot.photon.nominatim.testdb.H2DataAdapter;
import de.komoot.photon.nominatim.testdb.PlacexTestRow;
import de.komoot.photon.query.PhotonRequest;
import de.komoot.photon.searcher.PhotonResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class JsonDumpTest extends ESBaseTester {
    private NominatimImporter connector;
    private JdbcTemplate jdbc;

    @BeforeEach
    void setup() {
        final var db = new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .generateUniqueName(true)
                .addScript("/test-schema.sql")
                .build();

        connector = new NominatimImporter(null, 0, null, null, null, new H2DataAdapter());

        jdbc = new JdbcTemplate(db);
        final var txTemplate = new TransactionTemplate(new DataSourceTransactionManager(db));
        ReflectionTestUtil.setFieldValue(connector, NominatimConnector.class, "template", jdbc);
        ReflectionTestUtil.setFieldValue(connector, NominatimConnector.class, "txTemplate", txTemplate);
    }

    private void dumpImportJson(Path dumpFile) throws IOException {
        final Date importDate = Date.from(Instant.now());

        final JsonDumper dumper = new JsonDumper(dumpFile.toString(),
                new String[]{"en", "de"}, new String[]{"wikidata"}, importDate);

        ImportThread importThread = new ImportThread(dumper);
        try {
            for (var country : connector.getCountriesFromDatabase()) {
                connector.readCountry(country, importThread);
            }
        } finally {
            importThread.finish();
        }

        Files.copy(dumpFile, System.out);
    }


    private long readImportFromJson(Path dumpFile, String[] countryCodes, String[] languages, String[] extraTags) throws IOException {
        setUpES();

        final var reader = new OpenSearchJsonImporter(dumpFile.toFile(), (Importer) makeImporter());
        reader.readHeader();
        long numDocuments = reader.readData(countryCodes, languages, extraTags);

        refresh();

        return numDocuments;
    }

    private long readImportFromJson(Path dumpFile) throws IOException {
        return readImportFromJson(dumpFile, new String[]{}, new String[]{"en", "de"}, new String[]{});
    }

    private long readImportFromJsonWithCountries(Path dumpFile, String[] countryCodes) throws IOException {
        return readImportFromJson(dumpFile, countryCodes, new String[]{"en", "de"}, new String[]{});
    }

    @Test
    void testDumpImportSimplePlace(@TempDir Path tempDir) throws IOException {
        new PlacexTestRow("amenity", "cafe")
                .id(1234)
                .osm("N", 5000)
                .name("Spot").name("name:en", "EnSpot").name("name:es", "EsSpot")
                .centroid(45.0, 56.0)
                .addr("city", "Blue")
                .importance(0.3)
                .ranks(26)
                .postcode("AB-45")
                .add(jdbc);

        Path testFile = tempDir.resolve("dump.json");
        dumpImportJson(testFile);
        assertEquals(1, readImportFromJson(testFile));

        var results = getServer().createSearchHandler(new String[]{"en"}, 1).search(new PhotonRequest("Spot", "en"));
        assertEquals(1, results.size());

        PhotonResult response = getById("1234");

        assertNotNull(response);
        assertEquals("N", response.get(Constants.OSM_TYPE));
        assertEquals(5000, response.get(Constants.OSM_ID));
        assertEquals(Map.of("default", "Spot", "en", "EnSpot"), response.getMap(Constants.NAME));
        assertEquals(45.0, response.getCoordinates()[0]);
        assertEquals(56.0, response.getCoordinates()[1]);
        assertEquals(Map.of("default", "Blue"), response.getMap("city"));
        assertEquals(0.3, response.get(Constants.IMPORTANCE));
        assertEquals("AB-45", response.get(Constants.POSTCODE));
    }

    @Test
    void testDumpImportRestrictCountry(@TempDir Path tempDir) throws IOException {
        new PlacexTestRow("amenity", "cafe").id(1000).name("Berlin").country("de").add(jdbc);
        new PlacexTestRow("amenity", "cafe").id(2000).name("Amsterdam").country("nl").add(jdbc);
        new PlacexTestRow("amenity", "cafe").id(3000).name("Chicago").country("us").add(jdbc);

        Path testFile = tempDir.resolve("dump.json");
        dumpImportJson(testFile);
        assertEquals(2, readImportFromJsonWithCountries(testFile, new String[]{"hu", "nl", "us"}));

        assertNull(getById(1000));
        assertNotNull(getById(2000));
        assertNotNull(getById(3000));
    }
}
