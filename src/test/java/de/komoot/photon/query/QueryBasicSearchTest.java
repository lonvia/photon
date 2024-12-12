package de.komoot.photon.query;

import de.komoot.photon.ESBaseTester;
import de.komoot.photon.Importer;
import de.komoot.photon.PhotonDoc;
import de.komoot.photon.searcher.PhotonResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that the database backend produces queries which can find all
 * expected results. These tests do not check relevance.
 */
class QueryBasicSearchTest extends ESBaseTester {
    private int testDocId = 10000;

    @BeforeEach
    void setup() throws IOException {
        setUpES();
    }

    private PhotonDoc createDoc(String... names) {
        ++testDocId;
        return new PhotonDoc(testDocId, "N", testDocId, "place", "city")
                .names(makeName(names));
    }

    private List<PhotonResult> search(String query) {
        return getServer().createSearchHandler(new String[]{"en"}, 1).search(new PhotonRequest(query, "en"));
    }


    @Test
    void testSearchByDefaultName() throws IOException {
        Importer instance = makeImporter();
        instance.add(createDoc("name", "Muffle Flu"), 0);
        instance.finish();
        refresh();

        assertAll("default name",
                () -> assertEquals(1, search("muffle flu").size()),
                () -> assertEquals(1, search("flu").size()),
                () -> assertEquals(1, search("muffle").size()),
                () -> assertEquals(1, search("mufle flu").size()),
                () -> assertEquals(1, search("muffle flu 9").size()),
                () -> assertEquals(0, search("huffle fluff").size())
        );
    }

    @Test
    void testSearchNameSkipTerms() throws IOException {
        Importer instance = makeImporter();
        instance.add(createDoc("name", "Hunted House Hotel"), 0);
        instance.finish();
        refresh();

        assertAll("default name",
                () -> assertEquals(1, search("hunted").size()),
                () -> assertEquals(1, search("hunted hotel").size()),
                () -> assertEquals(1, search("hunted house hotel").size()),
                () -> assertEquals(1, search("hunted house hotel 7").size()),
                () -> assertEquals(1, search("hunted hotel 7").size())
        );
    }
    @Test
    void testSearchByAlternativeNames() throws IOException {
        Importer instance = makeImporter();
        instance.add(createDoc("name", "original", "alt_name", "alt", "old_name", "older", "int_name", "int",
                               "loc_name", "local", "reg_name", "regional", "addr:housename", "house",
                               "other_name", "other"), 0);
        instance.finish();
        refresh();

        assertAll("altnames",
                () -> assertEquals(1, search("original").size()),
                () -> assertEquals(1, search("alt").size()),
                () -> assertEquals(1, search("older").size()),
                () -> assertEquals(1, search("int").size()),
                () -> assertEquals(1, search("local").size()),
                () -> assertEquals(1, search("regional").size()),
                () -> assertEquals(1, search("house").size()),
                () -> assertEquals(0, search("other").size())
        );
    }

    @Test
    void testSearchByNameAndAddress() throws IOException {
        Map<String, String> address = new HashMap<>();
        address.put("street", "Callino");
        address.put("city", "Madrid");
        address.put("suburb", "Quartier");
        address.put("neighbourhood", "El Block");
        address.put("county", "Montagña");
        address.put("state", "Estado");

        Importer instance = makeImporter();
        instance.add(createDoc("name", "Castillo").completeAddress(null, address), 0);
        instance.finish();
        refresh();

        assertAll("name and address",
                () -> assertEquals(1, search("castillo").size()),
                () -> assertEquals(1, search("castillo callino").size()),
                () -> assertEquals(1, search("castillo quartier madrid").size()),
                () -> assertEquals(1, search("castillo block montagna estado").size()),

                () -> assertEquals(0, search("castillo state thing").size())
        );
    }

    @Test
    void testSearchMustContainANameTerm() throws IOException {
        Importer instance = makeImporter();
        instance.add(createDoc("name", "Palermo").completeAddress(null, Map.of("state", "Sicilia")), 0);
        instance.finish();
        refresh();

        assertAll("find names",
                () -> assertEquals(1, search("Palermo").size()),
                () -> assertEquals(1, search("Paler").size()),
                () -> assertEquals(1, search("Palermo Sici").size()),
                () -> assertEquals(1, search("Sicilia, Paler").size()),
                () -> assertEquals(0, search("Sicilia").size()),
                () -> assertEquals(0, search("Sici").size())
        );
    }

    @Test
    void testSearchWithHousenumberNamed() throws IOException {
        Importer instance = makeImporter();
        instance.add(createDoc("name", "Edeka").houseNumber("5").completeAddress(null, Map.of("street", "Hauptstrasse")), 0);
        instance.finish();
        refresh();

        assertAll("named housenumber",
                () -> assertEquals(1, search("hauptstrasse 5").size()),
                () -> assertEquals(1, search("edeka, hauptstrasse 5").size()),
                () -> assertEquals(1, search("edeka, hauptstr 5").size()),
                () -> assertEquals(1, search("edeka, hauptstrasse").size())
        );
    }

    @Test
    void testSearchWithHousenumberUnnamed() throws IOException {
        Importer instance = makeImporter();
        instance.add(createDoc()
                    .houseNumber("5")
                    .completeAddress(null, Map.of("street", "Hauptstrasse")),
                0);
        instance.finish();
        refresh();

        assertAll("unnamed housenumber",
                () -> assertEquals(1, search("hauptstrasse 5").size()),
                () -> assertEquals(0, search("hauptstrasse").size())
        );
    }
}
