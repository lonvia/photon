package de.komoot.photon.solr;

import de.komoot.photon.ESBaseTester;
import de.komoot.photon.Importer;
import de.komoot.photon.PhotonDoc;
import de.komoot.photon.searcher.PhotonResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SolrImporterTest extends ESBaseTester {

    @BeforeEach
    public void setUp() throws IOException {
        setUpES();
    }

    @Test
    public void testAddSimpleDoc() {
        Importer instance = makeImporterWithExtra("");

        instance.add(new PhotonDoc(1234, "N", 1000, "place", "city")
                .extraTags(Collections.singletonMap("maxspeed", "100")), 123);
        instance.finish();
        refresh();

        PhotonResult response = getById(1234);

        assertNotNull(response);

        assertAll("content",
                () -> assertEquals("N", response.get("osm_type")),
                () -> assertEquals(1000L, response.get("osm_id")),
                () -> assertEquals("place", response.get("osm_key")),
                () -> assertEquals("city", response.get("osm_value")),

                () -> assertNull(response.get("extra"))
        );
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"xx"})
    public void testAddDocWithoutCountry(String countryCode) {
        Importer instance = makeImporterWithExtra("");

        instance.add(new PhotonDoc(1234, "N", 1000, "place", "city")
                        .countryCode(countryCode)
                        .extraTags(Collections.singletonMap("maxspeed", "100")), 123);
        instance.finish();
        refresh();

        PhotonResult response = getById(1234);

        assertNotNull(response);

        assertAll("content",
                () -> assertEquals("N", response.get("osm_type")),
                () -> assertEquals(1000L, response.get("osm_id")),
                () -> assertEquals("place", response.get("osm_key")),
                () -> assertEquals("city", response.get("osm_value")),

                () -> assertNull(response.get("extra"))
        );
    }

    @Test
    public void testSelectedExtraTagsCanBeIncluded() {
        Importer instance = makeImporterWithExtra("maxspeed", "website");

        Map<String, String> extratags = new HashMap<>();
        extratags.put("website", "foo");
        extratags.put("maxspeed", "100 mph");
        extratags.put("source", "survey");

        instance.add(new PhotonDoc(1234, "N", 1000, "place", "city").extraTags(extratags), 123);
        instance.add(new PhotonDoc(1235, "N", 1001, "place", "city")
                .extraTags(Collections.singletonMap("wikidata", "100")), 1234);
        instance.finish();
        refresh();

        PhotonResult response = getById(1234);
        assertNotNull(response);

        Map<String, String> extra = response.getMap("extra");
        assertNotNull(extra);

        assertEquals(2, extra.size());
        assertEquals("100 mph", extra.get("maxspeed"));
        assertEquals("foo", extra.get("website"));

        response = getById(1235);
        assertNotNull(response);

        assertNull(response.get("extra"));
    }
}
