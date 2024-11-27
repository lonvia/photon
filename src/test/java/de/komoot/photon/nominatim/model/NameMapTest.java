package de.komoot.photon.nominatim.model;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class NameMapTest {

    @Test
    void testMakeAddressNames() {
        NameMap map = NameMap.makeAddressNames(Map.of(
                "name", "foo",
                "name:en", "bar",
                "name:fr", "belle",
                "alt_name", "A4"), new String[]{"de", "fr"});

        assertEquals(2, map.size());
        assertEquals("foo", map.get("default"));
        assertEquals("belle", map.get("fr"));
    }

    @Test
    void testMakePlaceNames() {
        NameMap map = NameMap.makePlaceNames(Map.of(
                "name", "foo",
                "name:en", "bar",
                "name:fr", "belle",
                "alt_name", "A4"), new String[]{"de", "fr"});

        assertEquals(3, map.size());
        assertEquals("foo", map.get("default"));
        assertEquals("belle", map.get("fr"));
        assertEquals("A4", map.get("alt"));
    }

    @Test
    void testPreferNamesFromPlaceNodes() {
        NameMap map = NameMap.makeAddressNames(Map.of(
                "name", "foo",
                "_place_name", "bar",
                "_place_name:fr", "belle",
                "alt_name", "A4"), new String[]{"de", "fr"});

        assertEquals(Map.of("default", "bar", "fr", "belle"), map);
    }

    @Test
    void testCopyWithReplacementExisting() {
        NameMap map = NameMap.makePlaceNames(Map.of(
                "name", "foo",
                "name:en", "bar"), new String[]{"en"});

        assertEquals(Map.of("default", "foo", "en", "bar"), map);

        var newmap = map.copyWithReplacement("default", "somethingelse");

        assertEquals(Map.of("default", "foo", "en", "bar"), map);
        assertEquals(Map.of("default", "somethingelse", "en", "bar"), newmap);
    }


    @Test
    void testCopyWithReplacementNotExisting() {
        NameMap map = NameMap.makePlaceNames(Map.of(
                "name", "foo",
                "name:en", "bar"), new String[]{"en"});

        assertEquals(Map.of("default", "foo", "en", "bar"), map);

        var newmap = map.copyWithReplacement("alt", "somethingelse");

        assertEquals(Map.of("default", "foo", "en", "bar"), map);
        assertEquals(Map.of("default", "foo", "alt", "somethingelse", "en", "bar"), newmap);
    }
}
