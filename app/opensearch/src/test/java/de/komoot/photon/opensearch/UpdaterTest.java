package de.komoot.photon.opensearch;

import de.komoot.photon.ESBaseTester;
import de.komoot.photon.Importer;
import de.komoot.photon.PhotonDoc;
import de.komoot.photon.Updater;
import de.komoot.photon.searcher.PhotonResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class UpdaterTest extends ESBaseTester {

    @TempDir
    private Path dataDirectory;

    @Test
    void addNameToDoc() throws IOException {
        Map<String, String> names = new HashMap<>();
        names.put("name", "Foo");
        PhotonDoc doc = new PhotonDoc(1234, "N", 1000, "place", "city").names(names);

        setUpES(dataDirectory);
        Importer instance = makeImporter();
        instance.add(Collections.singleton(doc));
        instance.finish();
        refresh();

        names.put("name:en", "Enfoo");
        Updater updater = makeUpdater();
        updater.addOrUpdate(List.of(doc));
        updater.finish();
        refresh();

        PhotonResult response = getById(1234);
        assertNotNull(response);

        Map<String, String> outNames = response.getMap("name");
        assertEquals("Foo", outNames.get("default"));
        assertEquals("Enfoo", outNames.get("en"));
    }

    @Test
    void removeNameFromDoc() throws IOException {
        Map<String, String> names = new HashMap<>();
        names.put("name", "Foo");
        names.put("name:en", "Enfoo");
        PhotonDoc doc = new PhotonDoc(1234, "N", 1000, "place", "city").names(names);

        setUpES(dataDirectory);
        Importer instance = makeImporter();
        instance.add(Collections.singleton(doc));
        instance.finish();
        refresh();

        names.remove("name");
        Updater updater = makeUpdater();
        updater.addOrUpdate(List.of(doc));
        updater.finish();
        refresh();

        PhotonResult response = getById(1234);
        assertNotNull(response);

        Map<String, String> outNames = response.getMap("name");
        assertFalse(outNames.containsKey("default"));
        assertEquals("Enfoo", outNames.get("en"));
    }

    @Test
    void addExtraTagsToDoc() throws IOException {
        Map<String, String> names = new HashMap<>();
        names.put("name", "Foo");
        PhotonDoc doc = new PhotonDoc(1234, "N", 1000, "place", "city").names(names);

        getProperties().setExtraTags(List.of("website"));
        setUpES(dataDirectory);
        Importer instance = makeImporter();
        instance.add(Collections.singleton(doc));
        instance.finish();
        refresh();

        PhotonResult response = getById(1234);
        assertNotNull(response);

        assertNull(response.get("extra"));

        doc.extraTags(Collections.singletonMap("website", "http://site.foo"));
        Updater updater = makeUpdater();
        updater.addOrUpdate(List.of(doc));
        updater.finish();
        refresh();

        response = getById(1234);
        assertNotNull(response);

        Map<String, String> extra = response.getMap("extra");

        assertNotNull(extra);
        assertEquals(Collections.singletonMap("website", "http://site.foo"), extra);
    }

    @Test
    void deleteDoc() throws IOException {
        getProperties().setExtraTags(List.of("website"));
        setUpES(dataDirectory);
        Importer instance = makeImporter();
        instance.add(List.of(
                new PhotonDoc(4432, "N", 100, "building", "yes").houseNumber("34"),
                new PhotonDoc(4432, "N", 100, "building", "yes").houseNumber("35")));
        instance.finish();
        refresh();

        assertNotNull(getById("4432"));
        assertNotNull(getById("4432.1"));

        Updater updater = makeUpdater();
        updater.delete(4432L);
        updater.finish();
        refresh();

        assertNotNull(getById("4432"));
        assertNull(getById("4432.1"));
    }
}