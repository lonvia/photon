package de.komoot.photon.searcher;

import de.komoot.photon.DBSchemaField;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class StreetDupesRemoverTest {

    @Test
    public void testDeduplicatesStreets() {
        StreetDupesRemover streetDupesRemover = new StreetDupesRemover("en");
        List<PhotonResult> allResults = new ArrayList<>();
        allResults.add(createDummyResult("99999", "Main Street", "highway", "Unclassified"));
        allResults.add(createDummyResult("99999", "Main Street", "highway", "Unclassified"));

        List<PhotonResult> dedupedResults = streetDupesRemover.execute(allResults);
        assertEquals(1, dedupedResults.size());
    }

    @Test
    public void testStreetAndBusStopNotDeduplicated() {
        StreetDupesRemover streetDupesRemover = new StreetDupesRemover("en");
        List<PhotonResult> allResults = new ArrayList<>();
        allResults.add(createDummyResult("99999", "Main Street", "highway", "bus_stop"));
        allResults.add(createDummyResult("99999", "Main Street", "highway", "Unclassified"));

        List<PhotonResult> dedupedResults = streetDupesRemover.execute(allResults);
        assertEquals(2, dedupedResults.size());
    }
    
    private PhotonResult createDummyResult(String postCode, String name, String osmKey,
                    String osmValue) {
        return new MockPhotonResult()
                .put(DBSchemaField.POSTCODE, postCode)
                .putLocalized(DBSchemaField.NAME, "en", name)
                .put(DBSchemaField.OSM_KEY, osmKey)
                .put(DBSchemaField.OSM_VALUE, osmValue);
    }

}
