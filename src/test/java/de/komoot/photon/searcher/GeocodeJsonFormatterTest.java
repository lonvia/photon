package de.komoot.photon.searcher;

import de.komoot.photon.DBSchemaField;
import org.json.JSONObject;
import org.json.JSONArray;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class GeocodeJsonFormatterTest {

    @Test
    public void testConvertToGeojson() {
        GeocodeJsonFormatter formatter = new GeocodeJsonFormatter(false, "en");
        List<PhotonResult> allResults = new ArrayList<>();
        allResults.add(createDummyResult("99999", "Park Foo", "leisure", "park"));
        allResults.add(createDummyResult("88888", "Bar Park", "leisure", "park"));

        String geojsonString = formatter.convert(allResults, null);
        JSONObject jsonObj = new JSONObject(geojsonString);
        assertEquals("FeatureCollection", jsonObj.getString("type"));
        JSONArray features = jsonObj.getJSONArray("features");
        assertEquals(2, features.length());
        for (int i = 0; i < features.length(); i++) {
            JSONObject feature = features.getJSONObject(i);
            assertEquals("Feature", feature.getString("type"));
            assertEquals("Point", feature.getJSONObject("geometry").getString("type"));
            assertEquals("leisure", feature.getJSONObject("properties").getString("osm_key"));
            assertEquals("park", feature.getJSONObject("properties").getString("osm_value"));
        }
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
