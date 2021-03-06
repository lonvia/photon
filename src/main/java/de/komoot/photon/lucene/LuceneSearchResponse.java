package de.komoot.photon.lucene;

import de.komoot.photon.Constants;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class LuceneSearchResponse {
    private static final String[] KEYS_LANG_UNSPEC = {Constants.OSM_ID, Constants.OSM_VALUE, Constants.OSM_KEY, Constants.POSTCODE, Constants.HOUSENUMBER, Constants.COUNTRYCODE, Constants.OSM_TYPE};
    private static final String[] KEYS_LANG_SPEC = {Constants.NAME, Constants.COUNTRY, Constants.CITY, Constants.DISTRICT, Constants.LOCALITY, Constants.STREET, Constants.STATE, Constants.COUNTY};
    private final List<Document> results;

    LuceneSearchResponse(List<Document> results) {
        this.results = results;
    }

    public int getHits() {
        return results.size();
    }


    public List<JSONObject> convertToJSON(String lang) {
        final List<JSONObject> list = new ArrayList<>(getHits());

        for (Document source : results) {
            final JSONObject feature = new JSONObject();
            feature.put(Constants.TYPE, Constants.FEATURE);

            final JSONObject properties = new JSONObject();

            /*for (String key : KEYS_LANG_UNSPEC) {
                if (source.get(key) != null)
                    properties.put(key, source.get(key));
            }

            // language specific properties
            for (String key : KEYS_LANG_SPEC) {
                putLocalized(source, properties, key, lang);
            }

            // place type
            properties.put("type", source.get(Constants.OBJECT_TYPE));*/
            for (IndexableField field : source) {
                properties.put(field.name(), field.stringValue());
            }

            feature.put(Constants.PROPERTIES, properties);
            list.add(feature);
        }

        return list;
    }

    private void putLocalized(Document source, JSONObject json, String key, String lang) {
        final String[] fields = {key + "." + lang, key + ".default"};
        for (String field : fields) {
            if (source.get(field) != null) {
                json.put(key, source.get(field));
                return;
            }
        }
    }
}
