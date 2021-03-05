package de.komoot.photon.searcher;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import de.komoot.photon.Constants;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.List;

/**
 * This is copy over from the method
 * <pre>private List<JSONObject> removeStreetDuplicates(List<JSONObject> results, String lang)</pre>
 * in class {@link de.komoot.photon.App}
 * <p/>
 * Created by Sachin Dole on 2/20/2015.
 */
public class StreetDupesRemover {
    private final String language;

    public StreetDupesRemover(String language) {
        this.language = language;
    }

    public List<JSONObject> execute(List<JSONObject>... allResults) {
        List<JSONObject> results = allResults[0];
        List<JSONObject> filteredItems = Lists.newArrayListWithCapacity(results.size());
        final HashSet<String> keys = Sets.newHashSet();
        for (JSONObject result : results) {
            final JSONObject properties = result.getJSONObject(Constants.PROPERTIES);
            if (properties.has(Constants.OSM_KEY) && "highway".equals(properties.getString(Constants.OSM_KEY))) {
                // result is a street
                if (properties.has(Constants.POSTCODE) && properties.has(Constants.NAME)) {
                    // street has a postcode and name
                    String postcode = properties.getString(Constants.POSTCODE);
                    String name = properties.getString(Constants.NAME);
                    // OSM_VALUE is part of key to avoid deduplication of e.g. bus_stops and streets with same name 
                    String key = (properties.has(Constants.OSM_VALUE) ? properties.getString(Constants.OSM_VALUE) : "") + ":";

                    if (language.equals("nl")) {
                        String onlyDigitsPostcode = stripNonDigits(postcode);
                        key += onlyDigitsPostcode + ":" + name;
                    } else {
                        key += postcode + ":" + name;
                    }

                    if (keys.contains(key)) {
                        // an osm highway object (e.g. street or bus_stop) with this osm_value + name + postcode is already part of the result list
                        continue;
                    }
                    keys.add(key);
                }
            }
            filteredItems.add(result);
        }
        return filteredItems;
    }


    // http://stackoverflow.com/a/4031040/1437096
    private static String stripNonDigits(final CharSequence input) {
        final StringBuilder sb = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); i++) {
            final char c = input.charAt(i);
            if (c > 47 && c < 58) {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
