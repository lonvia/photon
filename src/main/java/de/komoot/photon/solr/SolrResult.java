package de.komoot.photon.solr;

import de.komoot.photon.searcher.PhotonResult;
import org.apache.solr.common.SolrDocument;

import java.util.*;

public class SolrResult implements PhotonResult {
    private static final String[] NAME_PRECEDENCE = {"default", "housename", "int", "loc", "reg", "alt", "old"};
    SolrDocument document;

    SolrResult(SolrDocument doc) {
        this.document = doc;
    }

    @Override
    public Object get(String key) {
        return document.get(key);
    }

    @Override
    public String getLocalised(String key, String language) {
        assert(key != null);
        assert(language != null);

        String result = getAsString(key + "." + language);

        if (result != null) {
            return result;
        }

        if ("name".equals(key)) {
            for (String suffix : NAME_PRECEDENCE) {
                result = getAsString("name." + suffix);
                if (result != null) {
                    return result;
                }
            }
        }

        return getAsString((key + ".default"));
    }

    @Override
    public Map<String, String> getMap(String key) {
        Map<String, String> result = new HashMap<>();

        for (Map.Entry<String,Object> entry : document) {
            if (entry.getKey().startsWith(key + ".") && (entry.getValue() instanceof String)) {
                result.put(entry.getKey().substring(key.length() + 1), (String) entry.getValue());
            }
        }

        return result;
    }

    @Override
    public double[] getCoordinates() {
        // Looks like coordinates come back as a string "lat,lon". So let's parse them back into numbers.
        String value = getAsString("cooordinate");

        if (value != null) {
            String[] parts = value.split(",");
            if (parts.length == 2) {
                try {
                    return new double[]{Double.parseDouble(parts[1]), Double.parseDouble(parts[0])};
                } catch (NumberFormatException e) {
                    // fallthrough
                }
            }
        }

        return new double[]{180, 90};
    }

    @Override
    public double[] getExtent() {
        Collection<Object> coords = document.getFieldValues("extent");

        if (coords.size() != 4) {
            return null;
        }

        double[] result = new double[4];
        int pos = 0;
        for (Object o : coords) {
            if (!(o instanceof Double)) {
                return null;
            }
            result[pos++] = (Double) o;
        }

        return result;
    }

    @Override
    public double getScore() {
        Double result = (Double) document.get("score");

        return (result == null) ? 0 : result;
    }

    private String getAsString(String key) {
        Object result = document.get(key);
        return (result instanceof String) ? (String) result : null;
    }
}
