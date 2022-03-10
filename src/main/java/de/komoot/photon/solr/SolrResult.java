package de.komoot.photon.solr;

import de.komoot.photon.searcher.PhotonResult;
import org.apache.solr.common.SolrDocument;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

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
            if (entry.getKey().startsWith(key + ".")) {
                Object value = entry.getValue();

                if (value instanceof Collection) {
                    value = ((Collection<?>) value).iterator().next();
                }

                if (value instanceof String) {
                    result.put(entry.getKey().substring(key.length() + 1), (String) value);
                }
            }
        }

        return result.isEmpty() ? null : result;
    }

    @Override
    public double[] getCoordinates() {
        // Coordinate come back as WKT
        String value = getAsString("coordinate");

        if (value == null) {
            return INVALID_COORDINATES;
        }

        try {
            Coordinate coordinate = new WKTReader().read(value).getCoordinate();
            return new double[]{coordinate.getX(), coordinate.getY()};
        } catch (ParseException e) {
            // fallthrough
        }

        return INVALID_COORDINATES;
    }

    @Override
    public double[] getExtent() {
        Collection<Object> coords = document.getFieldValues("extent");

        if (coords == null || coords.size() != 4) {
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
        Float result = (Float) document.get("score");

        return (result == null) ? 0 : result;
    }

    private String getAsString(String key) {
        Object result = document.get(key);

        if (result instanceof Collection) {
            result = ((Collection) result).iterator().next();
        }

        return (result instanceof String) ? (String) result : null;
    }
}
