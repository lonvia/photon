package de.komoot.photon.searcher;

public class ClassificationString {
    public static String build(String key, String value) {
        if ("place".equals(key) || "building".equals(key)) {
            return null;
        }

        if ("highway".equals(key)
                && ("unclassified".equals(value) || "residential".equals(value))) {
            return null;
        }

        for (char c : value.toCharArray()) {
            if (!(c == '_'
                    || ((c >= 'a') && (c <= 'z'))
                    || ((c >= 'A') && (c <= 'Z'))
                    || ((c >= '0') && (c <= '9')))) {
                return null;
            }
        }

        return "tpfld" + value.replaceAll("_", "").toLowerCase() + "clsfld" + key.replaceAll("_", "").toLowerCase();
    }
}
