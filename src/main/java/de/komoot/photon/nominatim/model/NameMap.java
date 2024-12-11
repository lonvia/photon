package de.komoot.photon.nominatim.model;

import java.util.*;

public class NameMap extends AbstractMap<String, String> {
    private final Set<Entry<String, String>> entries = new HashSet<>();

    public NameMap copyWithReplacement(String field, String value) {
        var newmap = new NameMap();

        newmap.entries.add(new SimpleImmutableEntry<>(field, value));

        for (var entry: entries) {
            if (!entry.getKey().equals(field)) {
                newmap.entries.add(entry);
            }
        }

        return newmap;
    }

    public boolean matches(String other) {
        for (var e: entries) {
            if (e.getValue().equalsIgnoreCase(other)) {
                return true;
            }
        }

        return false;
    }

    private NameMap setName(String field, Map<String, String> source, String... keys) {
        if (!containsKey(field)) {
            for (var key : keys) {
                if (source.containsKey(key)) {
                    entries.add(new SimpleImmutableEntry<>(field, source.get(key)));
                    break;
                }
            }
        }
        return this;
    }

    private NameMap setLocaleNames(Map<String, String> source, String[] languages) {
        setName("default", source, "_place_name", "name");
        for (var lang : languages) {
            setName(lang, source, "_place_name:" + lang, "name:" + lang);
        }
        return this;
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return entries;
    }

    public static NameMap makeAddressNames(Map<String, String> source, String[] languages) {
        return new NameMap().setLocaleNames(source, languages);
    }

    public static NameMap makePlaceNames(Map<String, String> source, String[] languages) {
        return new NameMap()
                .setLocaleNames(source, languages)
                .setName("alt", source, "_place_alt_name", "alt_name")
                .setName("int", source, "_place_int_name", "int_name")
                .setName("loc", source, "_place_loc_name", "loc_name")
                .setName("old", source, "_place_old_name", "old_name")
                .setName("reg", source, "_place_reg_name", "reg_name")
                .setName("housename", source,"addr:housename");
    }

    public static NameMap makeSimpleName(String name) {
        var ret = new NameMap();
        ret.entries.add(new SimpleImmutableEntry<>("default", name));

        return ret;
    }
}