package de.komoot.photon.nominatim.model;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ContextMap extends AbstractMap<String, Set<String>> {
    private final Set<Entry<String, Set<String>>> entries = new HashSet<>();

    public void addName(String key, String name) {
        if (name != null) {
            for (var entry: entries) {
                if (entry.getKey().equals(key)) {
                    entry.getValue().add(name);
                    return;
                }
            }
            final Set<String> names = new HashSet<>();
            names.add(name);
            entries.add(new SimpleImmutableEntry<>(key, names));
        }
    }

    public void addFromMap(Map<String, String> map) {
        for (var entry: map.entrySet()) {
            addName(entry.getKey(), entry.getValue());
        }
    }

    public void addFromMap(ContextMap map) {
        for (var entry: map.entrySet()) {
            addAll(entry.getKey(), entry.getValue());
        }
    }

    private void addAll(String key, Set<String> names) {
        for (var e : entries) {
            if (e.getKey().equals(key)) {
                e.getValue().addAll(names);
                return;
            }
        }

        // add a copy of the set (might get more names later)
        entries.add(new SimpleImmutableEntry<>(key, new HashSet<>(names)));
    }

    @Override
    public Set<Entry<String, Set<String>>> entrySet() {
        return entries;
    }
}
