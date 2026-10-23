package de.komoot.photon.variants.config;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.NullMarked;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@NullMarked
public class LexicalRulesConfig {

    @JsonProperty("words")
    private List<String> replacements = List.of();

    @JsonAnySetter
    private final Map<String, String> properties = new HashMap<>();

    @Override
    public String toString() {
        var out = new StringBuilder();

        out.append("Properties:\n");
        for (var entry : properties.entrySet()) {
            out.append("  ").append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
        }
        out.append("Replacements:\n");
        for (var repl : replacements) {
            out.append(" - ").append(repl).append('\n');
        }

        return out.toString();
    }
}
