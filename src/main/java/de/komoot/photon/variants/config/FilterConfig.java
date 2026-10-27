package de.komoot.photon.variants.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;

@NullMarked
public class FilterConfig {

    @JsonProperty
    @Nullable String lang;

    @JsonProperty
    @Nullable String country;

    @JsonProperty
    List<String> categories = List.of();

    @JsonProperty
    Map<String, String> extra = Map.of();

    @Override
    public String toString() {
        var out = new StringBuilder();
        if (lang != null) {
            out.append("lang: ").append(lang).append('\n');
        }
        if (country != null) {
            out.append("country: ").append(country).append('\n');
        }
        if (!categories.isEmpty()) {
            out.append("categories:\n");
            for (var e: categories) {
                out.append("  ").append(e).append('\n');
            }
        }
        if (!extra.isEmpty()) {
            out.append("extra:\n");
            for (var e: extra.entrySet()) {
                out.append("  ").append(e.getKey()).append(": ").append(e.getValue()).append('\n');
            }
        }

        return out.toString();
    }
}
