package de.komoot.photon.variants.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.NullMarked;

import java.util.List;

@NullMarked
public class VariantConfig {

    @JsonProperty("variants")
    private List<ProcessorConfig> processors = List.of();

    @Override
    public String toString() {
        var out = new StringBuilder();

        out.append("Number of processors: ").append(processors.size());

        for (int i = 0; i < processors.size(); ++i) {
            out.append("\n\n").append(i).append(". Processor:\n").append(processors.get(i).toString().indent(2));
        }

        return out.toString();
    }
}
