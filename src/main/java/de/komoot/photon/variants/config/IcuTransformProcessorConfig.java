package de.komoot.photon.variants.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.komoot.photon.normalization.Normalizer;
import de.komoot.photon.variants.transformer.IcuTransformer;
import org.jspecify.annotations.NullMarked;

import java.util.ArrayList;
import java.util.List;

@NullMarked
public class IcuTransformProcessorConfig implements ProcessorConfig {

    @JsonProperty
    private final List<IcuTransformRulesConfig> rules = List.of();

    @Override
    public List<TransformerConfig> getTransformers(Normalizer normalizer) {
        var out = new ArrayList<TransformerConfig>();

        for (var rule : rules) {
            if (!rule.getRules().isEmpty()) {
                out.add(new TransformerConfig(new IcuTransformer(rule.getRules()), rule));
            }
        }

        return out;
    }
}
