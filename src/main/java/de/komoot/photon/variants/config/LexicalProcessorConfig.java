package de.komoot.photon.variants.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.komoot.photon.normalization.Normalizer;
import de.komoot.photon.variants.transformer.LexicalTransformer;
import org.jspecify.annotations.NullMarked;

import java.util.ArrayList;
import java.util.List;

@NullMarked
public class LexicalProcessorConfig implements ProcessorConfig {

    @JsonProperty
    private final List<LexicalRulesConfig> rules = List.of();

    @Override
    public String toString() {
        var out = new StringBuilder();

        out.append("Number of rules: ").append(rules.size());
        for (int i = 0; i < rules.size(); ++i) {
            out.append("\n\n").append(i + 1).append(". Rule:\n").append(rules.get(i).toString().indent(2));
        }

        return out.toString();
    }

    @Override
    public List<TransformerConfig> getTransformers(Normalizer normalizer) {
        List<TransformerConfig> list = new ArrayList<>();
        for (LexicalRulesConfig rule : rules) {
            var t = rule.getReplacementTree(normalizer);
            if (!t.isEmpty()) {
                var transformerConfig = new TransformerConfig(new LexicalTransformer(t), rule);
                list.add(transformerConfig);
            }
        }
        return list;
    }
}
