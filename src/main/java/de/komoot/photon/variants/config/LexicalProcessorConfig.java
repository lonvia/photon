package de.komoot.photon.variants.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.NullMarked;

import java.util.ArrayList;

@NullMarked
public class LexicalProcessorConfig implements ProcessorConfig {

    public static class RuleList extends ArrayList<LexicalRulesConfig> {}

    @JsonProperty
    private final RuleList rules = new RuleList();

    @Override
    public String toString() {
        var out = new StringBuilder();

        out.append("Number of rules: ").append(rules.size());
        for (int i = 0; i < rules.size(); ++i) {
            out.append("\n\n").append(i + 1).append(". Rule:\n").append(rules.get(i).toString().indent(2));
        }

        return out.toString();
    }
}
