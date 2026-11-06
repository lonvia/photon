package de.komoot.photon.variants.transformer;

import de.komoot.photon.normalization.NullNormalizer;
import de.komoot.photon.variants.config.LexicalRulesConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.util.Set;

import static org.assertj.core.api.Assertions.*;

class LexicalTransformerTest {

    LexicalTransformer fromRules(String... rules) {
        var content = new StringBuilder("words:\n");
        for (var rule : rules) {
            content.append("    - ").append(rule).append("\n");
        }

        var config = YAMLMapper.shared().readValue(content.toString(), LexicalRulesConfig.class);
        return new LexicalTransformer(config.getReplacementTree(NullNormalizer.shared()));
    }

    @Test
    void testAddAbbreviation() {
        var trans = fromRules("Air Force Base -> AFB", "Road -> Rd");

        assertThat(trans.transform(Set.of("Air Force Base")))
                .containsExactlyInAnyOrder("Air Force Base", "AFB");
        assertThat(trans.transform(Set.of("Air Force Base Road")))
                .containsExactlyInAnyOrder(
                        "Air Force Base Road", "AFB Road",
                        "Air Force Base Rd", "AFB Rd");
    }

    @ParameterizedTest
    @ValueSource(strings = {"Meier strasse", "Meierstrasse"})
    void testDecomposeSuffix(String name) {
        var trans = fromRules("~strasse -> str");

        assertThat(trans.transform(Set.of(name)))
                .containsExactlyInAnyOrder(
                        "Meier strasse", "Meier str",
                        "Meierstrasse", "Meierstr");
    }

    @Test
    void testDecomposePrefix() {
        var trans = fromRules("ober~ -> ob.");

        assertThat(trans.transform(Set.of("oberstadt")))
                .containsExactlyInAnyOrder("oberstadt", "ob.stadt", "ober stadt", "ob. stadt");
    }
}