package de.komoot.photon.variants;

import de.komoot.photon.PhotonDoc;
import de.komoot.photon.variants.config.VariantConfig;
import org.junit.jupiter.api.Test;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

class VariantsProcessorTest {

    private VariantsProcessor make(String rules, String... langs) {
        var config = YAMLMapper.shared().readValue(rules.stripIndent(), VariantConfig.class);

        return config.createProcessor(Arrays.stream(langs).collect(Collectors.toSet()));
    }

    @Test
    void testUnfilteredLexicalRules() {
        var proc = make("""
                variants:
                  - type: lexical
                    rules:
                      - words:
                        - Avenue -> Av,Ave
                        - Back -> Bk
                        - National Park -> NP
                """);

        var trans = proc.getTransformForDoc(new PhotonDoc());

        assertThat(trans.transform("default", "Red Avenue"))
                .containsExactlyInAnyOrder("Red Avenue", "Red Av", "Red Ave");

        assertThat(trans.transform("", "Back Avenue"))
                .containsExactlyInAnyOrder(
                        "Back Avenue", "Back Av", "Back Ave",
                        "Bk Avenue", "Bk Av", "Bk Ave");
    }

    @Test
    void testLanguageFilteredRules() {
        var proc = make("""
                variants:
                  - type: lexical
                    rules:
                      - lang: en
                        words:
                        - Avenue -> Av,Ave
                      - lang: de
                        words:
                        - ~strasse -> str
                """, "de", "en");

        var trans = proc.getTransformForDoc(new PhotonDoc());

        assertThat(trans.transform("default", "Red Avenue"))
                .containsExactlyInAnyOrder("Red Avenue");
        assertThat(trans.transform("de", "Red Avenue"))
                .containsExactlyInAnyOrder("Red Avenue");
        assertThat(trans.transform("en", "Red Avenue"))
                .containsExactlyInAnyOrder("Red Avenue", "Red Av", "Red Ave");
        assertThat(trans.transform("de", "Hauptstrasse"))
                .containsExactlyInAnyOrder("Hauptstrasse", "Haupt strasse", "Hauptstr", "Haupt str");
    }

    @Test
    void testLanguageApplyMultipleRules() {
        var proc = make("""
                variants:
                  - type: lexical
                    rules:
                      - lang: en
                        words:
                        - Avenue -> Av,Ave
                      - words:
                        - Red => RED
                """, "de", "en");

        var trans = proc.getTransformForDoc(new PhotonDoc());

        assertThat(trans.transform("default", "Red Avenue"))
                .containsExactlyInAnyOrder("RED Avenue");
        assertThat(trans.transform("de", "Red Avenue"))
                .containsExactlyInAnyOrder("RED Avenue");
        assertThat(trans.transform("en", "Red Avenue"))
                .containsExactlyInAnyOrder("RED Avenue", "RED Av", "RED Ave");
    }
}