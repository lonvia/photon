package de.komoot.photon.variants.config;

import de.komoot.photon.UsageException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

class LexicalRulesConfigTest {

    LexicalRulesConfig configFromRules(String... rules) {
        var content = new StringBuilder("words:\n");
        for (var rule : rules) {
            content.append("    - ").append(rule).append("\n");
        }

        return YAMLMapper.shared().readValue(content.toString(), LexicalRulesConfig.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"foo > bar", "foo -> bar -> bar", "~foo~ -> bar", "fo~ o -> bar"})
    void testIllegalRules(String rule) {
        var cfg = configFromRules(rule);
        assertThatExceptionOfType(UsageException.class)
                .isThrownBy(cfg::getReplacementTree);
    }

    @Test
    void testAddFull() {
        var cfg = configFromRules("foo -> bar", "Community Hall -> CH");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" foo ", Set.of(" bar", " foo")),
                        tuple(" Community Hall ", Set.of(" Community Hall", " CH"))
                );
    }

    @Test
    void testReplaceFull() {
        var cfg = configFromRules("foo => bar");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" foo ", Set.of(" bar"))
                );
    }

    @Test
    void testAddSuffixNoDecompose() {
        var cfg = configFromRules("~berg |-> bg");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" berg ", Set.of(" berg", " bg")),
                        tuple("berg ", Set.of("berg", "bg"))
                );
    }

    @Test
    void testReplaceSuffixNoDecompose() {
        var cfg = configFromRules("~berg |=> bg");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" berg ", Set.of(" bg")),
                        tuple("berg ", Set.of("bg"))
                );
    }

    @Test
    void testAddSuffixDecompose() {
        var cfg = configFromRules("~berg -> bg");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" berg ", Set.of(" berg", " bg", "berg", "bg")),
                        tuple("berg ", Set.of(" berg", " bg", "berg", "bg"))
                );
    }

    @Test
    void testReplaceSuffixDecompose() {
        var cfg = configFromRules("~berg => bg");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" berg ", Set.of("bg", " bg")),
                        tuple("berg ", Set.of("bg", " bg"))
                );
    }

    @Test
    void testAddPrefixNoDecompose() {
        var cfg = configFromRules("hinter~ |-> hnt");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" hinter", Set.of(" hinter", " hnt")),
                        tuple(" hinter ", Set.of(" hinter", " hnt"))
                );
    }

    @Test
    void testReplacePrefixNoDecompose() {
        var cfg = configFromRules("hinter~ |=> hnt");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" hinter", Set.of(" hnt")),
                        tuple(" hinter ", Set.of(" hnt"))
                );
    }

    @Test
    void testAddPrefixDecompose() {
        var cfg = configFromRules("hinter~ -> hnt");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" hinter", Set.of(" hinter", " hinter ", " hnt", " hnt ")),
                        tuple(" hinter ", Set.of(" hinter", " hnt"))
                );
    }

    @Test
    void testReplacePrefixDecompose() {
        var cfg = configFromRules("hinter~ => hnt");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" hinter", Set.of(" hnt", " hnt ")),
                        tuple(" hinter ", Set.of(" hnt"))
                );
    }

    @Test
    void testAddBeginningOnly() {
        var cfg = configFromRules("^Premier -> Pr");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple("^ Premier ", Set.of("^ Pr", "^ Premier"))
                );
    }

    @Test
    void testReplaceBeginningOnly() {
        var cfg = configFromRules("^Premier => Pr");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple("^ Premier ", Set.of("^ Pr"))
                );
    }

    @Test
    void testAddFinalOnly() {
        var cfg = configFromRules("road$ -> rd");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" road ^", Set.of(" rd ^", " road ^"))
                );
    }

    @Test
    void testReplaceFinalOnly() {
        var cfg = configFromRules("road$ => rd");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" road ^", Set.of(" rd ^"))
                );
    }

    @Test
    void testDecomposeOnly() {
        var cfg = configFromRules("~foo -> foo");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" foo ", Set.of(" foo", "foo")),
                        tuple("foo ", Set.of(" foo", "foo"))
                );
    }

    @Test
    void testAddSuffixDecomposeEndOnly() {
        var cfg = configFromRules("~berg |-> bg", "~berg$ -> bg");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" berg ", Set.of(" berg", " bg")),
                        tuple(" berg ^", Set.of(" berg ^", " bg ^", "berg ^", "bg ^")),
                        tuple("berg ", Set.of("berg", "bg")),
                        tuple("berg ^", Set.of(" berg ^", " bg ^", "berg ^", "bg ^"))
                );
    }

    @Test
    void testReplaceSuffixDecomposeEndOnly() {
        var cfg = configFromRules("~berg |=> bg", "~berg$ => bg");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" berg ", Set.of(" bg")),
                        tuple(" berg ^", Set.of(" bg ^", "bg ^")),
                        tuple("berg ", Set.of("bg")),
                        tuple("berg ^", Set.of(" bg ^", "bg ^"))
                );
    }

    @Test
    void testAddMultipleSuffix() {
        var cfg = configFromRules("~berg,~burg -> bg");

        assertThat(cfg.getReplacementTree())
                .extracting(Map.Entry::getKey,
                        e -> new HashSet<>(e.getValue()))
                .containsExactlyInAnyOrder(
                        tuple(" berg ", Set.of(" berg", "berg", " bg", "bg")),
                        tuple("berg ", Set.of(" berg", "berg", " bg", "bg")),
                        tuple(" burg ", Set.of(" burg", "burg", " bg", "bg")),
                        tuple("burg ", Set.of(" burg", "burg", " bg", "bg"))
                );
    }
}