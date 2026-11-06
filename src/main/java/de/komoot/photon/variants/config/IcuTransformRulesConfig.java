package de.komoot.photon.variants.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class IcuTransformRulesConfig extends FilterConfig {

    @JsonProperty("transforms")
    private List<String> rules = List.of();

    public List<String> getRules() {
        return rules;
    }
}
