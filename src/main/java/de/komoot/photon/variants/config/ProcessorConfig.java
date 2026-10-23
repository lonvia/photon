package de.komoot.photon.variants.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = LexicalProcessorConfig.class, name = "lexical"),
        @JsonSubTypes.Type(value = MutationProcessorConfig.class, name = "mutation")
})
public interface ProcessorConfig {
}
