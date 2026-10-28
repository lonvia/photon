package de.komoot.photon.variants.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import de.komoot.photon.normalization.Normalizer;
import de.komoot.photon.variants.WordTransformer;
import org.jspecify.annotations.NullMarked;

import java.util.List;

@NullMarked
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = LexicalProcessorConfig.class, name = "lexical"),
        @JsonSubTypes.Type(value = MutationProcessorConfig.class, name = "mutation")
})
public interface ProcessorConfig {
    record TransformerConfig (WordTransformer transformer, FilterConfig filters) {}

    List<TransformerConfig> getTransformers(Normalizer normalizer);
}
