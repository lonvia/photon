package de.komoot.photon.variants.config;

import de.komoot.photon.normalization.Normalizer;

import java.util.List;

public class IcuTransformProcessorConfig implements ProcessorConfig {
    @Override
    public List<TransformerConfig> getTransformers(Normalizer normalizer) {
        return List.of();
    }
}
