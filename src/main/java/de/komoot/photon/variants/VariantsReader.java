package de.komoot.photon.variants;

import de.komoot.photon.variants.config.LexicalProcessorConfig;
import de.komoot.photon.variants.config.RuleListDeserializer;
import de.komoot.photon.variants.config.VariantConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.io.File;

/**
 * Reads the variants configuration from a yaml file and
 * creates a variant processor.
 */
@NullMarked
public class VariantsReader {
    private static final Logger LOGGER = LogManager.getLogger();
    @Nullable
    VariantConfig config;

    public VariantsReader readConfig(File variantsConfigFile) {
        var ruleListDeserializer = new RuleListDeserializer(variantsConfigFile.getParentFile().toPath());

        YAMLMapper mapper = YAMLMapper.builder()
                .addModule(new SimpleModule()
                        .addDeserializer(LexicalProcessorConfig.RuleList.class, ruleListDeserializer))
                .build();

        config = mapper.readValue(variantsConfigFile, VariantConfig.class);

        LOGGER.info("Loaded config:\n{}", config);

        return this;
    }

    public VariantsProcessor getProcessor() {
        return new VariantsProcessor();
    }
}
