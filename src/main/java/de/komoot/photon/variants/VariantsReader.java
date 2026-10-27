package de.komoot.photon.variants;

import de.komoot.photon.variants.config.LexicalProcessorConfig;
import de.komoot.photon.variants.config.RuleListDeserializer;
import de.komoot.photon.variants.config.VariantConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.NullMarked;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.io.File;
import java.util.Set;

/**
 * Reads the variants configuration from a yaml file and
 * creates a variant processor.
 */
@NullMarked
public class VariantsReader {
    private static final Logger LOGGER = LogManager.getLogger();
    final VariantConfig config;

    private VariantsReader(VariantConfig config) {
        this.config = config;
    }

    public static VariantsReader createFromFile(File variantsConfigFile) {
        var ruleListDeserializer = new RuleListDeserializer(variantsConfigFile.getParentFile().toPath());

        YAMLMapper mapper = YAMLMapper.builder()
                .addModule(new SimpleModule()
                        .addDeserializer(LexicalProcessorConfig.RuleList.class, ruleListDeserializer))
                .build();

        var config = mapper.readValue(variantsConfigFile, VariantConfig.class);

        LOGGER.info("Loaded config:\n{}", config);

        return new VariantsReader(config);
    }

    public VariantsProcessor getProcessor(Set<String> languages) {
        return config.createProcessor(languages);
    }
}
