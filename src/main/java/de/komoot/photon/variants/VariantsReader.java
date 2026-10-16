package de.komoot.photon.variants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;

/**
 * Reads the variants configuration from a yaml file and
 * creates a variant processor.
 */
public class VariantsReader {
    private static final Logger LOGGER = LogManager.getLogger();

    public VariantsReader readConfig(File variantsConfigFile) {
        LOGGER.info("Reading variant file.");
        return this;
    }

    public VariantsProcessor getProcessor() {
        return new VariantsProcessor();
    }
}
