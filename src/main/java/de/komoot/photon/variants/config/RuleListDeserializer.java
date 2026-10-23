package de.komoot.photon.variants.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.deser.std.StdDeserializer;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.nio.file.Path;
import java.util.List;

public class RuleListDeserializer extends StdDeserializer<LexicalProcessorConfig.RuleList> {
    private static final Logger LOGGER = LogManager.getLogger();

    private final Path baseDir;

    public RuleListDeserializer(Path baseDir) {
        super(List.class);
        this.baseDir = baseDir.toAbsolutePath();
    }

    @Override
    public LexicalProcessorConfig.RuleList deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {
        var rules = new LexicalProcessorConfig.RuleList();

        if (p.isExpectedStartArrayToken()) {
            while (p.nextToken() != JsonToken.END_ARRAY) {
                if (p.currentToken() == JsonToken.VALUE_STRING
                    && p.getValueAsString().startsWith("file:")) {
                    rules.addAll(YAMLMapper.shared()
                            .readerForListOf(LexicalRulesConfig.class)
                            .readValue(baseDir.resolve(p.getValueAsString().substring(5))));
                } else {
                    rules.add(YAMLMapper.shared().readValue(p, LexicalRulesConfig.class));
                }
            }
        }

        return rules;
    }
}
