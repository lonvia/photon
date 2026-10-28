package de.komoot.photon.variants.config;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.exc.JacksonIOException;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.deser.std.StdDeserializer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class NestedConfigArrayDeserializer<T> extends StdDeserializer<List<T>> {
    private final Path baseDir;
    private final Class<T> classType;

    public NestedConfigArrayDeserializer(Path baseDir, Class<T> type) {
        super(List.class);
        this.baseDir = baseDir.toAbsolutePath();
        this.classType = type;
    }

    @Override
    public List<T> deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {
        var rules = new ArrayList<T>();

        if (p.isExpectedStartArrayToken()) {
            while (p.nextToken() != JsonToken.END_ARRAY) {
                if (p.currentToken() == JsonToken.VALUE_STRING
                    && p.getValueAsString().startsWith("/FILE/ ")) {
                    try {
                        var reader = new FileReader(baseDir.resolve(p.getValueAsString().substring(7)).toFile());
                        var parser = ctxt.createParser(reader);
                        if (parser.nextToken() != null) {
                            rules.addAll(deserialize(parser, ctxt));
                        }
                    } catch (FileNotFoundException e) {
                        throw JacksonIOException.construct(e);
                    }
                } else {
                    rules.add(ctxt.readValue(p, classType));
                }
            }
        }

        return rules;
    }
}
