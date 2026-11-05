package de.komoot.photon.variants.config;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.exc.JacksonIOException;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.deser.std.StdDeserializer;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class NestedConfigArrayDeserializer<T> extends StdDeserializer<List<T>> {
    private final NestedConfigResolver resolver;
    private final Class<T> classType;

    public NestedConfigArrayDeserializer(NestedConfigResolver resolver, Class<T> type) {
        super(List.class);
        this.resolver = resolver;
        this.classType = type;
    }

    @Override
    public List<T> deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {
        var rules = new ArrayList<T>();

        if (p.isExpectedStartArrayToken()) {
            while (p.nextToken() != JsonToken.END_ARRAY) {
                if (p.currentToken() == JsonToken.VALUE_STRING) {

                    try {
                        var stream = resolver.getStreamForFile(p.getValueAsString());
                        if (stream != null) {
                            var parser = ctxt.createParser(stream);
                            if (parser.nextToken() != null) {
                                rules.addAll(deserialize(parser, ctxt));
                            }
                            continue;
                        }
                    } catch (FileNotFoundException e) {
                        throw JacksonIOException.construct(e);
                    }
                }
                rules.add(ctxt.readValue(p, classType));
            }
        }

        return rules;
    }
}
