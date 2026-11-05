package de.komoot.photon.variants.config;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.BeanDescription;
import tools.jackson.databind.DeserializationConfig;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.deser.Deserializers;
import tools.jackson.databind.jsontype.TypeDeserializer;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.type.CollectionType;

import java.nio.file.Path;
import java.util.List;

@NullMarked
public class NestedConfigModule extends SimpleModule {

    class NestedConfigDeserializer extends Deserializers.Base {

        @Override
        public boolean hasDeserializerFor(DeserializationConfig config, Class<?> valueType) {
            return (valueType == List.class);
        }

        @Override
        @Nullable
        public ValueDeserializer<?> findCollectionDeserializer(CollectionType type, DeserializationConfig config, BeanDescription.Supplier beanDescRef, TypeDeserializer elementTypeDeserializer, ValueDeserializer<?> elementDeserializer) {
            if (type.getRawClass() == List.class) {
                return new NestedConfigArrayDeserializer<>(resolver, type.containedType(0).getRawClass());
            }

            return null;
        }
    }

    private final NestedConfigResolver resolver;

    public NestedConfigModule(@Nullable Path baseDir) {
        super("NestedConfigModule");
        this.resolver = new NestedConfigResolver(baseDir);
    }

    @Override
    public void setupModule(SetupContext context) {
        super.setupModule(context);
        context.addDeserializers(new NestedConfigDeserializer());
    }

    public NestedConfigResolver getResolver() {
        return resolver;
    }
}
