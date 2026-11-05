package de.komoot.photon.variants.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.komoot.photon.PhotonDoc;
import de.komoot.photon.normalization.ICUNormalizer;
import de.komoot.photon.normalization.NullNormalizer;
import de.komoot.photon.variants.VariantsProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;

@NullMarked
public class VariantConfig {
    private static final Logger LOGGER = LogManager.getLogger();

    private static class Normalizations {
        @JsonProperty
        @Nullable List<String> input;
    }

    @Nullable private NestedConfigResolver resolver = null;

    @JsonProperty("variants")
    private List<ProcessorConfig> processors = List.of();

    @JsonProperty("normalization")
    private Normalizations normalizers = new Normalizations();

    @Nullable private String countryLanguageMapFile = null;
    private Map<String, String> countryLanguageMap = Map.of();

    @JsonProperty("language-defaults-per-country")
    void setCountryLanguageMap(Map<String, String> map) {
        countryLanguageMap = map;
    }

    @JsonProperty("language-defaults-per-country")
    void setCountryLanguageMap(String fileName) {
        countryLanguageMapFile = fileName;
    }

    @Override
    public String toString() {
        var out = new StringBuilder();

        out.append("Number of processors: ").append(processors.size());

        for (int i = 0; i < processors.size(); ++i) {
            out.append("\n\n").append(i).append(". Processor:\n").append(processors.get(i).toString().indent(2));
        }

        return out.toString();
    }

    public void setResolver(Path baseDir) {
        resolver = new NestedConfigResolver(baseDir);
    }

    public VariantsProcessor createProcessor(Set<String> languages, List<String> normalizerRules) {
        if (resolver != null && countryLanguageMapFile != null) {
            try {
                var stream = resolver.getStreamForFile(
                        "/BUILTIN/".equals(countryLanguageMapFile) ? "/BUILTIN/ /default_languages.yaml" : countryLanguageMapFile);
                if (stream != null) {
                    countryLanguageMap = YAMLMapper.shared()
                            .readerForMapOf(String.class)
                            .readValue(stream);
                    LOGGER.info("Loaded country map: {}", countryLanguageMap);
                }
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        var normalizer = normalizerRules.isEmpty()? NullNormalizer.shared() : new ICUNormalizer("PhotonInputNormalizer", normalizerRules);
        var transformers = new ArrayList<ProcessorConfig.TransformerConfig>();

        var filterCollector = new FilterCollector();
        for (var proc : processors) {
            for (var trans : proc.getTransformers(normalizer)) {
                if (trans.filters().lang == null || languages.contains(trans.filters().lang)) {
                    filterCollector.update(trans.filters());
                    transformers.add(trans);
                }
            }
        }

        var proc = new VariantsProcessor(languages, countryLanguageMap,
                normalizer,
                filterCollector.createPhotonDocFilters(),
                filterCollector.createNameFilters());

        for (var t : transformers) {
            proc.addTransformer(filterCollector.getTransformerFilter(t.filters()), t.transformer());
        }

        return proc;
    }

    public List<String> getInputNormalizerRules() {
        return normalizers.input == null ? List.of() : normalizers.input;
    }

    private record PhotonDocCategoryFilter(String category) implements Function<PhotonDoc, @Nullable String> {

        @Override
        public String apply(PhotonDoc photonDoc) {
                return photonDoc.getCategories().contains(category) ? "1" : "0";
            }
        }

    private record PhotonDocExtraFilter(String extraKey) implements Function<PhotonDoc, @Nullable String> {

        @Override
            public @Nullable String apply(PhotonDoc photonDoc) {
                var value = photonDoc.getExtratags().get(extraKey);
                return (value instanceof String) ? (String) value : null;
            }
        }

    static class FilterCollector {
        boolean hasLang = false;
        boolean hasCountry = false;
        Set<String> categories = new HashSet<>();
        Set<String> extra = new HashSet<>();

        void update(FilterConfig cfg) {
            if (cfg.lang != null) {
                hasLang = true;
            }
            if (cfg.country != null) {
                hasCountry = true;
            }
            categories.addAll(cfg.categories);
            extra.addAll(cfg.extra.keySet());
        }

        List<Function<PhotonDoc, @Nullable String>> createPhotonDocFilters() {
            var funcs = new ArrayList<Function<PhotonDoc, @Nullable String>>();
            if (hasCountry) {
                funcs.add(PhotonDoc::getCountryCode);
            }
            for (var c : categories) {
                funcs.add(new PhotonDocCategoryFilter(c));
            }
            for (var e : extra) {
                funcs.add(new PhotonDocExtraFilter(e));
            }

            return funcs;
        }

        List<Function<String, @Nullable String>> createNameFilters() {
            if (hasLang) {
                return List.of(s -> s);
            }

            return List.of();
        }

        List<@Nullable String> getTransformerFilter(FilterConfig config) {
            var filters = new ArrayList<@Nullable String>();
            if (hasCountry) {
                filters.add(config.country == null ? null : config.country.toUpperCase());
            }
            for (var c : categories) {
                filters.add(config.categories.contains(c) ? "1" : "0");
            }
            for (var e : extra) {
                filters.add(config.extra.get(e));
            }
            if (hasLang) {
                filters.add(config.lang);
            }

            return filters;
        }
    }

    public static VariantConfig createFromFile(File variantsConfigFile) {
        var module = new NestedConfigModule(variantsConfigFile.getAbsoluteFile().getParentFile().toPath());
        YAMLMapper mapper = YAMLMapper.builder().addModule(module).build();

        var config = mapper.readValue(variantsConfigFile, VariantConfig.class);
        config.resolver = module.getResolver();
        return config;
    }

    public static VariantConfig createDefault() {
        var module = new NestedConfigModule(null);
        YAMLMapper mapper = YAMLMapper.builder().addModule(module).build();

        var config = mapper.readValue(VariantConfig.class.getResourceAsStream("/variants.yaml"), VariantConfig.class);
        config.resolver = module.getResolver();
        return config;
    }
}
