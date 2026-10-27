package de.komoot.photon.variants.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.komoot.photon.PhotonDoc;
import de.komoot.photon.variants.VariantsProcessor;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

@NullMarked
public class VariantConfig {

    @JsonProperty("variants")
    private List<ProcessorConfig> processors = List.of();

    @Override
    public String toString() {
        var out = new StringBuilder();

        out.append("Number of processors: ").append(processors.size());

        for (int i = 0; i < processors.size(); ++i) {
            out.append("\n\n").append(i).append(". Processor:\n").append(processors.get(i).toString().indent(2));
        }

        return out.toString();
    }

    public VariantsProcessor createProcessor(Set<String> languages) {
        var transformers = new ArrayList<ProcessorConfig.TransformerConfig>();
        var filterCollector = new FilterCollector();
        for (var proc : processors) {
            for (var trans : proc.getTransformers()) {
                if (trans.filters().lang == null || languages.contains(trans.filters().lang)) {
                    filterCollector.update(trans.filters());
                    transformers.add(trans);
                }
            }
        }

        var proc = new VariantsProcessor(languages,
                filterCollector.createPhotonDocFilters(),
                filterCollector.createNameFilters());

        for (var t : transformers) {
            proc.addTransformer(filterCollector.getTransformerFilter(t.filters()), t.transformer());
        }

        return proc;
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
                filters.add(config.country);
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
}
