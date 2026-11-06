package de.komoot.photon.variants.transformer;

import de.komoot.photon.normalization.ICUNormalizer;
import de.komoot.photon.variants.WordTransformer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IcuTransformer implements WordTransformer {
    private static int ruleId = 1;

    private final ICUNormalizer normalizer;

    public IcuTransformer(List<String> rules) {
        normalizer = new ICUNormalizer("photon-word-transformer-" + ruleId++, rules);
    }

    @Override
    public Set<String> transform(Set<String> input) {
        var out = new HashSet<String>();
        for (var word : input) {
            out.add(word);
            out.add(normalizer.transform(word));
        }
        return out;
    }
}
