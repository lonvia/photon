package de.komoot.photon.normalization;

import com.ibm.icu.text.Transliterator;

import java.util.List;

public class ICUNormalizer implements Normalizer {
    private final Transliterator transliterator;

    public ICUNormalizer(String id, List<String> rules) {
        transliterator = Transliterator.createFromRules(id, String.join(";", rules) + ";", Transliterator.FORWARD);
    }

    @Override
    public String transform(String input) {
        return transliterator.transliterate(input);
    }
}
