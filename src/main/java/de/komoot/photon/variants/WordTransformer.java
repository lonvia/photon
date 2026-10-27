package de.komoot.photon.variants;

import java.util.Set;

public interface WordTransformer {

    Set<String> transform(Set<String> input);
}
