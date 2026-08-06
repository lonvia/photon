package de.komoot.photon.nominatim.model;

import org.jspecify.annotations.NullMarked;

import java.util.List;
import java.util.Map;

@NullMarked
public class PostcodeUtils {
    private PostcodeUtils() {}

    static NameMap postcodeToName(String postcode) {
        String shortPostcode = postcode.replaceAll("[ -]", "");
        if (shortPostcode.length() != postcode.length()) {
            return NameMap.makeForPlace(Map.of(
                    "name", postcode,
                    "alt_name", shortPostcode
            ), List.of());
        }

        return NameMap.makeForPlace(Map.of("name", postcode), List.of());
    }

}
