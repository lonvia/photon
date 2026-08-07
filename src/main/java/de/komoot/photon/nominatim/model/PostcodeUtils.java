package de.komoot.photon.nominatim.model;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;

@NullMarked
public class PostcodeUtils {
    private PostcodeUtils() {}

    public static @Nullable String postcodeAltName(String postcode) {
        String shortPostcode = postcode.replaceAll("[ -]", "");

        return shortPostcode.length() == postcode.length() ? null : shortPostcode;
    }

    public static NameMap postcodeToName(String postcode) {
        String shortPostcode = postcodeAltName(postcode);
        if (shortPostcode != null) {
            return NameMap.makeForPlace(Map.of(
                    "name", postcode,
                    "alt_name", shortPostcode
            ), List.of());
        }

        return NameMap.makeForPlace(Map.of("name", postcode), List.of());
    }

}
