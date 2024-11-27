package de.komoot.photon.nominatim.model;

import java.util.*;

/**
 * Representation of an address as returned by Nominatim's get_addressdata PL/pgSQL function.
 */
public class AddressRow {
    private final NameMap name;
    private final ContextMap context;
    private final String osmKey;
    private final String osmValue;
    private final int rankAddress;

    private AddressRow(NameMap name, ContextMap context, String osmKey, String osmValue, int rankAddress) {
        this.name = name;
        this.context = context;
        this.osmKey = osmKey;
        this.osmValue = osmValue;
        this.rankAddress = rankAddress;
    }

    public AddressType getAddressType() {
        return AddressType.fromRank(rankAddress);
    }

    private boolean isPostcode() {
        if ("place".equals(osmKey) && "postcode".equals(osmValue)) {
            return true;
        }

        return "boundary".equals(osmKey) && "postal_code".equals(osmValue);
    }

    public boolean isUsefulForContext() {
        return !name.isEmpty() && !isPostcode();
    }

    public NameMap getName() {
        return this.name;
    }

    public ContextMap getContext() {
        return context;
    }

    @Override
    public String toString() {
        return "AddressRow{" +
                "name=" + name.getOrDefault("name", "?") +
                ", osmKey='" + osmKey + '\'' +
                ", osmValue='" + osmValue + '\'' +
                ", rankAddress=" + rankAddress +
                '}';
    }

    public static AddressRow makeRow(Map<String, String> name, String osmKey, String osmValue, int rankAddress, String[] languages) {
        ContextMap context = new ContextMap();

        // Makes US state abbreviations searchable.
        context.addName("default", name.get("ISO3166-2"));

        return new AddressRow(NameMap.makeAddressNames(name, languages), context, osmKey, osmValue, rankAddress);
    }
}
