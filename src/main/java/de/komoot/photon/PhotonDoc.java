package de.komoot.photon;

import de.komoot.photon.nominatim.model.AddressRow;
import de.komoot.photon.nominatim.model.ContextMap;
import de.komoot.photon.nominatim.model.NameMap;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import de.komoot.photon.nominatim.model.AddressType;
import org.slf4j.Logger;

import java.util.*;

/**
 * Denormalized document with all information needed for saving in the Photon database.
 */
public class PhotonDoc {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(PhotonDoc.class);

    private final long placeId;
    private final String osmType;
    private final long osmId;
    private String tagKey;
    private String tagValue;

    private NameMap name = new NameMap();
    private String postcode = null;
    private Map<String, String> extratags = Collections.emptyMap();
    private Envelope bbox = null;
    private long parentPlaceId = 0; // 0 if unset
    private double importance = 0;
    private String countryCode = null;
    private long linkedPlaceId = 0; // 0 if unset
    private int rankAddress = 30;

    private Map<AddressType, NameMap> addressParts = new EnumMap<>(AddressType.class);
    private ContextMap context = new ContextMap();
    private String houseNumber = null;
    private Point centroid = null;

    public PhotonDoc(long placeId, String osmType, long osmId, String tagKey, String tagValue) {
        this.placeId = placeId;
        this.osmType = osmType;
        this.osmId = osmId;
        this.tagKey = tagKey;
        this.tagValue = tagValue;
    }

    public PhotonDoc(PhotonDoc other) {
        this.placeId = other.placeId;
        this.osmType = other.osmType;
        this.osmId = other.osmId;
        this.tagKey = other.tagKey;
        this.tagValue = other.tagValue;
        this.name = other.name;
        this.houseNumber = other.houseNumber;
        this.postcode = other.postcode;
        this.extratags = other.extratags;
        this.bbox = other.bbox;
        this.parentPlaceId = other.parentPlaceId;
        this.importance = other.importance;
        this.countryCode = other.countryCode;
        this.centroid = other.centroid;
        this.linkedPlaceId = other.linkedPlaceId;
        this.rankAddress = other.rankAddress;
        this.addressParts = other.addressParts;
        this.context = other.context;
    }

    public PhotonDoc names(NameMap names) {
        this.name = names;
        return this;
    }

    public PhotonDoc houseNumber(String houseNumber) {
        this.houseNumber = (houseNumber == null || houseNumber.isEmpty()) ? null : houseNumber;
        return this;
    }

    public PhotonDoc bbox(Geometry geom) {
        if (geom != null) {
            this.bbox = geom.getEnvelopeInternal();
        }
        return this;
    }

    public PhotonDoc centroid(Geometry centroid) {
        this.centroid = (Point) centroid;
        return this;
    }

    public PhotonDoc countryCode(String countryCode) {
        if (countryCode != null) {
            this.countryCode = countryCode.toUpperCase();
        }
        return this;
    }


    public PhotonDoc address(Map<String, String> address) {
        if (address != null) {
            extractAddress(address, AddressType.STREET, "street");
            extractAddress(address, AddressType.CITY, "city");
            extractAddress(address, AddressType.DISTRICT, "suburb");
            extractAddress(address, AddressType.LOCALITY, "neighbourhood");
            extractAddress(address, AddressType.COUNTY, "county");
            extractAddress(address, AddressType.STATE, "state");

            String addressPostCode = address.get("postcode");
            if (addressPostCode != null && !addressPostCode.equals(postcode)) {
                LOGGER.debug("Replacing postcode {} with {} for osmId #{}", postcode, addressPostCode, osmId);
                postcode = addressPostCode;
            }
        }
        return this;
    }

    public PhotonDoc extraTags(Map<String, String> extratags) {
        if (extratags != null) {
            this.extratags = extratags;

            String place = extratags.get("place");
            if (place == null) {
                place = extratags.get("linked_place");
            }
            if (place != null) {
                // take more specific extra tag information
                tagKey = "place";
                tagValue = place;
            }
        }

        return this;
    }

    public PhotonDoc parentPlaceId(long parentPlaceId) {
        this.parentPlaceId = parentPlaceId;
        return this;
    }

    public PhotonDoc importance(Double importance) {
        this.importance = importance;

        return this;
    }

    public PhotonDoc linkedPlaceId(long linkedPlaceId) {
        this.linkedPlaceId = linkedPlaceId;
        return this;
    }

    public PhotonDoc rankAddress(int rank) {
        this.rankAddress = rank;
        return this;
    }

    public PhotonDoc postcode(String postcode) {
        this.postcode = postcode;
        return this;
    }

    public String getUid(int objectId) {
        return makeUid(placeId, objectId);
    }

    public static String makeUid(long placeId, int objectId) {
        if (objectId <= 0)
            return String.valueOf(placeId);

        return String.format("%d.%d", placeId, objectId);
    }

    public AddressType getAddressType() {
        return AddressType.fromRank(rankAddress);
    }

    public boolean isUsefulForIndex() {
        if ("place".equals(tagKey) && "houses".equals(tagValue)) return false;

        if (linkedPlaceId > 0) return false;

        return houseNumber != null || !name.isEmpty();
    }
    
    /**
     * Extract an address field from an address tag and replace the appropriate address field in the document.
     *
     * @param addressType The type of address field to fill.
     * @param addressFieldName The name of the address tag to use (without the 'addr:' prefix).
     */
    private void extractAddress(Map<String, String> address, AddressType addressType, String addressFieldName) {
        final String field = address.get(addressFieldName);

        if (field == null) {
            return;
        }

        var map = addressParts.get(addressType);
        if (map == null) {
            addressParts.put(addressType, NameMap.makeAddressNames(Map.of("name", field), new String[]{}));
        } else {
            final String existingName = map.get("name");
            if (!field.equals(existingName)) {
                LOGGER.debug("Replacing {} name '{}' with '{}' for osmId #{}", addressFieldName, existingName, field, osmId);
                // we keep the former name in the context as it might be helpful when looking up typos
                context.addName("default", existingName);
                addressParts.put(addressType, map.copyWithReplacement("default", field));
            }
        }
    }

    /**
     * Set names for the given address part if it is not already set.
     *
     * @return True, if the address was inserted.
     */
    private boolean setAddressPartIfNew(AddressType addressType, NameMap names) {
        return addressParts.computeIfAbsent(addressType, k -> names) == names;
    }

    /**
     * Complete address data from a list of address rows.
     */
    public void completePlace(List<AddressRow> addresses) {
        final AddressType doctype = getAddressType();
        for (AddressRow address : addresses) {
            final AddressType atype = address.getAddressType();

            if (atype != null
                    && (atype == doctype || !setAddressPartIfNew(atype, address.getName()))
                    && address.isUsefulForContext()) {
                // no specifically handled item, check if useful for context
                context.addFromMap(address.getName());
            }
            context.addFromMap(address.getContext());
        }
    }

    public void setCountry(NameMap names) {
        addressParts.put(AddressType.COUNTRY, names);
    }

    public long getPlaceId() {
        return this.placeId;
    }

    public String getOsmType() {
        return this.osmType;
    }

    public long getOsmId() {
        return this.osmId;
    }

    public String getTagKey() {
        return this.tagKey;
    }

    public String getTagValue() {
        return this.tagValue;
    }

    public Map<String, String> getName() {
        return this.name;
    }

    public String getPostcode() {
        return this.postcode;
    }

    public Map<String, String> getExtratags() {
        return this.extratags;
    }

    public Envelope getBbox() {
        return this.bbox;
    }

    public long getParentPlaceId() {
        return this.parentPlaceId;
    }

    public double getImportance() {
        return this.importance;
    }

    public String getCountryCode() {
        return this.countryCode;
    }

    public int getRankAddress() {
        return this.rankAddress;
    }

    public Map<AddressType, NameMap> getAddressParts() {
        return this.addressParts;
    }

    public ContextMap getContext() {
        return this.context;
    }

    public String getHouseNumber() {
        return this.houseNumber;
    }

    public Point getCentroid() {
        return this.centroid;
    }
}
