package de.komoot.photon;

import de.komoot.photon.nominatim.model.*;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
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
    

    public PhotonDoc completeAddress(AddressRowList addressPlaces, Map<String, String> addressTerms) {
        if (addressTerms != null) {
            setFromAddressTerms(
                    AddressType.STREET, new String[]{"street"},
                    26, 28,
                    addressPlaces, addressTerms);
            setFromAddressTerms(
                    AddressType.LOCALITY, new String[]{"place", "neighbourhood"},
                    17, 25,
                    addressPlaces, addressTerms);
            setFromAddressTerms(
                    AddressType.DISTRICT, new String[]{"suburb"},
                    17, 24,
                    addressPlaces, addressTerms);
            setFromAddressTerms(
                    AddressType.CITY, new String[]{"city"},
                    13, 21,
                    addressPlaces, addressTerms);
            setFromAddressTerms(
                    AddressType.COUNTY, new String[]{"county", "district", "subdistrict"},
                    10, 16,
                    addressPlaces, addressTerms);
            setFromAddressTerms(
                    AddressType.STATE, new String[]{"state", "province"},
                    5, 9,
                    addressPlaces, addressTerms);
        }

        if (addressPlaces != null) {
            final AddressType doctype = getAddressType();
            final Iterator<AddressRow> it = addressPlaces.reverseIterRanks();
            while (it.hasNext()) {
                final var address = it.next();
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

        // finally set postcode
        if (addressTerms != null && addressTerms.containsKey("postcode")) {
            this.postcode = addressTerms.get("postcode");
        } else if (addressPlaces != null) {
            var postcode_row = addressPlaces.getPostcode();
            if (postcode_row != null) {
                postcode = postcode_row.getName().get("default");
                if (postcode != null) {
                    this.postcode = postcode;
                }
            }
        }

        return this;
    }

    private void setFromAddressTerms(AddressType atype, String[] terms, int minRank, int maxRank,
                                     AddressRowList addressPlaces, Map<String, String> addressTerms) {
        for (var term: terms) {
            final String termName = addressTerms.get(term);
            if (termName != null) {
                if (addressPlaces != null) {
                    Iterator<AddressRow> it = addressPlaces.reverseIterRanks(minRank, maxRank);
                    while (it.hasNext()) {
                        AddressRow address = it.next();
                        if (address.getName().matches(termName)) {
                            addressParts.put(atype, address.getName());
                            context.addFromMap(address.getContext());
                            addressPlaces.removeRank(address.getRankAddress());
                            return;
                        }
                    }
                }
                // no matching address, fall back to just using the term
                NameMap names = new NameMap();
                addressParts.put(atype, NameMap.makeSimpleName(termName));
                return;
            }
        }
    }

    /**
     * Set names for the given address part if it is not already set.
     */
    private boolean setAddressPartIfNew(AddressType addressType, NameMap names) {
        return addressParts.computeIfAbsent(addressType, k -> names) == names;
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
