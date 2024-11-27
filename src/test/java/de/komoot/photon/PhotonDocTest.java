package de.komoot.photon;

import de.komoot.photon.nominatim.model.AddressRow;
import de.komoot.photon.nominatim.model.AddressType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PhotonDocTest {

    @Test
    void testCompleteAddressOverwritesStreet() {
        PhotonDoc doc = simplePhotonDoc();

        doc.completePlace(
            List.of(AddressRow.makeRow(
                    Map.of("name", "parent_place_street"),
                    "highway", "residential", 26,
                    new String[]{}))
        );
        doc.address(Map.of("street", "test street"));

        AssertUtil.assertAddressName("test street", doc, AddressType.STREET);
    }

    @Test
    void testCompleteAddressCreatesStreetIfNonExistentBefore() {
        PhotonDoc doc = simplePhotonDoc();

        doc.address(Map.of("street", "test street"));

        AssertUtil.assertAddressName("test street", doc, AddressType.STREET);
    }

    @Test
    void testAddCountryCode() {
        PhotonDoc doc = new PhotonDoc(1, "W", 2, "highway", "residential").countryCode("de");

        assertEquals("DE", doc.getCountryCode());
    }

    private PhotonDoc simplePhotonDoc() {
        return new PhotonDoc(1, "W", 2, "highway", "residential").houseNumber("4");
    }

}
