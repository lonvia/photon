package de.komoot.photon;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PhotonDocTest {


    @Test
    void testAddCountryCode() {
        PhotonDoc doc = new PhotonDoc(1, "W", 2, "highway", "residential");

        doc.countryCode("de");

        assertEquals("DE", doc.getCountryCode());
    }

    private PhotonDoc simplePhotonDoc() {
        return new PhotonDoc(1, "W", 2, "highway", "residential").houseNumber("4");
    }

}
