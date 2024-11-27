package de.komoot.photon.opensearch;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import de.komoot.photon.Constants;
import de.komoot.photon.PhotonDoc;
import de.komoot.photon.Utils;
import de.komoot.photon.nominatim.model.ContextMap;
import org.locationtech.jts.geom.Envelope;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class PhotonDocSerializer extends StdSerializer<PhotonDoc> {
    private final String[] extraTags;

    public PhotonDocSerializer(String[] extraTags) {
        super(PhotonDoc.class);
        this.extraTags = extraTags;
    }

    @Override
    public void serialize(PhotonDoc value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        final var atype = value.getAddressType();

        gen.writeStartObject();
        gen.writeNumberField(Constants.OSM_ID, value.getOsmId());
        gen.writeStringField(Constants.OSM_TYPE, value.getOsmType());
        gen.writeStringField(Constants.OSM_KEY, value.getTagKey());
        gen.writeStringField(Constants.OSM_VALUE, value.getTagValue());
        gen.writeStringField(Constants.OBJECT_TYPE, atype == null ? "locality" : atype.getName());
        gen.writeNumberField(Constants.IMPORTANCE, value.getImportance());

        String classification = Utils.buildClassificationString(value.getTagKey(), value.getTagValue());
        if (classification != null) {
            gen.writeStringField(Constants.CLASSIFICATION, classification);
        }

        if (value.getCentroid() != null) {
            gen.writeObjectFieldStart("coordinate");
            gen.writeNumberField("lat", value.getCentroid().getY());
            gen.writeNumberField("lon", value.getCentroid().getX());
            gen.writeEndObject();
        }

        if (value.getHouseNumber() != null) {
            gen.writeStringField("housenumber", value.getHouseNumber());
        }

        if (value.getPostcode() != null) {
            gen.writeStringField("postcode", value.getPostcode());
        }

        gen.writeObjectField("name", value.getName());

        for (var entry : value.getAddressParts().entrySet()) {
            gen.writeObjectField(entry.getKey().getName(), entry.getValue());
        }

        String countryCode = value.getCountryCode();
        if (countryCode != null) {
            gen.writeStringField(Constants.COUNTRYCODE, countryCode);
        }

        writeContext(gen, value.getContext());
        writeExtraTags(gen, value.getExtratags());
        writeExtent(gen, value.getBbox());

        gen.writeEndObject();
    }

    private void writeContext(JsonGenerator gen, ContextMap context) throws IOException {
        if (!context.isEmpty()) {
            gen.writeObjectFieldStart("context");
            for (Map.Entry<String, Set<String>> entry : context.entrySet()) {
                gen.writeStringField(entry.getKey(), String.join(", ", entry.getValue()));
            }
            gen.writeEndObject();
        }
    }

    private void writeExtraTags(JsonGenerator gen, Map<String, String> docTags) throws IOException {
        boolean foundTag = false;

        for (String tag: extraTags) {
            String value = docTags.get(tag);
            if (value != null) {
                if (!foundTag) {
                    gen.writeObjectFieldStart("extra");
                    foundTag = true;
                }
                gen.writeStringField(tag, value);
            }
        }

        if (foundTag) {
            gen.writeEndObject();
        }
    }

    private static void writeExtent(JsonGenerator gen, Envelope bbox) throws IOException {
        if (bbox == null || bbox.getArea() == 0.) return;

        //https://opensearch.org/docs/latest/field-types/supported-field-types/geo-shape/#envelope
        gen.writeObjectFieldStart("extent");
        gen.writeStringField("type", "envelope");

        gen.writeArrayFieldStart("coordinates");

        gen.writeStartArray();
        gen.writeNumber(bbox.getMinX());
        gen.writeNumber(bbox.getMaxY());
        gen.writeEndArray();
        gen.writeStartArray();
        gen.writeNumber(bbox.getMaxX());
        gen.writeNumber(bbox.getMinY());
        gen.writeEndArray();

        gen.writeEndArray();
        gen.writeEndObject();
    }

}
