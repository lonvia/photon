package de.komoot.photon.solr;

import de.komoot.photon.Importer;
import de.komoot.photon.PhotonDoc;
import de.komoot.photon.Utils;
import de.komoot.photon.nominatim.model.AddressType;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.locationtech.jts.geom.Point;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class SolrImporter implements Importer {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SolrImporter.class);
    private static final int BULK_SIZE = 10000;

    private final SolrClient client;
    private final String[] languages;
    private final String[] extraTags;
    private ArrayList<SolrInputDocument> pendingDocuments;

    public SolrImporter(SolrClient client, String[] languages, String[] extraTags) {
        this.client = client;
        this.languages = languages;
        this.extraTags = extraTags;
        pendingDocuments = new ArrayList<>(BULK_SIZE);
    }

    @Override
    public void add(PhotonDoc doc, int objectId) {
        pendingDocuments.add(convert(doc, objectId));

        if (pendingDocuments.size() >= BULK_SIZE) {
            sendDocuments();
        }
    }

    @Override
    public void finish() {
        sendDocuments();
        try {
            client.commit();
            client.optimize();
        } catch (SolrServerException e) {
            LOGGER.error("Commit failed: ", e);
        } catch (IOException e) {
            LOGGER.error("Commit failed: ", e);
        }
    }

    private void sendDocuments() {

        try {
            client.add(pendingDocuments);
            client.commit(true, true);
        } catch (SolrServerException e) {
            LOGGER.error("Documents could not be inserted: ", e);
        } catch (IOException e) {
            LOGGER.error("Documents could not be inserted: ", e);
        }
        pendingDocuments = new ArrayList<>(BULK_SIZE);
    }

    private SolrInputDocument convert(PhotonDoc doc, int objectId) {
        final AddressType atype = doc.getAddressType();

        DocumentBuilder builder = new DocumentBuilder()
                .add("id", doc.getUid(objectId))
                .add("osm_type", doc.getOsmType())
                .add("osm_id", doc.getOsmId())
                .add("osm_key", doc.getTagKey())
                .add("osm_value", doc.getTagValue())
                .add("object_type", atype == null ? "locality" : atype.getName())
                .add("importance", doc.getImportance())
                .addNoneNull("classification", Utils.buildClassificationString(doc.getTagKey(), doc.getTagValue()))
                .addNoneNull("housenumber", doc.getHouseNumber())
                .addNoneNull("postcode", doc.getPostcode());

        if (doc.getCentroid() != null) {
            Point pt = doc.getCentroid();
            builder.add("coordinate", pt.toString());
        }

        if (doc.getBbox() != null) {
            builder.add("extent", doc.getBbox().getMinX());
            builder.add("extent", doc.getBbox().getMaxY());
            builder.add("extent", doc.getBbox().getMaxX());
            builder.add("extent", doc.getBbox().getMinY());
        }


        final String countryCode = doc.getCountryCode();
        if (countryCode != null) {
            builder.add("countrycode", countryCode);
        }

        // Names
        final Map<String, String> name = doc.getName();
        builder.addNoneNull("name.default", name.get("name"))
                .addNoneNull("name.alt", name.get("alt_name"))
                .addNoneNull("name.int", name.get("int_name"))
                .addNoneNull("name.loc", name.get("loc_name"))
                .addNoneNull("name.old", name.get("old_name"))
                .addNoneNull("name.reg", name.get("reg_name"))
                .addNoneNull("name.housename", name.get("addr:housename"));

        for (String lang: languages) {
            builder.addNoneNull("name." + lang, name.get("name:" + lang));
        }

        // Address parts
        for (Map.Entry<AddressType, Map<String, String>> entry : doc.getAddressParts().entrySet()) {
            Map<String, String> names = entry.getValue();
            if (names != null && !names.isEmpty()) {
                String addrpart = entry.getKey().getName();
                builder.addNoneNull(addrpart + ".default", names.get("name"));
                for (String lang : languages) {
                    builder.addNoneNull(addrpart + "." + lang, names.get("name:" + lang));
                }
            }
        }

        // Context
        for (Map<String, String> context : doc.getContext()) {
            builder.addNoneNull("context.default", context.get("name"));
            for (String lang: languages) {
                builder.addNoneNull("context." + lang, context.get("name:" + lang));
            }
        }

        // Extratags
        for (String extra : extraTags) {
            builder.addNoneNull("extra." + extra, doc.getExtratags().get(extra));
        }

        return builder.build();
    }
}