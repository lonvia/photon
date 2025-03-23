package de.komoot.photon.opensearch;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.komoot.photon.JsonDumper;
import de.komoot.photon.JsonImporter;
import de.komoot.photon.nominatim.model.AddressType;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class OpenSearchJsonImporter implements JsonImporter {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(OpenSearchJsonImporter.class);
    final Importer importer;
    final JsonParser parser;

    public OpenSearchJsonImporter(File file, Importer importer) throws IOException {
        this.importer = importer;

        final ObjectMapper mapper = new ObjectMapper();
        final JsonFactory jfactory = mapper.getFactory();
        parser = jfactory.createParser(file);
    }

    public OpenSearchJsonImporter(InputStream stream, Importer importer) throws IOException {
        this.importer = importer;

        final ObjectMapper mapper = new ObjectMapper();
        final JsonFactory jfactory = mapper.getFactory();
        parser = jfactory.createParser(stream);

    }

    @Override
    public Date readHeader() throws IOException {
        final var header = parser.readValueAs(JsonNode.class);
        final var idNode = header.get("id");
        final var versionNode = header.get("version");
        final var importDateNode = header.get("importDate");

        if (idNode == null || !JsonDumper.HEADER_ID.equals(idNode.asText())) {
            throw new IOException("Missing header. Is this really a Photon dump file?");
        }

        if (versionNode == null || !PhotonDocSerializer.FORMAT_VERSION.equals(versionNode.asText())) {
            throw new IOException("Version of dump file cannot be read by this Photon version.");
        }

        if (importDateNode == null) {
            return Date.from(Instant.now());
        }

        return Date.from(Instant.parse(importDateNode.asText() + "Z"));
    }

    @Override
    public long readData(String[] countryCodes, String[] languages, String[] extraTags) throws IOException {
        final long startMillis = System.currentTimeMillis();
        long totalDocuments = 0;

        List<String> clist = null;
        if  (countryCodes.length > 0) {
            clist = new ArrayList<>();
            for (var cc : countryCodes) {
                clist.add(cc.toUpperCase());
            }
        }

        List<String> elist = extraTags.length == 0 ? null : Arrays.asList(extraTags);

        ArrayList<String> llist = new ArrayList<>();
        llist.add("default");
        for (var lang : languages) {
            llist.add(lang);
        }

        var tree = parser.readValueAs(JsonNode.class);
        while (tree != null) {
            final var doc = tree.get("document");
            final var countryCode = doc.get("countrycode");
            if (clist == null || (countryCode != null && clist.contains(countryCode.asText()))) {
                // filter extra tags
                if (elist == null) {
                    tree.withObject("document").remove("extra");
                } else if (doc.has("extra")) {
                    doc.withObject("extra").retain(elist);
                }

                // filter languages
                if (doc.has("name")) {
                    doc.withObject("name").retain(llist);
                }
                if (doc.has("context")) {
                    doc.withObject("context").retain(llist);
                }

                for (var addr : AddressType.values()) {
                    if (doc.has(addr.getName())) {
                        doc.withObject(addr.getName()).retain(llist);
                    }
                }

                importer.addRaw(doc, tree.get("id").asText());
                totalDocuments += 1;

                if (totalDocuments % 50000 == 0) {
                    final double documentsPerSecond = 1000d * totalDocuments / (System.currentTimeMillis() - startMillis);
                    LOGGER.info("Imported {} documents [{}/second]", totalDocuments, documentsPerSecond);
                }
            }
            tree = parser.readValueAsTree();
        }

        importer.finish();

        return totalDocuments;
    }
}
