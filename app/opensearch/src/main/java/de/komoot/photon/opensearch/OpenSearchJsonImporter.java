package de.komoot.photon.opensearch;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.komoot.photon.JsonDumper;
import de.komoot.photon.JsonImporter;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Date;

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
        TreeNode tree = parser.readValueAsTree();
        while (tree != null) {
            importer.addRaw(tree.get("document"), tree.get("id").toString());
            totalDocuments += 1;
            tree = parser.readValueAsTree();

            if (totalDocuments % 50000 == 0) {
                final double documentsPerSecond = 1000d * totalDocuments / (System.currentTimeMillis() - startMillis);
                LOGGER.info("Imported {} documents [{}/second]", totalDocuments, documentsPerSecond);
            }

        }

        importer.finish();

        return totalDocuments;
    }
}
