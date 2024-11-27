package de.komoot.photon;

import de.komoot.photon.elasticsearch.PhotonDocConverter;
import org.slf4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Importer which writes out the documents in a json-like file.
 */
public class JsonDumper implements Importer {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(JsonDumper.class);

    private PrintWriter writer;
    private final String[] extraTags;

    public JsonDumper(String filename, String[] extraTags) throws FileNotFoundException {
        this.writer = new PrintWriter(filename);
        this.extraTags = extraTags;
    }

    @Override
    public void add(PhotonDoc doc, int objectId) {
        try {
            writer.println("{\"index\": {}}");
            writer.println(PhotonDocConverter.convert(doc, extraTags).string());
        } catch (IOException e) {
            LOGGER.error("Error writing json file", e);
        }
    }

    @Override
    public void finish() {
        if (writer != null) {
            writer.close();
        }
    }
}
