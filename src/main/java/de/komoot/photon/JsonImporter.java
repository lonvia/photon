package de.komoot.photon;

import java.io.IOException;
import java.util.Date;

public interface JsonImporter {

    /**
     * Read the header document of the file.
     *
     * Throws a UserException when the header is not in the expected form.
     *
     * Returns the database date of the data.
     */
    Date readHeader() throws IOException;

    /**
     * Read documents from a dump file and import them into the database.
     *
     * Returns the number of documents imported.
     */
    long readData(String[] countryCodes, String[] languages, String[] extraTags) throws IOException;
}
