package de.komoot.photon.solr;

public class Server {
        /**
     * Database version created by new imports with the current code.
     *
     * Format must be: major.minor.patch-dev
     *
     * Increase to next to be released version when the database layout
     * changes in an incompatible way. If it is alredy at the next released
     * version, increase the dev version.
     */
    private static final String DATABASE_VERSION = "1.0.0-0";
    public static final String PROPERTY_DOCUMENT_ID = "DATABASE_PROPERTIES";

    private static final String BASE_FIELD = "document_properties";
    private static final String FIELD_VERSION = "database_version";
    private static final String FIELD_LANGUAGES = "indexed_languages";

    public Server(String mainDirectory) {

    }
}
