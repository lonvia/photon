package de.komoot.photon;

import com.fasterxml.jackson.core.TreeNode;

/**
 * Interface for bulk imports from a data source like nominatim
 */
public interface Importer {
    /**
     * Add a new document to the Photon database.
     */
    void add(PhotonDoc doc, int objectId);

    /**
     * Finish up the import.
     */
    void finish();
}
