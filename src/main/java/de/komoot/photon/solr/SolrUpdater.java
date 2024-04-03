package de.komoot.photon.solr;

import de.komoot.photon.PhotonDoc;
import de.komoot.photon.Updater;

class SolrUpdater implements Updater {
    @Override
    public void create(PhotonDoc doc, int objectId) {

    }

    @Override
    public boolean exists(long docId, int objectId) {
        return false;
    }

    @Override
    public void delete(long id, int objectId) {

    }

    @Override
    public void finish() {

    }
}
