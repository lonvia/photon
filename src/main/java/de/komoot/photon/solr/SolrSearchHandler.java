package de.komoot.photon.solr;

import de.komoot.photon.query.PhotonRequest;
import de.komoot.photon.searcher.PhotonResult;
import de.komoot.photon.searcher.SearchHandler;

import java.util.List;

public class SolrSearchHandler implements SearchHandler {
    @Override
    public List<PhotonResult> search(PhotonRequest photonRequest) {
        return null;
    }

    @Override
    public String dumpQuery(PhotonRequest photonRequest) {
        return null;
    }
}
