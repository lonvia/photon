package de.komoot.photon.solr;

import de.komoot.photon.searcher.PhotonResult;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;

public class SolrTestServer extends Server {

    public SolrTestServer(String mainDirectory) {
        super(mainDirectory);
    }

    public PhotonResult getById(String id) {
        try {
            return new SolrResult(getSolrClient().getById(id));
        } catch (SolrServerException e) {
            // fallthrough
        } catch (IOException e) {
            // fallthrough
        }

        return null;
    }

    public void refresh() {
        // nothing
    }
}
