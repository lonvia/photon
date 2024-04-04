package de.komoot.photon.solr;

import de.komoot.photon.query.ReverseRequest;
import de.komoot.photon.searcher.PhotonResult;
import de.komoot.photon.searcher.ReverseHandler;
import de.komoot.photon.searcher.TagFilter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SolrReverseHandler implements ReverseHandler {
    private final SolrClient client;

    SolrReverseHandler(SolrClient client) {
        this.client = client;
    }

    @Override
    public List<PhotonResult> reverse(ReverseRequest request) {
        SolrQueryBuilder builder = buildQuery(request);

        try {
            QueryResponse response = client.query(builder.build());

            final List<PhotonResult> results = new ArrayList<>(response.getResults().size());
            for (SolrDocument doc : response.getResults()) {
                results.add(new SolrResult(doc));
            }

            return results;
        } catch (SolrServerException e) {
            // ignore
        } catch (IOException e) {
            // ignore
        }

        return null;
    }

    @Override
    public String dumpQuery(ReverseRequest photonRequest) {
        return "";
    }

    private SolrQueryBuilder buildQuery(ReverseRequest request) {
        final SolrQueryBuilder builder = new SolrQueryBuilder(request.getLimit())
                .addGeoFilter(request.getLocationDistanceSort())
                .setSpatialParams("coordinate", request.getLocation(), request.getRadius());

        // Classification filters
        for (TagFilter filter : request.getOsmTagFilters()) {
            builder.addTagFilter(filter);
        }

        for (String layer: request.getLayerFilters()) {
            builder.addLayerFilter(layer);
        }

        return builder;
    }
}
