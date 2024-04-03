package de.komoot.photon.solr;

import de.komoot.photon.query.PhotonRequest;
import de.komoot.photon.searcher.PhotonResult;
import de.komoot.photon.searcher.SearchHandler;
import de.komoot.photon.searcher.TagFilter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SolrSearchHandler implements SearchHandler {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SolrSearchHandler.class);
    private final SolrClient client;

    SolrSearchHandler(SolrClient client) {
        this.client = client;
    }

    @Override
    public List<PhotonResult> search(PhotonRequest request) {
        SolrQueryBuilder builder = buildQuery(request);

        try {
            QueryResponse response = client.query(builder.build());

            if (response.getResults().size() == 0) {
                relaxQuery(builder);
                response = client.query(builder.build());
            }

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
    public String dumpQuery(PhotonRequest photonRequest) {
        return buildQuery(photonRequest).toString();
    }

    private SolrQueryBuilder buildQuery(PhotonRequest request) {
        final SolrQueryBuilder builder = new SolrQueryBuilder(request.getQuery(), request.getQueryLimit());

        // Basic search query (all terms must show up).
        builder.allTermsQuery(builder.numTerms() == 1 ? "collector.name.ngram" : "collector.all.ngram");

        // Require the name to be somewhere in the search query.
        if (builder.numTerms() > 1) {
            builder.addFilterOverTerms("collector.name.ngram", "1");
        }

        // Classification filters
        for (TagFilter filter : request.getOsmTagFilters()) {
            builder.addTagFilter(filter);
        }

        // Boosting
        builder.addBoostOverTerms("name.default") // TODO: need max over both
                .addBoostOverTerms("name." + request.getLanguage());

        if (request.hasLocationBias()) {
            final double scale = request.getScaleForBias();
            final int zoom = Integer.min(request.getZoomForBias(), 18);
            final double radius = (1 << (18 - zoom)) * 0.25;
            final double decay = 1;
            final double radius_decay = decay * 10 / radius;
            builder.addBoost("max(recip(max(0, sub(geodist(), " + radius + ")), " + radius_decay + "," + decay + "," + decay + "), linear(importance, " + scale/2.0 + " ,0.0001))");
            builder.setSpatialParams("coordinate", request.getLocationForBias(), radius);
        } else {
            builder.addBoost("add(importance, 0.0001)");
        }


        return builder;
    }

    private void relaxQuery(SolrQueryBuilder builder) {
        builder.allTermsQuery(builder.numTerms() == 1 ? "collector.name.ngram" : "collector.all.ngram", 1, "-1");
        LOGGER.info("[RELAXED]" + builder.debugInfo());
    }
}
