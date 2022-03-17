package de.komoot.photon.solr;

import de.komoot.photon.query.PhotonRequest;
import de.komoot.photon.searcher.PhotonResult;
import de.komoot.photon.searcher.SearchHandler;
import de.komoot.photon.searcher.TagFilter;
import de.komoot.photon.searcher.TagFilterKind;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class SolrSearchHandler implements SearchHandler {
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
                .addBoostOverTerms("name." + request.getLanguage())
                .addBoost("add(importance, 0.0001)");

        return builder;
    }

    private void relaxQuery(SolrQueryBuilder builder) {
        builder.allTermsQuery(builder.numTerms() == 1 ? "collector.name.ngram" : "collector.all.ngram", 1, "-1");
        log.info("[RELAXED]" + builder.debugInfo());
    }
}
