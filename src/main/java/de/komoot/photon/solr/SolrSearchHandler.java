package de.komoot.photon.solr;

import de.komoot.photon.query.PhotonRequest;
import de.komoot.photon.searcher.PhotonResult;
import de.komoot.photon.searcher.SearchHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

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
        SolrQuery query = buildQuery(request);

        try {
            final QueryResponse response = client.query(query);
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
        return null;
    }

    private SolrQuery buildQuery(PhotonRequest request) {
        final String inquery = request.getQuery().trim();
        final int lastSpace = inquery.lastIndexOf(' ');

        final StringBuffer solrTerm = new StringBuffer();

        if (lastSpace <= 0) {
            // just one term, use the name collector
            solrTerm.append("collector.name.ngram:");
            solrTerm.append(inquery);
        } else {
            solrTerm.append("collector.all.tokens:(");
            solrTerm.append(inquery.substring(0, lastSpace).replaceAll(" ", " && "));
            solrTerm.append(") AND collector.all.ngram:");
            solrTerm.append(inquery, lastSpace + 1, inquery.length());
        }

        SolrQuery query = new SolrQuery(solrTerm.toString());

        if (lastSpace > 0) {
            query.addFilterQuery("collector.name.ngram:(" + inquery.replaceAll(" ", " || ") + ")");
        }

        log.info("Query: " + query);
        return query;
    }
}
