package de.komoot.photon.searcher;

import de.komoot.photon.lucene.LuceneQueryBuilder;
import de.komoot.photon.lucene.LuceneSearchResponse;
import de.komoot.photon.lucene.LuceneSearcher;
import de.komoot.photon.query.ReverseRequest;
import org.json.JSONObject;

import java.util.List;

public class ReverseRequestHandler {
    private final LuceneSearcher searcher;

    public ReverseRequestHandler(LuceneSearcher searcher) {
        this.searcher = searcher;
    }

    public List<JSONObject> handle(ReverseRequest photonRequest) {
        LuceneQueryBuilder queryBuilder = buildQuery(photonRequest);
        LuceneSearchResponse results = searcher.search(queryBuilder, photonRequest.getLimit());
        List<JSONObject> resultJsonObjects = results.convertToJSON(photonRequest.getLanguage());
        if (resultJsonObjects.size() > photonRequest.getLimit()) {
            resultJsonObjects = resultJsonObjects.subList(0, photonRequest.getLimit());
        }
        return resultJsonObjects;
    }

    public LuceneQueryBuilder buildQuery(ReverseRequest photonRequest) {
        return searcher.makeQueryBilder().
                reverseLookup(photonRequest.getLocation(), photonRequest.getRadius(), photonRequest.getQueryStringFilter());
    }
}
