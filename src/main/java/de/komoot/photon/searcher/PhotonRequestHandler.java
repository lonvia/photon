package de.komoot.photon.searcher;

import de.komoot.photon.lucene.LuceneQueryBuilder;
import de.komoot.photon.lucene.LuceneSearchResponse;
import de.komoot.photon.lucene.LuceneSearcher;
import de.komoot.photon.query.PhotonRequest;
import org.json.JSONObject;

import java.util.List;

/**
 * Given a {@link PhotonRequest photon request}, execute the search, process it (for example, de-duplicate) and respond with results formatted in a list of {@link JSONObject json
 * object}s.
 * <p/>
 * Created by Sachin Dole on 2/12/2015.
 */
public class PhotonRequestHandler {

    private final LuceneSearcher searcher;
    private boolean lastLenient = false;

    public PhotonRequestHandler(LuceneSearcher searcher) {
        this.searcher = searcher;
    }

    public List<JSONObject> handle(PhotonRequest photonRequest) {
        lastLenient = false;
        LuceneQueryBuilder queryBuilder = buildQuery(photonRequest, false);
        // for the case of deduplication we need a bit more results, #300
        int limit = photonRequest.getLimit();
        int extLimit = limit > 1 ? (int) Math.round(photonRequest.getLimit() * 1.5) : 1;
        LuceneSearchResponse results = searcher.search(queryBuilder, extLimit);
        if (results.getHits() == 0) {
            lastLenient = true;
            results = searcher.search(buildQuery(photonRequest, true), extLimit);
        }
        List<JSONObject> resultJsonObjects = results.convertToJSON(photonRequest.getLanguage());
        StreetDupesRemover streetDupesRemover = new StreetDupesRemover(photonRequest.getLanguage());
        resultJsonObjects = streetDupesRemover.execute(resultJsonObjects);
        if (resultJsonObjects.size() > limit) {
            resultJsonObjects = resultJsonObjects.subList(0, limit);
        }
        return resultJsonObjects;
    }

    public String dumpQuery(PhotonRequest photonRequest) {
        return buildQuery(photonRequest, lastLenient).toJSONString();
    }

   public LuceneQueryBuilder buildQuery(PhotonRequest photonRequest, boolean lenient) {
        return searcher.makeQueryBilder().
                searchQuery(photonRequest.getQuery(), photonRequest.getLanguage(), lenient);
                /*withTags(photonRequest.tags()).
                withKeys(photonRequest.keys()).
                withValues(photonRequest.values()).
                withoutTags(photonRequest.notTags()).
                withoutKeys(photonRequest.notKeys()).
                withoutValues(photonRequest.notValues()).
                withTagsNotValues(photonRequest.tagNotValues()).
                withLocationBias(photonRequest.getLocationForBias(), photonRequest.getScaleForBias()).
                withBoundingBox(photonRequest.getBbox());*/
    }
}
