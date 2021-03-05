package de.komoot.photon.lucene;

import org.json.JSONObject;

import java.util.List;

public class LuceneSearcher {
    public LuceneQueryBuilder makeQueryBilder() {
        return new LuceneQueryBuilder();
    }

    public LuceneSearchResponse search(LuceneQueryBuilder queryBuilder, int extLimit) {
        return new LuceneSearchResponse();
    }

    public List<JSONObject> convertToJSON(LuceneSearchResponse results) {
        return null;
    }
}
