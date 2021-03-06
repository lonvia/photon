package de.komoot.photon.lucene;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import de.komoot.photon.Constants;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.QueryBuilder;

import java.util.List;

public class LuceneQueryBuilder {
    private final String[] languages;
    private final QueryBuilder builder;
    private Query query;
    private final Analyzer analyser;

    LuceneQueryBuilder(String[] languages, Analyzer analyzer) {
        this.languages = languages;
        this.builder = new QueryBuilder(analyzer);
        this.analyser = analyzer;
    }

    public LuceneQueryBuilder searchQuery(String queryString, String language, boolean lenient) {
        query = builder.createPhraseQuery("collector.default.ngrams", queryString);
        //query = new TermQuery(new Term(Constants.OSM_KEY, queryString));
        return this;
    }

    public LuceneQueryBuilder reverseLookup(Point location, Double radius, String queryStringFilter) {
        return this;
    }

    public Query get() {
        return query;
    }

    LuceneQueryBuilder withLocationBias(Point point, double scale) {
        return this;
    }

    LuceneQueryBuilder withBoundingBox(Envelope bbox) {
        return this;
    }

    public String toJSONString() {
        return " ";
    }
}
