package de.komoot.photon.lucene;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

import java.util.List;

public class LuceneQueryBuilder {

    public LuceneQueryBuilder searchQuery(String query, String language, List<String> languages, boolean lenient) {
        return this;
    }

    public LuceneQueryBuilder reverseLookup(Point location, Double radius, String queryStringFilter) {
        return this;
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
