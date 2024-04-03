package de.komoot.photon.solr;

import de.komoot.photon.searcher.TagFilter;
import de.komoot.photon.searcher.TagFilterKind;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.params.*;
import org.locationtech.jts.geom.Point;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class SolrQueryBuilder {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SolrQueryBuilder.class);
    private final String[] terms;
    private ModifiableSolrParams query = new ModifiableSolrParams();

    private List<String> orFilterTerms;

    public SolrQueryBuilder(String searchQuery, int limit) {
        terms = Arrays.stream(searchQuery.trim().split("\\s+"))
                .map(s -> ClientUtils.escapeQueryChars(s))
                .toArray(String[]::new);

        // default query settings
        query.set(CommonParams.FL, "* score");
        query.set(CommonParams.ROWS, limit);
    }

    public SolrQueryBuilder(int limit) {
        terms = null;
        // default query settings
        query.set(CommonParams.FL, "* score");
        query.set(CommonParams.ROWS, limit);
    }

    public int numTerms() {
        return terms.length;
    }

    public SolrQueryBuilder allTermsQuery(String field) {
        query.set(CommonParams.DF, field);
        query.set(CommonParams.Q, edismaxParserQuery(field, "100%"));

        return this;
    }

    public SolrQueryBuilder allTermsQuery(String field, int editingDistance, String minMatch) {
        query.set(CommonParams.DF, field);
        query.set(CommonParams.Q, edismaxParserString(field, minMatch)
         + Arrays.stream(terms)
                .map(s -> s + "~" + editingDistance)
                .collect(Collectors.joining(" ")));

        return this;
    }

    public SolrQueryBuilder addFilterOverTerms(String field, String minMatch) {
        query.add(CommonParams.FQ, edismaxParserQuery(field, minMatch));

        return this;
    }

    public SolrQueryBuilder addTagFilter(TagFilter filter) {
        if (filter.getKind() == TagFilterKind.EXCLUDE_VALUE) {
            addOrFilter(String.format("(osm_key:%s && !osm_value:%s)",
                    ClientUtils.escapeQueryChars(filter.getKey()),
                    ClientUtils.escapeQueryChars(filter.getValue())));
        } else {
            String boostQuery;
            if (filter.isKeyOnly()) {
                boostQuery = String.format("osm_key:%s", ClientUtils.escapeQueryChars(filter.getKey()));
            } else if (filter.isValueOnly()) {
                boostQuery = String.format("osm_value:%s", ClientUtils.escapeQueryChars(filter.getValue()));
            } else {
                boostQuery = String.format("(osm_key:%s && osm_value:%s)",
                        ClientUtils.escapeQueryChars(filter.getKey()),
                        ClientUtils.escapeQueryChars(filter.getValue()));
            }
            if (filter.getKind() == TagFilterKind.INCLUDE) {
                addOrFilter(boostQuery);
            } else {
                query.add(CommonParams.FQ, "!" + boostQuery);
            }
        }

        return this;
    }

    public SolrQueryBuilder addLayerFilter(String layer) {
        addOrFilter("object_type:" + layer);

        return this;
    }

    public SolrQueryBuilder addBoost(String rawBoostTerm) {
        query.add("boost", rawBoostTerm);

        return this;
    }

    public SolrQueryBuilder addOrFilter(String rawFilterTerm) {
        if (orFilterTerms == null) {
            orFilterTerms = new ArrayList<>();
        }

        orFilterTerms.add(rawFilterTerm);

        return this;
    }

    public SolrQueryBuilder addGeoFilter(boolean useDistanceSort) {
        query.add(CommonParams.FQ, "{!geofilt cache=false}");

        query.set("q", useDistanceSort ? "{!func}geodist()" : "*.*");

        if (useDistanceSort) {
            addSort("score asc");
        }

        return this;
    }

    public SolrQueryBuilder addBoostOverTerms(String field) {
        query.set("bq", field + ":(" + String.join(" || ", terms) + ")");

        return this;
    }

    public SolrQueryBuilder addSort(String func) {
        query.add("sort", func);

        return this;
    }


    public SolrQueryBuilder setSpatialParams(String field, Point center, double distance) {
        query.set("sfield", field);
        query.set("pt", String.format("%f,%f", center.getY(), center.getX()));
        query.set("d", Double.toString(distance));

        return this;
    }


    public SolrParams build() {
        if (orFilterTerms != null) {
            query.add(CommonParams.FQ, String.join(" || ", orFilterTerms));
            orFilterTerms = null;
        }

        LOGGER.info(debugInfo());


        return query;
    }

    private String edismaxParserQuery(String field, String minMatch) {
        return edismaxParserString(field, minMatch) + String.join(" ", terms);
    }

    private String edismaxParserString(String field, String minMatch) {
        return String.format("{!edismax qf=%s mm=%s}", field, minMatch);
    }

    public String debugInfo() {
        return "{ \"query\": \"" + query +"\"}";
    }
}
