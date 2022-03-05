package de.komoot.photon.solr;

import org.apache.solr.common.SolrInputDocument;

public class DocumentBuilder {
    private final SolrInputDocument doc = new SolrInputDocument();

    DocumentBuilder add(String field, Object value) {
        doc.addField(field, value);
        return this;
    };

    DocumentBuilder addNoneNull(String field, Object value) {
        if (value != null) {
            doc.addField(field, value);
        }
        return this;
    };

    SolrInputDocument build() {
        return doc;
    }
}