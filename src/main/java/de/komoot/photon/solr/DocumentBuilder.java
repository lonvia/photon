package de.komoot.photon.solr;

import org.apache.solr.common.SolrInputDocument;

class DocumentBuilder {
    private final SolrInputDocument doc = new SolrInputDocument();

    public DocumentBuilder add(String field, Object value) {
        doc.addField(field, value);
        return this;
    };

    public DocumentBuilder addNoneNull(String field, Object value) {
        if (value != null) {
            doc.addField(field, value);
        }
        return this;
    };

    public SolrInputDocument build() {
        return doc;
    }
}