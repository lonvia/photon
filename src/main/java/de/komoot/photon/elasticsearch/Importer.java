package de.komoot.photon.elasticsearch;

import de.komoot.photon.PhotonDoc;
import de.komoot.photon.Utils;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * elasticsearch importer
 *
 * @author felix
 */
@Slf4j
public class Importer implements de.komoot.photon.Importer {
    private int documentCount = 0;

    private final RestHighLevelClient esClient;
    private BulkRequest bulkRequest;
    private final String[] languages;

    public Importer(RestHighLevelClient esClient, String languages) {
        this.esClient = esClient;
        this.bulkRequest = Requests.bulkRequest();
        this.languages = languages.split(",");
    }

    @Override
    public void add(PhotonDoc doc) {
        try {
            IndexRequest idx = new IndexRequest(PhotonIndex.NAME).type(PhotonIndex.TYPE)
                    .source(Utils.convert(doc, languages)).id(doc.getUid());
            this.bulkRequest.add(idx);
        } catch (IOException e) {
            log.error("could not bulk add document " + doc.getUid(), e);
            return;
        }
        this.documentCount += 1;
        if (this.documentCount > 0 && this.documentCount % 10000 == 0) {
            this.saveDocuments();
        }
    }

    private void saveDocuments() {
        if (this.documentCount < 1) return;

        BulkResponse bulkResponse = null;
        try {
            bulkResponse = esClient.bulk(bulkRequest);
        } catch (IOException e) {
            // TODO: need to handle error.
            log.error(" Could not push documents to database.");
        }
        if (bulkResponse.hasFailures()) {
            log.error("error while bulk import:" + bulkResponse.buildFailureMessage());
        }
        this.bulkRequest = Requests.bulkRequest();
    }

    @Override
    public void finish() {
        this.saveDocuments();
        this.documentCount = 0;
    }
}
