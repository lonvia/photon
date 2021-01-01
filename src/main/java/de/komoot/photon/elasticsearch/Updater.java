package de.komoot.photon.elasticsearch;

import de.komoot.photon.PhotonDoc;
import de.komoot.photon.Utils;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * Updater for elasticsearch
 *
 * @author felix
 */
@Slf4j
public class Updater implements de.komoot.photon.Updater {
    private final RestHighLevelClient esClient;
    private BulkRequest bulkRequest;
    private final String[] languages;

    public Updater(RestHighLevelClient esClient, String languages) {
        this.esClient = esClient;
        this.bulkRequest = Requests.bulkRequest();
        this.languages = languages.split(",");
    }

    public void finish() {
        this.updateDocuments();
    }

    @Override
    public void create(PhotonDoc doc) {
        try {
            this.bulkRequest.add(this.esClient.prepareIndex(PhotonIndex.NAME, PhotonIndex.TYPE).setSource(Utils.convert(doc, this.languages)).setId(String.valueOf(doc.getPlaceId())));
        } catch (IOException e) {
            log.error(String.format("creation of new doc [%s] failed", doc), e);
        }
    }

    public void delete(Long id) {
        this.bulkRequest.add(new DeleteRequest(PhotonIndex.NAME, PhotonIndex.TYPE, String.valueOf(id)));
    }

    private void updateDocuments() {
        if (this.bulkRequest.numberOfActions() == 0) {
            log.warn("Update empty");
            return;
        }
        BulkResponse bulkResponse = null;
        try {
            bulkResponse = esClient.bulk(bulkRequest);
        } catch (IOException e) {
            // TODO
        }
        if (bulkResponse.hasFailures()) {
            log.error("error while bulk update: " + bulkResponse.buildFailureMessage());
        }
        this.bulkRequest = Requests.bulkRequest();
    }
}
