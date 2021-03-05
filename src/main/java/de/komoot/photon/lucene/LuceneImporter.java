package de.komoot.photon.lucene;

import de.komoot.photon.PhotonDoc;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

@Slf4j
public class LuceneImporter implements de.komoot.photon.Importer {
    private final String[] languages;

    private final IndexWriter writer;

    LuceneImporter(String indexPath, String languages) throws IOException {
        this.languages = languages.split(",");

        Directory dir = FSDirectory.open(Paths.get(indexPath));
        Analyzer analyser = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyser);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        this.writer = new IndexWriter(dir, iwc);
    }

    @Override
    public void add(PhotonDoc doc) {
        try {
            writer.addDocument(convert(doc));
        } catch (IOException e) {
            log.error("could not bulk add document " + doc.getUid(), e);
        }
    }

    @Override
    public void finish() {

    }

    private Document convert(PhotonDoc doc) {
        Document dbdoc = new Document();

        return dbdoc;
    }
}
