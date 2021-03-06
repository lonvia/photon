package de.komoot.photon.lucene;

import de.komoot.photon.PhotonDoc;
import de.komoot.photon.Updater;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

public class LuceneUpdater implements Updater {
    private final String[] languages;

    private final IndexWriter writer;

    LuceneUpdater(String indexPath, String[] languages) throws IOException {
        this.languages = languages;

        Directory dir = FSDirectory.open(Paths.get(indexPath));
        Analyzer analyser = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyser);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        this.writer = new IndexWriter(dir, iwc);
    }

    @Override
    public void create(PhotonDoc doc) {
        // TODO
    }

    @Override
    public void delete(Long id) {
        // TODO
    }

    @Override
    public void finish() {
        // TODO
    }
}
