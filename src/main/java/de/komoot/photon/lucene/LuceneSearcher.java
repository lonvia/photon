package de.komoot.photon.lucene;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class LuceneSearcher {
    private final String[] languages;
    private final IndexSearcher searcher;
    private final Analyzer analyzer;

    LuceneSearcher(String indexPath, String[] languages) throws IOException {
        this.languages = languages;

       IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
       searcher = new IndexSearcher(reader);
       analyzer = new SearchAnalyzer();
    }

    public LuceneQueryBuilder makeQueryBilder() {
        return new LuceneQueryBuilder(languages, analyzer);
    }

    public LuceneSearchResponse search(LuceneQueryBuilder queryBuilder, int extLimit) {
        List<Document> results = new ArrayList<>();
        try {
            TopDocs hits = searcher.search(queryBuilder.get(), extLimit);

            for (ScoreDoc hit : hits.scoreDocs) {
                results.add(searcher.doc(hit.doc));
            }
        } catch (IOException e) {
            log.error("Error in search:", e);
        }

        return new LuceneSearchResponse(results);
    }

}
