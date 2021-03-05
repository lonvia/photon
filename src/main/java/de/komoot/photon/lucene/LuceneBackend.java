package de.komoot.photon.lucene;

import de.komoot.photon.CommandLineArgs;
import de.komoot.photon.Updater;

import java.io.IOException;

public class LuceneBackend {
    private final String indexPath;
    private final String languages;

    public LuceneBackend(CommandLineArgs args) {
        this.indexPath = args.getDataDirectory();
        this.languages = args.getLanguages();
    }

    public void recreateIndex() {
        // nothing todo
    }


    public de.komoot.photon.Importer getImporter() {
        try {
            return new LuceneImporter(indexPath, languages);
        } catch (IOException e) {
            throw new RuntimeException("Cannot create importer.", e);
        }
    }

    public Updater getUpdater() {
        try {
            return new LuceneUpdater(indexPath, languages);
        } catch (IOException e) {
            throw new RuntimeException("Cannot create importer.", e);
        }
    }

    public LuceneSearcher getSearcher() {
        return new LuceneSearcher();
    }
}
