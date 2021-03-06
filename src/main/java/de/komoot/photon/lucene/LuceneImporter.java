package de.komoot.photon.lucene;

import com.vividsolutions.jts.geom.Envelope;
import de.komoot.photon.Constants;
import de.komoot.photon.PhotonDoc;
import de.komoot.photon.nominatim.model.AddressType;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

@Slf4j
public class LuceneImporter implements de.komoot.photon.Importer {
    private final String[] languages;

    private final IndexWriter writer;

    private interface KeyValueConverter {
        public void operation(String k, String v);
    }

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
        try {
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException("Could not close index.", e);
        }
    }

    private Document convert(PhotonDoc doc) {
        Document dbdoc = new Document();

        Map<String, Set<String>> collectors = createLanguageMapSet();

        dbdoc.add(new StringField(Constants.PLACE_ID, Long.toString(doc.getPlaceId()), Field.Store.NO));
        dbdoc.add(new StoredField(Constants.OSM_ID, doc.getOsmId()));
        dbdoc.add(new StoredField(Constants.OSM_TYPE, doc.getOsmType()));
        dbdoc.add(new StringField(Constants.OSM_KEY, doc.getTagKey(), Field.Store.YES));
        dbdoc.add(new StringField(Constants.OSM_VALUE, doc.getTagValue(), Field.Store.YES));

        final AddressType atype = doc.getAddressType();
        dbdoc.add(new StoredField(Constants.OBJECT_TYPE, atype == null ? "locality" : atype.getName()));

        dbdoc.add(new FeatureField(" features", "importance", (float) doc.getImportance()));

        if (doc.getCentroid() != null) {
            dbdoc.add(new LatLonDocValuesField("coordinate.centroid", doc.getCentroid().getY(), doc.getCentroid().getX()));
            // TODO Stored value
        }

        Envelope bbox = doc.getBbox();
        if (bbox != null && bbox.getArea() > 0.) {
            dbdoc.add(new LatLonPoint("coordinate.bbox", bbox.getMinY(), bbox.getMinX()));
            dbdoc.add(new LatLonPoint("coordinate.bbox", bbox.getMaxY(), bbox.getMaxX()));
            // TODO stored value
        }


        if (doc.getHouseNumber() != null) {
            dbdoc.add(new StringField("housenumber", doc.getHouseNumber(), Field.Store.YES));
            collectors.get("default").add(doc.getHouseNumber());
        }

        if (doc.getPostcode() != null) {
            dbdoc.add(new StoredField("postcode", doc.getPostcode()));
            collectors.get("default").add(doc.getPostcode());
        }

        if (doc.getCountryCode() != null) {
            dbdoc.add(new StoredField(Constants.COUNTRYCODE, doc.getCountryCode().getAlpha2()));
        }

        convertContext(doc.getContext()).forEach((k, v) -> {
            if (!v.isEmpty()) {
                collectors.get(k).addAll(v);
                dbdoc.add(new StoredField("context." + k, String.join(",", v)));
            }
        });

        convertNames(doc.getName(), true, (k, v) -> {
            collectors.getOrDefault(k, collectors.get("default")).add(v);
            dbdoc.add(new TextField("name." + k, v, Field.Store.YES));
        });

        doc.getAddressParts().forEach((kind, amap) -> {
            convertNames(amap, false, (k, v) -> {
                collectors.getOrDefault(k, collectors.get("default")).add(v);
                dbdoc.add(new StoredField(kind + ":" + k, v));
            });
        });

        collectors.forEach((k, v) -> {
            if (!v.isEmpty()) {
                // TODO provide appropriate analysers
                dbdoc.add(new TextField("collector." + k + ".ngrams", String.join(",", v), Field.Store.NO));
                dbdoc.add(new TextField("collector." + k + ".raw", String.join(",", v), Field.Store.NO));
            }
        });

        return dbdoc;
    }

    private Map<String, Set<String>> convertContext(Set<Map<String, String>> contexts) {
        Map<String, Set<String>> langStrings = createLanguageMapSet();

        for (Map<String, String> context : contexts) {
            appendIf(langStrings.get("default"), context.get("name"));

            for (String lang : languages) {
                appendIf(langStrings.get(lang), context.get("name:" + lang));
            }
        }

        return langStrings;
    }

    private void convertNames(Map<String, String> names, boolean extra, KeyValueConverter conv) {
        names.forEach((k, v) -> {
            if (extra) {
                if (k == "name") conv.operation("default", v);
                if (k == "alt_name") conv.operation("alt", v);
                if (k == "int_name") conv.operation("int", v);
                if (k == "loc_name") conv.operation("loc", v);
                if (k == "old_name") conv.operation("old", v);
                if (k == "reg_name") conv.operation("reg", v);
                if (k == "addr:housename") conv.operation("housename", v);
            }

            for (String lang : languages) {
                if (k == "name:" + lang) conv.operation(lang, v);
            }
        });
    }

    private void appendIf(Set<String> stringsSet, String value) {
        if (value == null || value.isEmpty())
            stringsSet.add(value);
;
    }

    private Map<String, Set<String>> createLanguageMapSet() {
        Map<String, Set<String>> langStrings = new HashMap<>();
        langStrings.put("default", new HashSet<>());
        for (String lang : languages) {
            langStrings.put(lang, new HashSet<>());
        }

        return langStrings;
    }

}
