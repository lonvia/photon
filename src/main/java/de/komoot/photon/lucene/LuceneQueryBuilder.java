package de.komoot.photon.lucene;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import de.komoot.photon.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class LuceneQueryBuilder {
    private final String[] languages;
    private Query query;
    private final QueryBuilder queryBuilder;
    private final Analyzer analyzer;
    private final Tokenizer querySplitter;

    LuceneQueryBuilder(String[] languages, Analyzer analyzer) {
        this.languages = languages;
        this.queryBuilder = new QueryBuilder(analyzer);
        this.analyzer = analyzer;
        this.querySplitter = new CharTokenizer() {
            @Override
            protected boolean isTokenChar(int i) {
                return Character.isDigit(i) || Character.isLetter(i);
            }
        };
    }

    public LuceneQueryBuilder searchQuery(String queryString, String language, boolean lenient) {
        String[] terms;
        try {
            terms = splitQuery(queryString);
        } catch (IOException e) {
            query = new MatchNoDocsQuery();
            return this;
        }

        log.warn("Split query: " + String.join(", ", terms));

        BooleanQuery.Builder baseQuery = new BooleanQuery.Builder();
        if (lenient) {
            baseQuery.add(lenientSearchQuery(terms, language), BooleanClause.Occur.MUST);
        } else {
            baseQuery.add(strictSearchQuery(terms, language), BooleanClause.Occur.MUST);
        }
        query = baseQuery.build();
        log.warn("QUREY: " + query.toString());

        return this;
    }

    public LuceneQueryBuilder reverseLookup(Point location, Double radius, String queryStringFilter) {
        return this;
    }

    public Query get() {
        return query;
    }

    LuceneQueryBuilder withLocationBias(Point point, double scale) {
        return this;
    }

    LuceneQueryBuilder withBoundingBox(Envelope bbox) {
        return this;
    }

    public String toJSONString() {
        return " ";
    }

    private String[] splitQuery(String queryString) throws IOException {
            querySplitter.setReader(new StringReader(queryString));
            querySplitter.reset();
            final List<String> terms = new ArrayList<>();
            final CharTermAttribute termAtt = querySplitter.getAttribute(CharTermAttribute.class);
            while (querySplitter.incrementToken()) {
                terms.add(termAtt.toString());
            }
            querySplitter.end();

            return terms.toArray(new String[0]);
    }

    private Query strictSearchQuery(String[] terms, String language) {
        final BooleanQuery.Builder builder = new BooleanQuery.Builder();

        for (String term : terms) {
            final BooleanQuery.Builder term_builder = new BooleanQuery.Builder();

            term_builder.add(queryBuilder.createPhraseQuery("collector.default.ngrams", term), BooleanClause.Occur.SHOULD);

            for (String prelang : languages) {
                Query q = queryBuilder.createPhraseQuery("collector." + prelang + ".ngrams", term);
                if (!prelang.equals(language)) {
                    q = new BoostQuery(q, 0.6f);
                }
                term_builder.add(q, BooleanClause.Occur.SHOULD);
            }
            term_builder.setMinimumNumberShouldMatch(1);

            builder.add(term_builder.build(), BooleanClause.Occur.MUST);
        }

        return builder.build();
    }

    private Query lenientSearchQuery(String[] terms, String language) {
        final BooleanQuery.Builder builder = new BooleanQuery.Builder();

        for (String term : terms) {
            String normterm = "";
            try {
                TokenStream ts = analyzer.tokenStream("collector.default.ngrams", term);
                final CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
                ts.reset();
                while (ts.incrementToken()) {
                    normterm = termAtt.toString();
                }
                ts.close();
            } catch (IOException e) {
                continue;
            }
            log.warn("NORMTERM: " + normterm);
            final BooleanQuery.Builder term_builder = new BooleanQuery.Builder();

            term_builder.add(collectorFuzzyQuery("collector.default.ngrams", normterm), BooleanClause.Occur.SHOULD);

            for (String prelang : languages) {
                Query q = collectorFuzzyQuery("collector." + prelang + ".ngrams", normterm);
                if (!prelang.equals(language)) {
                    q = new BoostQuery(q, 0.6f);
                }
                term_builder.add(q, BooleanClause.Occur.SHOULD);
            }
            term_builder.setMinimumNumberShouldMatch(1);

            builder.add(term_builder.build(), terms.length > 2 ? BooleanClause.Occur.SHOULD : BooleanClause.Occur.MUST);
        }

        if (terms.length > 2) {
            builder.setMinimumNumberShouldMatch(terms.length - 1);
        }

        return builder.build();
    }

    private Query collectorFuzzyQuery(String fieldName, String queryString) {
        FuzzyQuery q = new FuzzyQuery(new Term(fieldName, queryString), 1, 2);

//        q.setRewriteMethod(new MultiTermQuery.TopTermsScoringBooleanQueryRewrite(100));

        return q;
    }
}
