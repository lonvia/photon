package de.komoot.photon.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.de.GermanNormalizationFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.analysis.pattern.PatternReplaceCharFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

import java.io.Reader;
import java.util.regex.Pattern;

public class IndexAnalyzer extends AnalyzerWrapper {

    private final Analyzer ngramAnalyzer = new Analyzer() {

        @Override
        protected TokenStreamComponents createComponents(String s) {
            Tokenizer tok = new EdgeNGramTokenizer(1, 100) {
                @Override
                protected boolean isTokenChar(int chr) {
                    return Character.isDigit(chr) || Character.isLetter(chr);
                }
            };

            TokenStream filter = new WordDelimiterGraphFilter(tok, WordDelimiterGraphFilter.PRESERVE_ORIGINAL, null);
            filter = new LowerCaseFilter(filter);
            filter = new GermanNormalizationFilter(filter);
            filter = new ASCIIFoldingFilter(filter);
            filter = new UniqueTokenFilter(filter);

            return new TokenStreamComponents(tok, filter);
        }
    };

    private final Analyzer rawAnalyzer = new Analyzer() {

        @Override
        protected TokenStreamComponents createComponents(String s) {
            Tokenizer tok = new StandardTokenizer();
            TokenStream filter = new WordDelimiterGraphFilter(tok, 0, null);
            filter = new LowerCaseFilter(filter);
            filter = new GermanNormalizationFilter(filter);
            filter = new ASCIIFoldingFilter(filter);
            filter = new UniqueTokenFilter(filter);

            return new TokenStreamComponents(tok, filter);
        }
    };

    private final Analyzer housenumberAnalyzer = new Analyzer() {

        @Override
        protected TokenStreamComponents createComponents(String s) {
            Tokenizer tok = new StandardTokenizer();
            TokenStream filter = new LowerCaseFilter(tok);
            filter = new WordDelimiterGraphFilter(filter, 0, null);

            return new TokenStreamComponents(tok, filter);
        }
    };


    public IndexAnalyzer() {
        super(PER_FIELD_REUSE_STRATEGY);
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        if (fieldName.endsWith(".ngrams")) {
            return ngramAnalyzer;
        }

        if (fieldName.equals("housenumber")) {
            return housenumberAnalyzer;
        }

        return rawAnalyzer;
    }

    private static final Pattern PUNCTUATION_PATTERN = Pattern.compile("[\\.,']");
    private static final Pattern WSN_PATTERN = Pattern.compile("(\\d+)\\s(?=\\p{L}\\b)");

    @Override
    protected Reader wrapReader(String fieldName, Reader reader) {
        reader = new PatternReplaceCharFilter(PUNCTUATION_PATTERN, " ", reader);
        if (fieldName.endsWith(".ngram") || fieldName.equals("housenumber")) {
            reader = new PatternReplaceCharFilter(WSN_PATTERN, "$1", reader);
        }
        return reader;
    }

    @Override
    protected Reader wrapReaderForNormalization(String fieldName, Reader reader) {
        return wrapReader(fieldName, reader);
    }
}
