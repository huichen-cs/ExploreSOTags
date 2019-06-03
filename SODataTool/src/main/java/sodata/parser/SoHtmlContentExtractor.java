package sodata.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetector;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.langdetect.LanguageDetectorModel;
import opennlp.tools.lemmatizer.DictionaryLemmatizer;
import opennlp.tools.ngram.NGramModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.StringList;

public class SoHtmlContentExtractor {
    private static Logger LOGGER = LoggerFactory.getLogger(SoHtmlContentExtractor.class);
    
    private LanguageDetector ld;
    private SentenceDetectorME sd;
    private TokenizerME tokenizer;
    private POSTaggerME posTagger;
    private DictionaryLemmatizer lemmatizer;
    private Set<String> stopWords;
    
    public SoHtmlContentExtractor() throws IOException {
        InputStream is = SoHtmlContentExtractor.class.getClassLoader().getResourceAsStream("models/langdetect-183.bin");
        LanguageDetectorModel ldModel = new LanguageDetectorModel(is);
        ld = new LanguageDetectorME(ldModel);
        
        is = SoHtmlContentExtractor.class.getClassLoader().getResourceAsStream("models/en-sent.bin");
        SentenceModel sdModel = new SentenceModel(is);
        sd = new SentenceDetectorME(sdModel);
        
        is = SoHtmlContentExtractor.class.getClassLoader().getResourceAsStream("models/en-token.bin");
        TokenizerModel tknModel = new TokenizerModel(is);
        tokenizer = new TokenizerME(tknModel);
        
        is = SoHtmlContentExtractor.class.getClassLoader().getResourceAsStream("models/en-pos-maxent.bin");
        POSModel posModel = new POSModel(is);
        posTagger = new POSTaggerME(posModel);
       
        is = SoHtmlContentExtractor.class.getClassLoader().getResourceAsStream("models/en-lemmatizer.dict");
        lemmatizer = new DictionaryLemmatizer(is);
        
        is = SoHtmlContentExtractor.class.getClassLoader().getResourceAsStream("models/en_stop_words.txt");
        stopWords = new HashSet<String>(new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)).lines()
                .collect(Collectors.toList()));
        stopWords.add("O"); // opennlp undetermined lemma
    }
    
    public Language getTopLanguage(String text) throws IOException {
        Language[] languages = ld.predictLanguages(text);
        return languages[0];
    }
    
    public NGramModel extractNGramModel(String question, String title, int minLength, int maxLength) throws IOException {
        // following the pipeline
        // 1. extract text from questin and title, and extract sentences from text
        List<String> sentenceList = extractText(question);
        sentenceList.addAll(extractText(title));
        
        // 2. extract tokens
        List<String> tokenList = extractTokens(sentenceList);
        String[] tokens = tokenList.toArray(new String[tokenList.size()]);
        LOGGER.debug("Tokens: " + String.join(",", tokens));
        
        // 3. do Part-of-Speech Tagging
        String[] taggers = getPosTaggers(tokens);
        
        // 4. Lemmatize the tokens
        String[] lemmas = lemmatize(tokens, taggers);
        LOGGER.debug("Lemmas: " + String.join(",", tokens));
        
        // 5. remove stop words and non-lemmas (proper nouns?)
        String[] filteredLemmas = Arrays.stream(lemmas)
                .filter(e -> !stopWords.contains(e)).toArray(String[]::new);
        LOGGER.debug("Filtered Lemmas: " + String.join(",", tokens));
        
        if (filteredLemmas.length == 0) return null;
        
        // 6. build NGramModel
        NGramModel nGramModel = new NGramModel();
        nGramModel.add(new StringList(filteredLemmas), minLength, maxLength);
        
        return nGramModel;
    }
    
//    public List<String> extractNGrams(NGramModel nGramModel) {
//        System.out.println("Total ngrams: " + nGramModel.numberOfGrams());
//        for (StringList ngram : nGramModel) {
//            System.out.println(nGramModel.getCount(ngram) + " - " + ngram);
//        }
//    }
    
    public String[] lemmatize(String[] tokens, String[] tags) {
        String[] lemmas = lemmatizer.lemmatize(tokens, tags);
        return lemmas;
    }
    
    public String[] getPosTaggers(List<String> tokenList) {
        String tags[] = posTagger.tag(tokenList.toArray(new String[tokenList.size()]));
        return tags;
    }
    
    public String[] getPosTaggers(String[] tokens) {
        String tags[] = posTagger.tag(tokens);
        return tags;
    }
    
    public List<String> extractTokens(List<String> sentenceList) {
        List<String> tokenList = new LinkedList<String>();
        for (String sentence: sentenceList) {
            String[] tokens = tokenizer.tokenize(sentence);
            tokenList.addAll(Arrays.asList(tokens));
        }
        return tokenList;
    }
    
    
    public List<String> extractText(String html) throws IOException {
        List<String> sentenceList = new LinkedList<String>();
        Document doc = Jsoup.parseBodyFragment(html);
        if (doc.body().hasText() && doc.body().ownText() != null && !doc.body().ownText().isEmpty()) {
            getTextSentences(doc.body().ownText(), sentenceList);
        }
        if (doc.body().children().size() > 0) {
            getElementsSentences(doc.body().children(), sentenceList);
        }
        return sentenceList;
    }
    
    public static List<String> extractCode(String html) {
        List<String> codeSnippetList = new LinkedList<String>();
        Document doc = Jsoup.parseBodyFragment(html);
        getElementsCodeSnippets(doc.body().children(), codeSnippetList);
        return codeSnippetList;
    }
    
    private static void getElementsCodeSnippets(Elements elements, List<String> snippetList) {
        for (Element e: elements) {
            switch(e.nodeName()) {
            case "code":
                snippetList.add(e.text());
                break;
            default:
                Elements children = e.children();
                if (children != null) {
                    getElementsCodeSnippets(children, snippetList);
                }
            }
        }
    }
    
    private void getTextSentences(String text, List<String> sentenceList) throws IOException {
        String[] sentences = getParagraphSentences(text);
        sentenceList.addAll(Arrays.asList(sentences));
    }
    
    private void getElementsSentences(Elements elements, List<String> sentenceList) throws IOException {
        for (Element e: elements) {
            switch(e.nodeName()) {
            case "p":
                getTextSentences(e.text(), sentenceList);
                break;
            case "pre":
                getElementsSentences(e.children(), sentenceList);
                break;
            default:
                LOGGER.debug("Found " + e.nodeName() + " with " + e.text());
            }
        }
    }
    
    private String[] getParagraphSentences(String paragraph) throws IOException {
        String sentences[] = sd.sentDetect(paragraph);
        return sentences;
    }
}
