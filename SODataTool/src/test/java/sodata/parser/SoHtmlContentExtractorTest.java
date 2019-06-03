package sodata.parser;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import opennlp.tools.langdetect.Language;
import opennlp.tools.ngram.NGramGenerator;
import opennlp.tools.ngram.NGramModel;
import opennlp.tools.util.StringList;

public class SoHtmlContentExtractorTest {
    private static String[] bodies =
        {
            " <p>this is my first question and sorry for my bad English</p>" +
            "                                +" +
            "" +
            "                                +" +
            " <p>I want to extract only word from String that have combination of letter and"
             +
            "number and store it in array</p>+" +
            "" +
            "                                +" +
            " <p>I try this code, but I don't get what I want</p>" +
            "                                +" +
            "" +
            "                                +" +
            " <pre><code>String temp = \"74 4F 4C 4F 49 65  brown fox jump over the fence\";"
             +
            "                                +" +
            " String [] word = temp.split(\"\\W\");" +
            "                                +" +
            " </code></pre>" +
            "                                +" +
            "" +
            "                                +" +
            " <p>this is the result that I want (only word and no empty array)</p>" +
            "                                +" +
            "" +
            "                                +" +
            " <pre><code>brown" +
            "                                +" +
            " fox" +
            "                                +" +
            " jump" +
            "                                +" +
            " over" +
            "                                +" +
            " the" +
            "                                +" +
            " fence" +
            "                                +" +
            " </code></pre>" +
            "                                +" +
            "" +
            "                                +" +
            " <p>Please help, Thank you !</p>" +
            "                                +" +
            ""
            ,
            "<p>I am playing around with if statements but I'm having some trouble. I want to pass random numbers to a variable and then be able to make something happen base on the clicking event.</p>"
            + "<p>Below is what\"I have played around with but it's not working:</p>"
            + "<pre class=\"lang-js prettyprint-override\"><code>$(document).ready(function() {"
            + "var $r;"
            + "$('.cell').click(function() {"
            + "$r = Math.floor(Math.random() * 5);"
            + "    if $($r == 3) {"
            + "      $(this).addClass('img');"
            + "    } else {"
            + "      $(this).css(\"background-color\", \"#708090\").slideUp(150).slideDown(150);"
            + "    }"
            + "  });"
            + " });"
            + "</code></pre>"
        };
    
    
    private String[][] expectedText = {
            {
                "+ + + + + + + + + + + + +",
                "this is my first question and sorry for my bad English",
                "I want to extract only word from String that have combination of letter andnumber and store it in array",
                "I try this code, but I don't get what I want",
                "this is the result that I want (only word and no empty array)",
                "Please help, Thank you !"
            },
            {
                "I am playing around with if statements but I'm having some trouble.", 
                "I want to pass random numbers to a variable and then be able to make something happen base on the clicking event.", 
                "Below is what\"I have played around with but it's not working:"
            }
    };
    private String[][] expectedCode = {
            {
                "String temp = \"74 4F 4C 4F 49 65  brown fox jump over the fence\";                                + String [] word = temp.split(\"\\W\");                                +",
                "brown                                + fox                                + jump                                + over                                + the                                + fence                                +"
            },
            {
                "$(document).ready(function() {var $r;$('.cell').click(function() {$r = Math.floor(Math.random() * 5);    if $($r == 3) {      $(this).addClass('img');    } else {      $(this).css(\"background-color\", \"#708090\").slideUp(150).slideDown(150);    }  }); });"
            }
    };
    
    private static SoHtmlContentExtractor extractor;
    
    @BeforeClass
    public static void init() throws IOException {
        extractor = new SoHtmlContentExtractor();
    }
    
    @Test
    public void testTextExtraction() throws IOException {
        for (int i=0; i<bodies.length; i++) {
            List<String> sentenceList = extractor.extractText(bodies[i]);
            for (String s: sentenceList) {
                System.out.println(s);
            }
            System.out.println("---------------------------------------");
            assertEquals(Arrays.asList(expectedText[i]), sentenceList);
        }
    }
    
    @Test
    public void testOwnTextExtraction() throws IOException {
        String text = "What does it mean by \"error: not declared in this scope?\"<p>Hello, World!</p>";
        List<String> sentenceList = extractor.extractText(text);
        assertEquals(2, sentenceList.size());
        assertEquals("What does it mean by \"error: not declared in this scope?\"", sentenceList.get(0));
        assertEquals("Hello, World!", sentenceList.get(1));
        
        text = "What does it mean by \"error: not declared in this scope?\"";
        sentenceList = extractor.extractText(text);
        assertEquals(1, sentenceList.size());
        assertEquals("What does it mean by \"error: not declared in this scope?\"", sentenceList.get(0));
    }
    
    @Test
    public void testCodeExtraction() {
        for (int i=0; i<bodies.length; i++) {
            List<String> snippetList = SoHtmlContentExtractor.extractCode(bodies[i]);
            assertEquals(Arrays.asList(expectedCode[i]), snippetList);
        }
    }

    @Test
    public void testLanguageDetection() throws IOException {
        for (String html : bodies) {
            Language l = extractor.getTopLanguage(html);
            assertEquals("eng", l.getLang());
        }
    }
    
    @Test
    public void testTokenExtraction() {
        String[] sentences = {
                "This is a sentence.",
                "Is this a sentence?"
        };
        List<String> tokens = extractor.extractTokens(Arrays.asList(sentences));
        String result = tokens.stream()
                .collect(Collectors.joining("-", "{", "}"));
        assertEquals("{This-is-a-sentence-.-Is-this-a-sentence-?}", result);
    }
    
    @Test
    public void testPosTagging() {
        List<String> sentenceList = new LinkedList<String>();
        sentenceList.add("John has a sister named Penny.");
        List<String> tokenList = extractor.extractTokens(sentenceList);
        String tags[] = extractor.getPosTaggers(tokenList);
      
        System.out.println(Arrays.toString(tags));
        assertTrue(Arrays.asList(tags).containsAll(Arrays.asList("NNP", "VBZ", "DT", "NN", "VBN", "NNP", ".")));
    }
    
    @Test
    public void testLemmatize() {
        List<String> sentenceList = new LinkedList<String>();
        sentenceList.add("John has a sister named Penny.");
        List<String> tokenList = extractor.extractTokens(sentenceList);
        String tags[] = extractor.getPosTaggers(tokenList);
        String lemmas[] = extractor.lemmatize(tokenList.toArray(new String[tokenList.size()]), tags);
      
        System.out.println(Arrays.toString(lemmas));
        assertTrue(Arrays.asList(lemmas).containsAll(Arrays.asList("O", "have", "a", "sister", "name", "O", "O")));
    }
    
    @Test
    public void testNGram() {
        List<String> sentenceList = new LinkedList<String>();
        sentenceList.add("John has a sister named Penny.");
        List<String> tokenList = extractor.extractTokens(sentenceList);
        
        List<String> nGramList = NGramGenerator.generate(tokenList, 2, "_");

        for (String nGram: nGramList) {
            System.out.println(nGram);
        }
        
        System.out.println("-----------------");
        String[] tokens = tokenList.toArray(new String[tokenList.size()]);
        NGramModel nGramModel = new NGramModel();
        nGramModel.add(new StringList(tokens), 1, 3);

        System.out.println("Total ngrams: " + nGramModel.numberOfGrams());
        for (StringList ngram : nGramModel) {
            System.out.println(nGramModel.getCount(ngram) + " - " + ngram);
        }
    }
}
