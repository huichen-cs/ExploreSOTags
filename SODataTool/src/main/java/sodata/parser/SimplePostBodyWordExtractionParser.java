package sodata.parser;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SimplePostBodyWordExtractionParser {
    private static Pattern p = Pattern.compile("\\w+", Pattern.UNICODE_CHARACTER_CLASS);
    private static Matcher m;

    public static String[] getWordsFromPostText(String body) {
        m = p.matcher(body);
        List<String> wordList = new LinkedList<String>();
        while (m.find()) {
            wordList.add(m.group());
        }
        return wordList.toArray(new String[wordList.size()]);
    }
    
    public static String[] getWordsFromPostText(String body, int n) {
        m = p.matcher(body);
        List<String> wordList = new LinkedList<String>();
        while (m.find()) {
            wordList.add(m.group());
        }
        
        List<String> nGramList = new LinkedList<String>();
        for (int i=0; i<wordList.size()-n+1; i++) {
        	String nGram = wordList.get(i);
        	for (int j=i; j<i+n; j++) {
        		nGram += " " + wordList.get(j);
        	}
        	nGramList.add(nGram);
        }
        
        wordList.addAll(nGramList);
        
        return wordList.toArray(new String[wordList.size()]);
    }
}
