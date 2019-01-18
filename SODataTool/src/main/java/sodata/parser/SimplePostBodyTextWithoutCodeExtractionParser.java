package sodata.parser;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SimplePostBodyTextWithoutCodeExtractionParser {
    private static Pattern p = Pattern.compile("\\w+", Pattern.UNICODE_CHARACTER_CLASS);
    private static Matcher m;

    private static String[] originalWordSplit(String body) {
        m = p.matcher(body);
        List<String> wordList = new LinkedList<String>();
        while (m.find()) {
            wordList.add(m.group());
        }
        return wordList.toArray(new String[wordList.size()]);
    }

    public static String[] getWordsFromPostText(String body) {
        body = removeTextBetweenCodeTags(body);
        body = removeHTMLTags(body);
        return originalWordSplit(body);
    }

    private static String removeTextBetweenCodeTags(String body) {
        int location1 = body.indexOf("<code>");
        int location2 = -1;
        while(location1 != -1) {
            location2 = body.indexOf("</code>") + "</code>".length();
            String newBody1 = body.substring(0, location1);

            // If location2 ends up being -1, then this will throw an exception. That's desired behavior, since this
            // method won't be able to work properly if the tags aren't closed correctly.
            String newBody2 = body.substring(location2, body.length());
            body = newBody1 + newBody2;
            location1 = body.indexOf("<code>");
        }
        return body;
    }

    private static String removeHTMLTags(String body) {
        // Note: This might not work in all cases, but it seems to do well in testing so far.
        return body.replaceAll("\\<.*?\\>", "");
    }
}
