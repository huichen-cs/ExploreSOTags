/**
 * This parser splits long strings into "words". However, it is based on Regular Expression
 * prefined character classes \p{Punct}, and works well only for ASCII characters.
 */
package sodata.parser;

import java.util.regex.Pattern;

public class SimplePostBodyPunctuationParser {
    /* 
     * Posix punctuation are !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~; however,
     * some of them, such as `^~<=> are not in Unicode punctuations
     */
    
    public static Pattern WORD_DELIMITER_PATTERN 
        = Pattern.compile("[`^<=>|_\\-()\\[\\]\\{\\}!\"#\\$+\\p{Punct}\\s]+", 
                Pattern.UNICODE_CHARACTER_CLASS);
    
    public static String[] getWordsFromPostBody(String text) {
        return WORD_DELIMITER_PATTERN.split(text);
    }
}
