package sodata.parser;

import static org.junit.Assert.*;

import org.junit.Test;

public class SimplePostTextPunctuationParserTest {
    @Test
    public void testGetWordsFromPostBody() {
        String[] bodies =
            {
                "<p>Aha!Yahoo!</p>" +
                "<p>This is a quoted \"word\"" +
                "class#subclass#subsubclass" +
                "object$object$$object\n" +
                "a=123;" +
                "mymethod(a,b,c)" +
                "b[3]=\"literal\"" +
                "123-234+635;" +
                "abc'bcd'" +
                "a123?b456?" +
                "myaddress@aol.com" +
                "z/x\\sx" +
                "{abc; if (con|zcon){dosomething}" +
                "Yellow`Fox`Jump^Over^The^Fence_It_is-a-thing(yes)no[NO]YES"+
                "zyx[1]xyz{2}"
                ,
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
        for (String body:bodies) {
            String[] words = SimplePostBodyPunctuationParser.getWordsFromPostBody(body);
            assertTrue(words.length >= 0);
            for (String word:words) {
                System.out.println("[" + word + "]");
            }
            System.out.println("----------------");            
        }
    }
}
