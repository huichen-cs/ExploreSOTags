package sodata.testpreparation;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.database.DbUtils;

public class TestDbBuilder {
    private final static Logger LOGGER = LoggerFactory.getLogger(TestDbBuilder.class);
    
    public final static String[] TEST_SODATA_FILENAMES = { "../SOResults/testdata/post_text_17054.txt",
                    "../SOResults/testdata/post_text_2041.txt", "../SOResults/testdata/post_text_66542.txt",
                    "../SOResults/testdata/post_text_17612.txt", "../SOResults/testdata/post_text_23063.txt" };
    public final static String[] TEST_SODATA_POSTTAGS_FILENAMES = {"../SOResults/testdata/post_tag_ids_17054.txt",
            "../SOResults/testdata/post_tag_ids_2041.txt", "../SOResults/testdata/post_tag_ids_66542.txt",
            "../SOResults/testdata/post_tag_ids_17612.txt", "../SOResults/testdata/post_tag_ids_23063.txt" };
    public final static String TEST_SODATA_TAGS_FILENAME = "../SOResults/testdata/tags_2041_66542_17054_17612_23063.txt";
    public final static long[] TEST_SODATA_POSTIDS = { 17054, 2041, 66542, 17612, 23063 };  
    public final static String[] TEST_SODATA_CREATIONDATES = {
            "2017-03-17 20:15:23", "2017-03-17 21:15:23", "2017-03-18 20:15:23",
            "2017-03-18 01:15:23", "2017-03-19 20:15:23", "2017-03-20 20:15:23"
    };
    public final static String[][] TEST_SODATA_VOCAB = {
            { "0", "a", "a", "2", "3" },
            { "1", "Any", "Any", "1", "1" },
            { "2", "bin", "bin", "1", "1" },
            { "3", "branch", "branch", "1", "2" },
            { "4", "capistrano", "capistrano", "1", "1" },
            { "5", "comparable", "comparable", "1", "1" },
            { "6", "course", "course", "1", "1" },
            { "7", "create", "create", "1", "2" },
            { "8", "delete", "delete", "1", "1" },
            { "9", "do", "do", "4", "6" },
            { "10", "file", "file", "1", "1" },
            { "11", "for", "for", "1", "1" },
            { "12", "from", "from", "1", "1" },
            { "13", "get", "get", "1", "1" },
            { "14", "How", "How", "4", "6" },
            { "15", "I", "I", "2", "4" },
            { "16", "in", "in", "3", "5" },
            { "17", "instead", "instead", "1", "1" },
            { "18", "Lisp", "Lisp", "1", "1" },
            { "19", "net", "net", "1", "1" },
            { "20", "network", "network", "1", "1" },
            { "21", "of", "of", "1", "2" },
            { "22", "Oracle", "Oracle", "1", "1" },
            { "23", "p", "p", "5", "10" },
            { "24", "Pascal", "Pascal", "1", "2" },
            { "25", "place", "place", "1", "1" },
            { "26", "plsql", "plsql", "1", "1" },
            { "27", "procedure", "procedure", "1", "1" },
            { "28", "Programmatic", "Programmatic", "1", "1" },
            { "29", "recycle", "recycle", "1", "1" },
            { "30", "run", "run", "1", "1" },
            { "31", "See", "See", "1", "1" },
            { "32", "sockets", "sockets", "1", "1" },
            { "33", "Sockets", "Sockets", "1", "1" },
            { "34", "solution", "solution", "1", "1" },
            { "35", "started", "started", "1", "1" },
            { "36", "SVN", "SVN", "1", "2" },
            { "37", "title", "title", "1", "1" },
            { "38", "use", "use", "1", "1" },
            { "39", "windows", "windows", "1", "1" },
            { "40", "you", "you", "2", "2" }
    };
    public final static String[][] TEST_SODATA_VOCAB_IGNORECASE = {
            { "0", "a", "a", "2", "3" },
            { "1", "any", "any", "1", "1" },
            { "2", "bin", "bin", "1", "1" },
            { "3", "branch", "branch", "1", "2" },
            { "4", "capistrano", "capistrano", "1", "1" },
            { "5", "comparable", "comparable", "1", "1" },
            { "6", "course", "course", "1", "1" },
            { "7", "create", "create", "1", "2" },
            { "8", "delete", "delete", "1", "1" },
            { "9", "do", "do", "4", "6" },
            { "10", "file", "file", "1", "1" },
            { "11", "for", "for", "1", "1" },
            { "12", "from", "from", "1", "1" },
            { "13", "get", "get", "1", "1" },
            { "14", "how", "how", "4", "6" },
            { "15", "i", "i", "2", "4" },
            { "16", "in", "in", "3", "5" },
            { "17", "instead", "instead", "1", "1" },
            { "18", "lisp", "lisp", "1", "1" },
            { "19", "net", "net", "1", "1" },
            { "20", "network", "network", "1", "1" },
            { "21", "of", "of", "1", "2" },
            { "22", "oracle", "oracle", "1", "1" },
            { "23", "p", "p", "5", "10" },
            { "24", "pascal", "pascal", "1", "2" },
            { "25", "place", "place", "1", "1" },
            { "26", "plsql", "plsql", "1", "1" },
            { "27", "procedure", "procedure", "1", "1" },
            { "28", "programmatic", "programmatic", "1", "1" },
            { "29", "recycle", "recycle", "1", "1" },
            { "30", "run", "run", "1", "1" },
            { "31", "see", "see", "1", "1" },
            { "32", "sockets", "sockets", "1", "2" },
            { "33", "solution", "solution", "1", "1" },
            { "34", "started", "started", "1", "1" },
            { "35", "svn", "svn", "1", "2" },
            { "36", "title", "title", "1", "1" },
            { "37", "use", "use", "1", "1" },
            { "38", "windows", "windows", "1", "1" },
            { "39", "you", "you", "2", "2" }
    };    
    public final static long[][] TEST_SODATA_QW = {
            { 2041, 0, 2 },
            { 2041, 3, 2 },
            { 2041, 7, 2 },
            { 2041, 9, 2 },
            { 2041, 14, 2 },
            { 2041, 15, 2 },
            { 2041, 16, 2 },
            { 2041, 23, 2 },
            { 2041, 36, 2 },
            { 17054, 9, 1 },
            { 17054, 14, 1 },
            { 17054, 16, 2 },
            { 17054, 20, 1 },
            { 17054, 23, 2 },
            { 17054, 24, 2 },
            { 17054, 32, 1 },
            { 17054, 33, 1 },
            { 17054, 38, 1 },
            { 17054, 40, 1 },
            { 17612, 0, 1 },
            { 17612, 2, 1 },
            { 17612, 6, 1 },
            { 17612, 8, 1 },
            { 17612, 9, 1 },
            { 17612, 10, 1 },
            { 17612, 14, 1 },
            { 17612, 16, 1 },
            { 17612, 17, 1 },
            { 17612, 21, 2 },
            { 17612, 23, 2 },
            { 17612, 25, 1 },
            { 17612, 28, 1 },
            { 17612, 29, 1 },
            { 17612, 34, 1 },
            { 17612, 40, 1 },
            { 23063, 1, 1 },
            { 23063, 4, 1 },
            { 23063, 5, 1 },
            { 23063, 11, 1 },
            { 23063, 19, 1 },
            { 23063, 23, 2 },
            { 23063, 31, 1 },
            { 23063, 37, 1 },
            { 23063, 39, 1 },
            { 66542, 9, 2 },
            { 66542, 12, 1 },
            { 66542, 13, 1 },
            { 66542, 14, 2 },
            { 66542, 15, 2 },
            { 66542, 18, 1 },
            { 66542, 22, 1 },
            { 66542, 23, 2 },
            { 66542, 26, 1 },
            { 66542, 27, 1 },
            { 66542, 30, 1 },
            { 66542, 35, 1 }
    };
    public final static long[][] TEST_SODATA_QW_IGNORECASE = {
            { 2041, 0, 2 },
            { 2041, 3, 2 },
            { 2041, 7, 2 },
            { 2041, 9, 2 },
            { 2041, 14, 2 },
            { 2041, 15, 2 },
            { 2041, 16, 2 },
            { 2041, 23, 2 },
            { 2041, 35, 2 },
            { 17054, 9, 1 },
            { 17054, 14, 1 },
            { 17054, 16, 2 },
            { 17054, 20, 1 },
            { 17054, 23, 2 },
            { 17054, 24, 2 },
            { 17054, 32, 2 },
            { 17054, 37, 1 },
            { 17054, 39, 1 },
            { 17612, 0, 1 },
            { 17612, 2, 1 },
            { 17612, 6, 1 },
            { 17612, 8, 1 },
            { 17612, 9, 1 },
            { 17612, 10, 1 },
            { 17612, 14, 1 },
            { 17612, 16, 1 },
            { 17612, 17, 1 },
            { 17612, 21, 2 },
            { 17612, 23, 2 },
            { 17612, 25, 1 },
            { 17612, 28, 1 },
            { 17612, 29, 1 },
            { 17612, 33, 1 },
            { 17612, 39, 1 },
            { 23063, 1, 1 },
            { 23063, 4, 1 },
            { 23063, 5, 1 },
            { 23063, 11, 1 },
            { 23063, 19, 1 },
            { 23063, 23, 2 },
            { 23063, 31, 1 },
            { 23063, 36, 1 },
            { 23063, 38, 1 },
            { 66542, 9, 2 },
            { 66542, 12, 1 },
            { 66542, 13, 1 },
            { 66542, 14, 2 },
            { 66542, 15, 2 },
            { 66542, 18, 1 },
            { 66542, 22, 1 },
            { 66542, 23, 2 },
            { 66542, 26, 1 },
            { 66542, 27, 1 },
            { 66542, 30, 1 },
            { 66542, 34, 1 }
    };    
    public final static long[] TEST_SODATA_QID = {2041, 17054, 17612, 23063, 66542};
    public final static long[][] TEST_SODATA_QUESTIONTAG = {
            { 2041,5 },
            { 2041,10 },
            { 2041,12 },
            { 2041,18 },
            { 17054,4 },
            { 17054,11 },
            { 17054,13 },
            { 17612,0 },
            { 17612,1 },
            { 17612,2 },
            { 17612,6 },
            { 17612,9 },
            { 23063,0 },
            { 23063,6 },
            { 23063,7 },
            { 23063,16 },
            { 66542,3 },
            { 66542,8 },
            { 66542,14 },
            { 66542,15 },
            { 66542,17 }            
    };
    public final static String[][] TEST_SODATA_TAG = {
            { ".net", "0"},
            { "c#","1" },
            { "c++","2" },
            { "lisp","3" },
            { "sockets","4" },
            { "svn","5" },
            { "windows","6" },
            { "deployment","7" },
            { "oracle","8" },
            { "io","9" },
            { "version-control","10" },
            { "networking","11" },
            { "branch","12" },
            { "pascal","13" },
            { "plsql","14" },
            { "stored-procedures","15" },
            { "capistrano","16" },
            { "clojure","17" },
            { "branching-and-merging","18" }
    };
    public final static String[] TEST_SODATA_WVOC_FILE_LINES = {
            "a",
            "Any",
            "bin",
            "branch",
            "capistrano",
            "comparable",
            "course",
            "create",
            "delete",
            "do",
            "file",
            "for",
            "from",
            "get",
            "How",
            "I",
            "in",
            "instead",
            "Lisp",
            "net",
            "network",
            "of",
            "Oracle",
            "p",
            "Pascal",
            "place",
            "plsql",
            "procedure",
            "Programmatic",
            "recycle",
            "run",
            "See",
            "sockets",
            "Sockets",
            "solution",
            "started",
            "SVN",
            "title",
            "use",
            "windows",
            "you"
    };
    public final static String[] TEST_SODATA_LVOC_FILE_LINES = {
             ".net",
             "c#",
             "c++",
             "lisp",
             "sockets",
             "svn",
             "windows",
             "deployment",
             "oracle",
             "io",
             "version-control",
             "networking",
             "branch",
             "pascal",
             "plsql",
             "stored-procedures",
             "capistrano",
             "clojure",
             "branching-and-merging"
    };
    public final static String[] TEST_SODATA_DOCTERMFREQ_FILE_LINES = {
            "9 0:2 3:2 7:2 9:2 14:2 15:2 16:2 23:2 36:2",
            "10 9:1 14:1 16:2 20:1 23:2 24:2 32:1 33:1 38:1 40:1",
            "16 0:1 2:1 6:1 8:1 9:1 10:1 14:1 16:1 17:1 21:2 23:2 25:1 28:1 29:1 34:1 40:1",
            "9 1:1 4:1 5:1 11:1 19:1 23:2 31:1 37:1 39:1",
            "12 9:2 12:1 13:1 14:2 15:2 18:1 22:1 23:2 26:1 27:1 30:1 35:1"
    };
    public final static String[] TEST_SODATA_DOCLABEL_FILE_LINES = {
            "2041\t5\t10\t12\t18",
            "17054\t4\t11\t13",
            "17612\t0\t1\t2\t6\t9",
            "23063\t0\t6\t7\t16",
            "66542\t3\t8\t14\t15\t17" 
    };    
    
    
    public final static long[] TEST_SODATA_LIVE_QID = {2041, 17054, 17612, 23063, 66542};
    public final static long[][] TEST_SODATA_LIVE_QUESTIONTAG = {
            { 2041 ,    63 },
            { 2041 ,   456 },
            { 2041 ,  1879 },
            { 2041 , 53847 },
            { 17054 ,   35 },
            { 17054 ,   794 },
            { 17054 ,  1949 },
            { 17612 ,     1 },
            { 17612 ,     9 },
            { 17612 ,    10 },
            { 17612 ,    64 },
            { 17612 ,   345 },
            { 23063 ,     1 },
            { 23063 ,    64 },
            { 23063 ,   190 },
            { 23063 ,  2831 },
            { 66542 ,    14 },
            { 66542 ,   194 },
            { 66542 ,  2148 },
            { 66542 ,  2494 },
            { 66542 ,  6525 }
    };
    public final static String[][] TEST_SODATA_LIVE_TAG = {
            { ".net", "1" },
            { "c#", "9" },
            { "c++", "10" },
            { "lisp", "14" },
            { "sockets", "35" },
            { "svn", "63" },
            { "windows", "64" },
            { "deployment", "190" },
            { "oracle", "194" },
            { "io", "345" },
            { "version-control", "456" },
            { "networking", "794" },
            { "branch", "1879" },
            { "pascal", "1949" },
            { "plsql", "2148" },
            { "stored-procedures", "2494" },
            { "capistrano", "2831" },
            { "clojure", "6525" },
            { "branching-and-merging", "53847" }
    };
    public final static String[] TEST_SODATA_LIVE_WVOC_FILE_LINES = {
            "a",
            "Any",
            "bin",
            "branch",
            "capistrano",
            "comparable",
            "course",
            "create",
            "delete",
            "do",
            "file",
            "for",
            "from",
            "get",
            "How",
            "I",
            "in",
            "instead",
            "Lisp",
            "net",
            "network",
            "of",
            "Oracle",
            "p",
            "Pascal",
            "place",
            "plsql",
            "procedure",
            "Programmatic",
            "recycle",
            "run",
            "See",
            "sockets",
            "Sockets",
            "solution",
            "started",
            "SVN",
            "title",
            "use",
            "windows",
            "you"
    };
    public final static String[] TEST_SODATA_LIVE_LVOC_FILE_LINES = {
             ".net",
             "c#",
             "c++",
             "lisp",
             "sockets",
             "svn",
             "windows",
             "deployment",
             "oracle",
             "io",
             "version-control",
             "networking",
             "branch",
             "pascal",
             "plsql",
             "stored-procedures",
             "capistrano",
             "clojure",
             "branching-and-merging"
    };
    public final static String[] TEST_SODATA_LIVE_DOCTERMFREQ_FILE_LINES = {
            "9 0:2 3:2 7:2 9:2 14:2 15:2 16:2 23:2 36:2",
            "10 9:1 14:1 16:2 20:1 23:2 24:2 32:1 33:1 38:1 40:1",
            "16 0:1 2:1 6:1 8:1 9:1 10:1 14:1 16:1 17:1 21:2 23:2 25:1 28:1 29:1 34:1 40:1",
            "9 1:1 4:1 5:1 11:1 19:1 23:2 31:1 37:1 39:1",
            "12 9:2 12:1 13:1 14:2 15:2 18:1 22:1 23:2 26:1 27:1 30:1 35:1"
    };
    public final static String[] TEST_SODATA_LIVE_DOCLABEL_FILE_LINES = {
            "2041\t5\t10\t12\t18",
            "17054\t4\t11\t13",
            "17612\t0\t1\t2\t6\t9",
            "23063\t0\t6\t7\t16",
            "66542\t3\t8\t14\t15\t17" 
    };    

    
    
    
    
    public final static String[] TEST_1WORD_FILENAMES = { "../SOResults/testdata/one_word.txt" };
    public final static long[] TEST_1WORD_POSTIDS = { 1 };
    public final static String TEST_1WORD_TAGS_FILENAME = "../SOResults/testdata/one_word_tags.txt";
    public final static String[] TEST_1WORD_POSTTAGS_FILENAMES = { "../SOResults/testdata/one_word_post_tag.txt" };
    public final static String[] TEST_1WORD_CREATIONDATES = {
            "2017-03-17 20:15:23"
    };
    public final static String[][] TEST_1WORD_VOCAB = {
            { "0", "word", "word", "1", "1" }
    };
    public final static long[][] TEST_1WORD_QW = {
            { 1, 0, 1 }
    };
    public final static long[] TEST_1WORD_QID = {1};
    public final static String TEST_1WORD_STARTDATE_1 = "2017-03-17 00:00:00";
    public final static long[][] TEST_1WORD_QUESTIONTAG = {
            {1, 0}
    };
    public final static String[][] TEST_1WORD_TAG = {
            {"TEST_1WORD_TAGS_FILENAME", "0"}
    };
    public final static String[] TEST_1WORD_WVOC_FILE_LINES = {
            "word"
    };
    public final static String[] TEST_1WORD_LVOC_FILE_LINES = {
            "TEST_1WORD_TAGS_FILENAME"
    };
    public final static String[] TEST_1WORD_DOCTERMFREQ_FILE_LINES = {
            "1 0:1"
    };
    public final static String[] TEST_1WORD_DOCLABEL_FILE_LINES = {
            "1\t0"
    };
    
    
    public final static String[] TEST_1W2DS_FILENAMES = { "../SOResults/testdata/one_word_1.txt", "../SOResults/testdata/one_word_2.txt" };
    public final static long[] TEST_1W2DS_POSTIDS = { 1, 2 };
    public final static String TEST_1W2DS_TAGS_FILENAME = "../SOResults/testdata/one_word_tags_1_2.txt";
    public final static String[] TEST_1W2DS_POSTTAGS_FILENAMES = { 
            "../SOResults/testdata/one_word_1_tag.txt",
            "../SOResults/testdata/one_word_2_tag.txt"
    };
    public final static String[] TEST_1W2DS_CREATIONDATES = {
            "2017-03-17 20:15:23", "2017-03-19 20:15:23"
    };
    public final static String[][] TEST_1W2DS_VOCAB = {
            { "0", "word", "word", "2", "3" }
    };
    public final static long[][] TEST_1W2DS_QW = {
            { 1, 0, 1 },
            { 2, 0, 2 }
    };  
    public final static String TEST_1W2DS_STARTDATE_0 = "2017-03-16 20:15:23";
    public final static String TEST_1W2DS_STARTDATE_1 = "2017-03-18 20:15:23";
    public final static String[][] TEST_1W2DS_VOCAB_SD_1 = {
            { "0", "word", "word", "1", "2" }
    };
    public final static long[][] TEST_1W2DS_QW_SD_1 = {
            { 2, 0, 2 }
    }; 
    public final static String TEST_1W2DS_ENDDATE_0 = "2017-04-18 20:15:23";
    public final static String TEST_1W2DS_ENDDATE_1 = "2017-03-18 20:15:23";
    public final static String[][] TEST_1W2DS_VOCAB_ED_1 = {
            { "0", "word", "word", "1", "1" }
    };
    public final static long[][] TEST_1W2DS_QW_ED_1 = {
            { 1, 0, 1 }
    };     
    public final static long[] TEST_1W2DS_QID = { 1, 2 };
    public final static long[][] TEST_1W2DS_QUESTIONTAG = {
            {1, 0},
            {2, 1}
    };
    public final static String[][] TEST_1W2DS_TAG = {
            {"one_word_1.txt", "0"},
            {"one_word_2.txt", "1"}
    };
    public final static String[] TEST_1W2DS_WVOC_FILE_LINES = {
            "word"
    };
    public final static String[] TEST_1W2DS_LVOC_FILE_LINES = {
            "one_word_1.txt",
            "one_word_2.txt"
    };
    public final static String[] TEST_1W2DS_DOCTERMFREQ_FILE_LINES = {
            "1 0:1",
            "1 0:2"
    };
    public final static String[] TEST_1W2DS_DOCLABEL_FILE_LINES = {
            "1\t0",
            "2\t1"
    };
    
    
    public final static String[] TEST_1REPETITIVE_WORD_FILENAMES = { "../SOResults/testdata/one_repetitive_word.txt" };
    public final static long[] TEST_1REPETITIVE_WORD_POSTIDS = { 1 };
    public final static String TEST_1REPETITIVE_WORD_TAGS_FILENAME = "../SOResults/testdata/one_word_tags.txt";
    public final static String[] TEST_1REPETITIVE_WORD_POSTTAGS_FILENAMES = { "../SOResults/testdata/one_word_post_tag.txt" };
    
    public final static String[] TEST_1REPETITIVE_WORD_CREATIONDATES = {
            "2017-03-17 20:15:23"
    };
    public final static String[][] TEST_1REPETITIVE_WORD_VOCAB = {
            { "0", "word", "word", "1", "2" }
    };
    public final static long[][] TEST_1REPETITIVE_WORD_QW = {
            { 1, 0, 2 }
    };
    
    
    public final static String[] TEST_2WORDS_FILENAMES = { "../SOResults/testdata/two_word.txt" };
    public final static long[] TEST_2WORDS_POSTIDS = { 1 };
    public final static String TEST_2WORDS_TAGS_FILENAME = "../SOResults/testdata/one_word_tags.txt";
    public final static String[] TEST_2WORDS_POSTTAGS_FILENAMES = { "../SOResults/testdata/one_word_post_tag.txt" };
    
    public final static String[] TEST_2WORDS_CREATIONDATES = {
            "2017-03-17 20:15:23"
    };    
    public final static String[][] TEST_2WORDS_VOCAB = {
            { "0", "two", "two", "1", "2" },
            { "1", "word", "word", "1", "2" }
    };
    public final static long[][] TEST_2WORDS_QW = {
            { 1, 0, 2 },
            { 1, 1, 2 }
    };

    
    
    public final static String[] TEST_2W2DS_FILENAMES = { "../SOResults/testdata/two_word_1.txt",
            "../SOResults/testdata/two_word_2.txt"
            };
    public final static long[] TEST_2W2DS_POSTIDS = { 1, 2 };
    public final static String TEST_2W2DS_TAGS_FILENAME = "../SOResults/testdata/two_word_tags_1_2.txt";
    public final static String[] TEST_2W2DS_POSTTAGS_FILENAMES = { 
            "../SOResults/testdata/two_word_1_tag.txt",
            "../SOResults/testdata/two_word_2_tag.txt"
    };    
    public final static String[] TEST_2W2DS_CREATIONDATES = {
            "2017-03-17 20:15:23", "2017-03-19 20:15:23"
    };    
    public final static String[][] TEST_2W2DS_VOCAB = {
            { "0", "two", "two", "2", "4" },
            { "1", "word", "word", "2", "5" }
    };
    public final static long[][] TEST_2W2DS_QW = {
            { 1, 0, 2 },
            { 1, 1, 2 },
            { 2, 0, 2 },
            { 2, 1, 3 }
    };    
    public final static long[] TEST_2W2DS_QID = { 1, 2 };
    public final static long[][] TEST_2W2DS_QUESTIONTAG = {
            {1, 0},
            {2, 1}
    };
    public final static String[][] TEST_2W2DS_TAG = {
            {"two_word_1.txt", "0"},
            {"two_word_2.txt", "1"}
    };
    public final static String[] TEST_2W2DS_WVOC_FILE_LINES = {
            "two",
            "word"
    };
    public final static String[] TEST_2W2DS_LVOC_FILE_LINES = {
            "two_word_1.txt",
            "two_word_2.txt"
    };
    public final static String[] TEST_2W2DS_DOCTERMFREQ_FILE_LINES = {
            "2 0:2 1:2",
            "2 0:2 1:3"
    };
    public final static String[] TEST_2W2DS_DOCLABEL_FILE_LINES = {
            "1\t0",
            "2\t1"
    };
    
    public final static String JAVA_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public final static String SQL_TIMESTAMP_FORMAT = "yyyy-mm-dd hh24:mi:ss";
    
    public static boolean populatesTestDb(String testDbPropertiesFilename, String srcDbPropertiesFilename, long[] postIds) {

        try (Connection srcConn = DbUtils.connect(srcDbPropertiesFilename);
                Connection dstConn = DbUtils.connect(testDbPropertiesFilename)) {
            dstConn.setAutoCommit(false);
            if (!purgeWorkingTables(dstConn)) {
                dstConn.rollback();
                return false;
            }
            
            if (!populatePostsTable(dstConn, srcConn, postIds)) {
                return false;
            }
            
            if (!populatePostTypesTable(dstConn)) {
                return false;
            }
            
            if (!populateTagsTable(dstConn, srcConn, postIds)) {
                return false;
            }
            
            if (!populatePostTagsTable(dstConn, srcConn, postIds)) {
                return false;
            }            
            
            dstConn.commit();
            return true;
        } catch (SQLException e) {
            LOGGER.error("Exception encountered", e);
            return false;            
        }
        
    }
    
    private static boolean populatePostTagsTable(Connection dstConn, Connection srcConn, long[] postIds) {
        try (PreparedStatement pstmt = dstConn.prepareStatement("DELETE FROM posttags")) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("error", e);
            return false;            
        }

        String sqlInsert = "INSERT INTO posttags (postid, tagid) VALUES (?,?)";
        String sqlSelect 
        = " SELECT postid,tagid FROM posttags WHERE postid in ( ";
        boolean isFirst = true;
        for (long postId : postIds) {
            if (isFirst) {
                sqlSelect += Long.toString(postId);
                isFirst = false;
            } else {
                sqlSelect += "," + Long.toString(postId);
            }
        }
        sqlSelect += " ) ";
        
        
        try (PreparedStatement dstPstmt = dstConn.prepareStatement(sqlInsert);
                PreparedStatement srcPstmt = srcConn.prepareStatement(sqlSelect);
                ResultSet srcRs = srcPstmt.executeQuery()) {
            while (srcRs.next()) {
                long postId = srcRs.getLong(1);
                long tagId = srcRs.getLong(2);
                dstPstmt.setLong(1,  postId);
                dstPstmt.setLong(2,  tagId);
                dstPstmt.addBatch();
            }
            dstPstmt.executeBatch();
            return true;
        } catch (SQLException e) {
            LOGGER.error("error", e);
            return false;
        } 
    }

    private static boolean populateTagsTable(Connection dstConn, Connection srcConn, long[] postIds) {
        try (PreparedStatement pstmt = dstConn.prepareStatement("DELETE FROM tags")) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("error", e);
            return false;            
        }
        
        String sqlInsert = "INSERT INTO tags (id, tagname) VALUES (?,?)";
        String sqlSelect 
            = " SELECT DISTINCT t.id,t.tagname "
            + " FROM posttags AS pt,tags AS t "
            + " WHERE pt.tagid=t.id AND pt.postid in ( ";
        boolean isFirst = true;
        for (long postId: postIds) {
            if (isFirst) {
                sqlSelect += Long.toString(postId);
                isFirst = false;
            } else {
                sqlSelect += "," + Long.toString(postId);
            }
        }
        sqlSelect += " ) ORDER BY t.id";

        try (PreparedStatement dstPstmt = dstConn.prepareStatement(sqlInsert);
                PreparedStatement srcPstmt = srcConn.prepareStatement(sqlSelect);
                ResultSet srcRs = srcPstmt.executeQuery()) {
            while (srcRs.next()) {
                long tagId = srcRs.getLong(1);
                String tagName = srcRs.getString(2);
                
                dstPstmt.setLong(1, tagId);
                dstPstmt.setString(2, tagName);
                dstPstmt.addBatch();
            }
            dstPstmt.executeBatch();
            return true;
        } catch (SQLException e) {
            LOGGER.error("failed to populate tags table.", e);
            return false;
        } 
    }

    public static boolean populatePostsTable(Connection dstConn, Connection srcConn, long[] postIds) {
        try (PreparedStatement pstmt = dstConn.prepareStatement("DELETE FROM posts")) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("error", e);
            return false;            
        }        
        
        String sqlSelect 
            = " SELECT id,posttypeid,title,body,creationdate "
            + " FROM posts WHERE id in ( ";
        boolean isFirst = true;
        for (long id:postIds) {
            if (isFirst) {
                sqlSelect += Long.toString(id);
                isFirst = false;
            } else {
                sqlSelect += "," + Long.toString(id);
            }
        }
        sqlSelect = sqlSelect + " ) ";
        
        String sqlInsert = "INSERT INTO posts (id, posttypeid, creationdate, title, body) VALUES (?,?,?,?,?)";
        try (PreparedStatement dstPstmt = dstConn.prepareStatement(sqlInsert);
                PreparedStatement srcPstmt = srcConn.prepareStatement(sqlSelect);
                ResultSet srcRs = srcPstmt.executeQuery()) {
            while (srcRs.next()) {
                long id = srcRs.getLong(1);
                long postTypeId = srcRs.getLong(2);
                String title = srcRs.getString(3);
                String body = srcRs.getString(4);
                Timestamp timestamp = srcRs.getTimestamp(5);

                dstPstmt.setLong(1, id);
                dstPstmt.setLong(2, postTypeId);
                dstPstmt.setTimestamp(3, timestamp);
                dstPstmt.setString(4, title);
                dstPstmt.setString(5, body);
                dstPstmt.addBatch();
            }
            dstPstmt.executeBatch();
            return true;
        } catch (SQLException e) {
            LOGGER.info("Cannot populate posts table.", e);
            return false;
        }
    }
            
    
    public static boolean populateTestDb(String dbPropertiesFilename, 
            String[] testDataFilenames,
            long[] testPostIds,
            String testTagsFilename, 
            String[] testPostTagsFilenames,
            String[] testCreationDates) {
        /*
         * First create test user sodumptestuser with password "test" 
         *      # createuser -U postgres -W -D -P sodumptestuser 
         *      # createdb -U postgres -W -O sodumptestuser sodumptestdb 
         *      # ppg_dump -s -U sodumpuser sodumpdb > schema.txt 
         *      # psql -U sodumptestuser sodumptestdb < schema.txt
         */

        Connection conn = null;
        
        try {
            conn = DbUtils.connect(dbPropertiesFilename);
            conn.setAutoCommit(false);
            
            if (!purgeWorkingTables(conn)) {
                return false;
            }
            
            if (!populatePostsTable(conn, testDataFilenames, testPostIds, testCreationDates)) {
                return false;
            }

            if (!populatePostTypesTable(conn)) {
                return false;
            }
            
            if (!populateTagsTable(conn, testTagsFilename)) {
                return false;
            }
            
            if (!populatePostTagsTable(conn, testPostTagsFilenames)) {
                return false;
            }
            conn.commit();
            return true;            
        } catch (SQLException e) {
            LOGGER.error("failed to create testdb.", e);
            return false;
        } finally {
            if (conn != null) {
                try {
                conn.rollback();
                conn.close();
                } catch (SQLException e) {
                    LOGGER.info("error.", e);
                }
            }
        }
    }

    private static boolean populatePostsTable(Connection conn, String[] testDataFilenames, long[] testPostIds, String[] testCreationDates) {
        final String sql_inst_posts_row = "INSERT INTO posts (id, posttypeid, creationdate, title, body) VALUES (?,1,?,?,?)";
        
        try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM posts")) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("error", e);
            return false;            
        }

        try (PreparedStatement pstmt = conn.prepareStatement(sql_inst_posts_row)) {
            int index = 0;
            for (String fn : testDataFilenames) {
                String title = "";
                String body = "";
                int lineNum = 0;
                String line;
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fn), StandardCharsets.UTF_8));

                while ((line = br.readLine()) != null) {
                    if (lineNum > 0) {
                        body = body + " " + line;
                    } else {
                        title = title + " " + line;
                    }
                    lineNum++;
                }

                br.close();

                pstmt.setLong(1, testPostIds[index]);
                // pstmt.setTimestamp(2, new
                // Timestamp(System.currentTimeMillis()));
                pstmt.setTimestamp(2, new Timestamp(
                        new SimpleDateFormat(JAVA_TIMESTAMP_FORMAT).parse(testCreationDates[index]).getTime()));
                pstmt.setString(3, title);
                pstmt.setString(4, body);
                pstmt.addBatch();
                LOGGER.info("added batch: " + testPostIds[index]);

                index++;
            }
            pstmt.executeBatch();
            conn.commit();
            return true;
        } catch (IOException | SQLException e) {
            LOGGER.error("error", e);
            return false;
        } catch (ParseException e) {
            LOGGER.error("error", e);
            return false;
        } 
    }
    
    private static boolean populatePostTypesTable(Connection conn) {
        final String sql = 
                "INSERT INTO posttypes (id, name) VALUES (1, 'Question')";

        try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM posttypes")) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("Failed to update the posttypes table.", e);
            return false;
        }
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            LOGGER.error("Failed to update the posttypes table.", e);
            return false;
        } 
    }
    
    
    private static boolean populateTagsTable(Connection conn, String testTagsFilename) {
        final String sql_inst_tags_row = "INSERT INTO tags (id, tagname) VALUES (?,?)";
        
        try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM tags")) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("error", e);
            return false;            
        }

        try (PreparedStatement pstmt = conn.prepareStatement(sql_inst_tags_row);
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(testTagsFilename), StandardCharsets.UTF_8))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                pstmt.setLong(1, Long.parseLong(fields[2]));
                pstmt.setString(2,  fields[1]);
                LOGGER.info("Inserting tag: " + Long.parseLong(fields[2]) + ", " + fields[1]);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            conn.commit();
            return true;
        } catch (IOException | SQLException e) {
            LOGGER.error("error", e);
            return false;
        } 
    }    
    
    private static boolean populatePostTagsTable(Connection conn, String[] testPostTagsFilenames) {
        final String sql_inst_tags_row = "INSERT INTO posttags (postid, tagid) VALUES (?,?)";
        
        try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM posttags")) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("error", e);
            return false;            
        }

        try (PreparedStatement pstmt = conn.prepareStatement(sql_inst_tags_row)) {
            for (String fn: testPostTagsFilenames) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fn), StandardCharsets.UTF_8))) {
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        String[] fields = line.split(",");
                        LOGGER.info("read line: " + line);
                        LOGGER.info("Inserting post-tag: " + Long.parseLong(fields[0]) + ", " + Long.parseLong(fields[2]));
                        pstmt.setLong(1, Long.parseLong(fields[0]));
                        pstmt.setLong(2, Long.parseLong(fields[2]));
                        pstmt.addBatch();
                    }
                }
            }
            pstmt.executeBatch();
            conn.commit();
            return true;
        } catch (IOException | SQLException e) {
            LOGGER.error("error", e);
            return false;
        } 
    }

    
    private static boolean purgeWorkingTables(Connection conn) {
        final String[] workingTables = {
                DbUtils.getQuestionWordTable(),
                DbUtils.getVocabularyTable(),
                DbUtils.getQuestionIdTable(),
                DbUtils.getQuestionTagTable(),
                DbUtils.getTagTable()
        };
        
        for (String tbl:workingTables) {
            String sql = "DROP TABLE IF EXISTS " + tbl;
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.executeUpdate();
                LOGGER.info("executed " + sql + ".");
            } catch (SQLException e) {
                LOGGER.error("Failed to purge " + tbl, e);
                return false;
            }
        }
        return true;
    }
     
    public static boolean uniqWords(String dbPropertiesFilename) {
        try (Connection conn = DbUtils.connect(dbPropertiesFilename)) {
            long numWords;
            long numDistinctWords;
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT count(DISTINCT word) FROM " + DbUtils.getVocabularyTable());
                    ResultSet rs = pstmt.executeQuery()) {
                rs.next();
                numDistinctWords = rs.getLong(1);
            }
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT count(word) FROM " + DbUtils.getVocabularyTable());
                    ResultSet rs = pstmt.executeQuery()) {
                rs.next();
                numWords = rs.getLong(1);
            }
            return numWords == numDistinctWords;
        } catch (SQLException e) {
            LOGGER.error("failed to query testdb.", e);
            return false;
        }
    }
    
    public static boolean correctVocab(String dbPropertiesFilename, String[][] wantedVocabTable) {
        try (Connection conn = DbUtils.connect(dbPropertiesFilename)) {
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT id, word, word_index, ndocuments, noccurrences FROM  " + DbUtils.getVocabularyTable() + " ORDER BY id");
                    ResultSet rs = pstmt.executeQuery()) {
                int i = 0;
                while (rs.next()) {
                    long dbId = rs.getLong(1);
                    String dbWord = rs.getString(2);
                    String dbWordIdx = rs.getString(3);
                    long dbNumDocuments = Long.parseLong(rs.getString(4));
                    long dbNumOccurrences = Long.parseLong(rs.getString(5));
                    
                    if (dbId != Long.parseLong(wantedVocabTable[i][0])) {
                        LOGGER.error("word " + dbId + "," + dbWord + "," + dbWordIdx + "," + dbNumDocuments + "," + dbNumOccurrences + " does not match " + i);                        
                        return false;
                    }
                    
                    if (!dbWord.equals(wantedVocabTable[i][1])) {
                        LOGGER.error("word " + dbId + "," + dbWord + "," + dbWordIdx + "," + dbNumDocuments + "," + dbNumOccurrences + " does not match " + i);                        
                        return false;
                    }
                    
                    if (!dbWordIdx.equals(wantedVocabTable[i][2])) {
                        LOGGER.error("word " + dbId + "," + dbWord + "," + dbWordIdx + "," + dbNumDocuments + "," + dbNumOccurrences + " does not match " + i);                        
                        return false;
                    }
                    
                    if (dbNumDocuments != Long.parseLong(wantedVocabTable[i][3])) {
                        LOGGER.error("word " + dbId + "," + dbWord + "," + dbWordIdx + "," + dbNumDocuments + "," + dbNumOccurrences + " does not match " + i);                        
                        return false;
                    }
                    
                    if (dbNumOccurrences != Long.parseLong(wantedVocabTable[i][4])) {
                        LOGGER.error("word " + dbId + "," + dbWord + "," + dbWordIdx + "," + dbNumDocuments + "," + dbNumOccurrences + " does not match " + i);                        
                        return false;
                    }     
                    i ++;
                }
                
                if (i != wantedVocabTable.length) {
                    return false;
                }
                
                LOGGER.info("Words are correct for " + (i+1) + " words");
                return true;
            }
        } catch (SQLException e) {
            LOGGER.error("failed to do vocabulary table check.", e);
            return false;
        }
    }
    
    public static boolean correctQW(String dbPropertiesFilename, long[][] wantedQWTable) {
        try (Connection conn = DbUtils.connect(dbPropertiesFilename)) {
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT postid, wordid, wordcount FROM " + DbUtils.getQuestionWordTable() + " ORDER BY postid, wordid");
                    ResultSet rs = pstmt.executeQuery()) {
                int i = 0;
                while (rs.next()) {
                    long postId = rs.getLong(1);
                    long wordId = rs.getLong(2);
                    long wordCount = rs.getLong(3);
                    
                    if (postId != wantedQWTable[i][0]) {
                        return false;
                    }
                    
                    if (wordId != wantedQWTable[i][1]) {
                        return false;
                    }
                    
                    if (wordCount != wantedQWTable[i][2]) {
                        return false;
                    }
                    
                    i ++;
                }
                
                if (i != wantedQWTable.length) {
                    return false;
                }
                LOGGER.info("Question-word tuples are correct for " + (i+1) + " tuples");
                return true;                
            }
        } catch (SQLException e) {
            LOGGER.error("failed to do QW table check.", e);
            return false;
        }
    }
    


    public static boolean correctQW(String dbPropertiesFilename, String[][] wantedVocabTable, long[][] wantedQWTable) {
        try (Connection conn = DbUtils.connect(dbPropertiesFilename)) {
            try (PreparedStatement pstmt 
                    = conn.prepareStatement("SELECT qw.postid, qw.wordid, qw.wordcount, v.word FROM " 
                            + DbUtils.getVocabularyTable() + " as v," + DbUtils.getQuestionWordTable() + " as qw "
                            + " WHERE v.id=qw.wordid " 
                            + " ORDER BY postid, wordid");
                    ResultSet rs = pstmt.executeQuery()) {
                int i = 0;
                while (rs.next()) {
                    long postId = rs.getLong(1);
                    long wordCount = rs.getLong(3);
                    String word = rs.getString(4);
                    
                    String[] wordTuple = findWord(wantedVocabTable, word);
                    long[] qwTuple = findQW(wantedQWTable, postId, Long.parseLong(wordTuple[0]));
                    
                    if (postId != qwTuple[0]) {
                        LOGGER.error("Mismatch found for word " + word + " in post " + postId);
                        return false;
                    }
                    
                    if (wordCount != qwTuple[2]) {
                        LOGGER.error("Mismatch found for word " + word + " in post " + postId);                        
                        return false;
                    }
                    
                    i ++;
                }
                
                if (i != wantedQWTable.length) {
                    return false;
                }
                LOGGER.info("Question-word tuples are correct for " + (i+1) + " tuples");
                return true;                
            }
        } catch (SQLException e) {
            LOGGER.error("failed to do QW table check.", e);
            return false;
        }
    }
    

    public static boolean correctVocabOnW(String dbPropertiesFilename, String[][] wantedVocabTable) {
        try (Connection conn = DbUtils.connect(dbPropertiesFilename)) {
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT id, word, word_index, ndocuments, noccurrences FROM  " + DbUtils.getVocabularyTable() + " ORDER BY id");
                    ResultSet rs = pstmt.executeQuery()) {
                int i = 0;
                while (rs.next()) {
                    long dbId = rs.getLong(1);
                    String dbWord = rs.getString(2);
                    String dbWordIdx = rs.getString(3);
                    long dbNumDocuments = Long.parseLong(rs.getString(4));
                    long dbNumOccurrences = Long.parseLong(rs.getString(5));
                    
                    String[] wordTuple = findWord(wantedVocabTable, dbWord);
                    
                    if (wordTuple == null) {
                        LOGGER.error("word " + dbWord + " is not found from the wanted.");                        
                        return false;                    
                    }
                    
                    if (!dbWordIdx.equals(wordTuple[2])) {
                        LOGGER.error("word " + dbId + "," + dbWord + "," + dbWordIdx + "," + dbNumDocuments + "," + dbNumOccurrences + " does not match " + i);                        
                        return false;
                    }
                    
                    if (dbNumDocuments != Long.parseLong(wordTuple[3])) {
                        LOGGER.error("word " + dbId + "," + dbWord + "," + dbWordIdx + "," + dbNumDocuments + "," + dbNumOccurrences + " does not match " + i);                        
                        return false;
                    }
                    
                    if (dbNumOccurrences != Long.parseLong(wordTuple[4])) {
                        LOGGER.error("word " + dbId + "," + dbWord + "," + dbWordIdx + "," + dbNumDocuments + "," + dbNumOccurrences + " does not match " + i);                        
                        return false;
                    }     
                    i ++;
                }
                
                if (i != wantedVocabTable.length) {
                    return false;
                }
                
                LOGGER.info("Words are correct for " + (i+1) + " words");
                return true;
            }
        } catch (SQLException e) {
            LOGGER.error("failed to do vocabulary table check.", e);
            return false;
        }
    }

    public static boolean correctQuestionId(final String dbPropertiesFilename, final long[] wantedQIDTable) {
        try (Connection conn = DbUtils.connect(dbPropertiesFilename)) {
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT postid FROM  " + DbUtils.getQuestionIdTable() + " ORDER BY postid");
                    ResultSet rs = pstmt.executeQuery()) {
                int i = 0;
                while (rs.next()) {
                    long postId = rs.getLong(1);
                    
                    if (postId != wantedQIDTable[i]) {
                        LOGGER.error("postid " + postId + " does not match desired postid " + wantedQIDTable[i] + " at row " + i);                        
                        return false;                    
                    }
                    i ++;
                }
                LOGGER.info("PostIds are correct for " + i + " rows.");
                return true;
            }
        } catch (SQLException e) {
            LOGGER.error("failed to do questionid table check.", e);
            return false;
        }        
    }
    
    public static boolean correctQuestionTag(final String dbPropertiesFilename, final long[][] wantedQidTidTable) {
        try (Connection conn = DbUtils.connect(dbPropertiesFilename)) {
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT postid,tagid FROM  " + DbUtils.getQuestionTagTable() + " ORDER BY postid,tagid");
                    ResultSet rs = pstmt.executeQuery()) {
                int i = 0;
                while (rs.next()) {
                    long postId = rs.getLong(1);
                    long tagId = rs.getLong(2);
                    
                    if (postId != wantedQidTidTable[i][0]) {
                        LOGGER.error("postid " + postId + " does not match desired postid " + wantedQidTidTable[i][0] + " at row " + i);                        
                        return false;                    
                    }
                    
                    if (tagId != wantedQidTidTable[i][1]) {
                        LOGGER.error("tagid " + tagId + " does not match desired tagid " + wantedQidTidTable[i][1] + " at row " + i);                        
                        return false;                    
                    }                    
                    i ++;
                }
                LOGGER.info("PostIds and TagIds are correct for " + i + " rows.");
                return true;
            }
        } catch (SQLException e) {
            LOGGER.error("failed to do questionid-tagid table check.", e);
            return false;
        }        
    }
    
    public static boolean correctTag(final String dbPropertiesFilename, final String[][] wantedTagTable) {
        try (Connection conn = DbUtils.connect(dbPropertiesFilename)) {
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT tagid,tagname FROM  " + DbUtils.getTagTable() + " ORDER BY tagid");
                    ResultSet rs = pstmt.executeQuery()) {
                int i = 0;
                while (rs.next()) {
                    long tagId = rs.getLong(1);
                    String tagName = rs.getString(2);
                    
                    if (tagId != Long.parseLong(wantedTagTable[i][1])) {
                        LOGGER.error("tagid " + tagId + " does not match desired tagid " + wantedTagTable[i][1] + " at row " + i);                        
                        return false;                    
                    }
                    
                    if (!tagName.equals(wantedTagTable[i][0])) {
                        LOGGER.error("tagName " + tagName + " does not match desired tagName " + wantedTagTable[i][0] + " at row " + i);                        
                        return false;                    
                    }                    
                    i ++;
                }
                LOGGER.info("TagIds and TagNames are correct for " + i + " rows.");
                return true;
            }
        } catch (SQLException e) {
            LOGGER.error("failed to do tag table check.", e);
            return false;
        }        
    }    

    
    private static String[] findWord(String[][] wantedVocabTable, String word) {
        for (String[] wordTuple:wantedVocabTable) {
            if (wordTuple[1].equals(word)) {
                return wordTuple;
            }
        }
        return null;
    } 
    
    private static long[] findQW(long[][] wantedQWTable, long postId, long wordId) {
        for (long[] qwTuple:wantedQWTable) {
            if (qwTuple[0] == postId && qwTuple[1] == wordId) {
                return qwTuple;
            }
        }
        return null;
    }
        
}
