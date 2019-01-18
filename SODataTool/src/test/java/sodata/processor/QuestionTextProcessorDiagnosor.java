package sodata.processor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.QuestionWord;
import sodata.database.DbUtils;
import sodata.parser.SimplePostBodyWordExtractionParser;

public class QuestionTextProcessorDiagnosor {
    private final static Logger LOGGER = LoggerFactory.getLogger(QuestionTextProcessorDiagnosor.class);
    private final static String DB_PROPERTIES_FILENAME = "sodumpdb181.properties";
    
    
    @Test
    public void testpopulateTempQuestionWordTableUsingCopyAlgorithm() throws SQLException {
        try (Connection conn = DbUtils.connect(DB_PROPERTIES_FILENAME)) {
            conn.setAutoCommit(false);
            populateTempQuestionWordTableUsingCopyAlgorithm(conn, 95007);
        }
    }
    
    public String getSqlSelectTitleBodies(long postId) {
        return 
                " SELECT "
            +    "p.id as id, p.title as title, p.body as body"
            + " FROM "
            +    "posts as p, posttypes as pt"
            + " WHERE "
            +    "p.id = " + postId + " AND " + "pt.name='Question' AND p.posttypeid=pt.id";
    }

    private Map<String, QuestionWord> buildQuestionWordMap(long postId, String[] docWords, boolean ignoreCase) {
        Map<String, QuestionWord> questionWordMap = new HashMap<String, QuestionWord>();
        QuestionWord questionWord = null;
        String word;
        for (String aWord:docWords) {
            if (ignoreCase) {
                word = aWord.trim().toLowerCase();
            } else {
                word = aWord.trim();
            }
            if (word.length() > 0) {
                if (questionWordMap.containsKey(word)) {
                    questionWord = questionWordMap.get(word);
                    questionWord.nOccurrences ++;
                } else {
                    questionWord = new QuestionWord(postId, word, 1L);
                    questionWordMap.put(word,  questionWord);
                }
            }
        }
        return questionWordMap;
    }
    
    private boolean populateTempQuestionWordTableUsingCopyAlgorithm(Connection conn, long postId) {  
        PreparedStatement pstmt_slct_titlebodies = null;
        ResultSet rs = null;
        try {
            if (conn.getAutoCommit()) {
                LOGGER.error("The database connection's autocommit mode is true. It must be false to use cursor to query a large table");
                throw new RuntimeException("The database connection's autocommit mode must be false.");
            }
            
            File qwFile = new File("tmpfile_tmpqw.csv");
            PrintWriter qwPrintWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(qwFile), StandardCharsets.UTF_8));

            String sqlSelectTitleBodies = getSqlSelectTitleBodies(postId);
            pstmt_slct_titlebodies = conn.prepareStatement(sqlSelectTitleBodies,  
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            
            pstmt_slct_titlebodies.setFetchSize(DbUtils.FETCH_BATCH_SIZE);
            LOGGER.info("Running query " + pstmt_slct_titlebodies.toString());
            rs = pstmt_slct_titlebodies.executeQuery();
            
            LOGGER.info("Start processing questions.");
            long batchCount = 0;
            long processedQuestions = 0;
            while (rs.next()) {
                String questionTitle = rs.getString("title");
                String questionBody = rs.getString("body");
                String questionText = questionTitle + " " + questionBody;
                long questionId = rs.getLong("id");
                String[] wordSequence = SimplePostBodyWordExtractionParser.getWordsFromPostText(questionText);
                boolean ignoreCase = true;
                Map<String, QuestionWord> questionWordMap = buildQuestionWordMap(questionId, wordSequence, ignoreCase);
                for (Map.Entry<String, QuestionWord> entry : questionWordMap.entrySet()) {
                    qwPrintWriter.println(entry.getValue().questionId 
                            + "," + entry.getValue().word
                            + "," + entry.getValue().nOccurrences);
                    batchCount ++;
                    
                    if (batchCount % DbUtils.TRANSACTION_BATCH_SIZE == 0) {
                        LOGGER.info("Processed " + batchCount + " question-word tuples: instance: " 
                        + entry.getValue().questionId 
                        + "," + entry.getValue().word
                        + "," + entry.getValue().nOccurrences);
                    }                   
                }

                processedQuestions ++;
                if (processedQuestions % DbUtils.FETCH_BATCH_SIZE == 0) {
                    LOGGER.info("Processed " + processedQuestions + " questions.");
                }
            }   
            
            if (batchCount % DbUtils.TRANSACTION_BATCH_SIZE != 0) {
                LOGGER.info("Completed processing " + batchCount + " words.");              
            }
            
            qwPrintWriter.close();
            
            return true;
        } catch (SQLException e) {
            LOGGER.error("Failed to compute vocabulary.", e);
            return false;
        } catch (IOException e) {
            LOGGER.error("Failed to copy database table.", e);
            return false;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (pstmt_slct_titlebodies != null) {
                    pstmt_slct_titlebodies.close();
                }       
            } catch (SQLException e) {
                LOGGER.info("Failed to compute vocabulary.", e);
            }
        }           
    }    
}
