package sodata.processor;

import static java.lang.Math.toIntExact;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.QuestionWord;
import sodata.StringUtils;
import sodata.database.DbUtils;
import sodata.parser.SimplePostBodyWordExtractionParser;
import sodata.utils.monitor.Monitor;

public class QuestionTextProcessor {
    private final static Logger LOGGER = LoggerFactory.getLogger(QuestionTextProcessor.class);

    private Connection conn = null;

    private final Monitor monitor; // monitor resource usage
    
    private String dbPropertiesFilename;
    
    private String startDate;
    private String endDate;
    private String sqlSelectTitleBodies;
    
    private boolean useCopy;
    private boolean ignoreCase;
    
    public QuestionTextProcessor() {
        conn = null;
        monitor = new Monitor("memory_usage");
        dbPropertiesFilename = DbUtils.SODUMPDB_PROPERTIES_FN;
        startDate = null;
        endDate = null;
        sqlSelectTitleBodies = DbUtils.getSqlSelectTitleBodies();
        useCopy = false;
    }
    
    public QuestionTextProcessor(boolean useCopy, boolean ignoreCase) {
        conn = null;
        monitor = new Monitor("memory_usage");
        dbPropertiesFilename = DbUtils.SODUMPDB_PROPERTIES_FN;
        startDate = null;
        endDate = null;
        sqlSelectTitleBodies = DbUtils.getSqlSelectTitleBodies();
        this.useCopy = useCopy;
        this.ignoreCase = ignoreCase;
    }    
    
    public QuestionTextProcessor(String dbPropertiesFilename) {
        this.conn = null;
        this.monitor = new Monitor("memory_usage");
        this.dbPropertiesFilename = dbPropertiesFilename;
        startDate = null;
        endDate = null;
        sqlSelectTitleBodies = DbUtils.getSqlSelectTitleBodies();     
        useCopy = false;
        ignoreCase = false;
    }
    
    public QuestionTextProcessor(String dbPropertiesFilename, boolean useCopy, boolean ignoreCase) {
        this.conn = null;
        this.monitor = new Monitor("memory_usage");
        this.dbPropertiesFilename = dbPropertiesFilename;
        startDate = null;
        endDate = null;
        sqlSelectTitleBodies = DbUtils.getSqlSelectTitleBodies();     
        this.useCopy = useCopy;
        this.ignoreCase = ignoreCase;        
    }    
    
    public boolean buildWordQuestionTables() {
        try {

            if ((conn = DbUtils.connect(dbPropertiesFilename)) == null) {
                LOGGER.info("Failed to establish database connection.");
                return false;
            }
            LOGGER.info("Estabished database connection");
        
            // Begin transaction
            conn.setAutoCommit(false);
            
            if (DbUtils.tableExists(conn, DbUtils.getQuestionWordTable())) {
                LOGGER.info("Table " + DbUtils.getQuestionWordTable() + " exists.");
                return false;
            }
            
            if (DbUtils.tableExists(conn, DbUtils.getVocabularyTable())) {
                LOGGER.info("Table " + DbUtils.getVocabularyTable() + " exists.");
                return false;
            }            
            
            if (!createTempQuestionWordTable()) {
                LOGGER.info("Cannot create the temporary working question-word table.");
                return false;
            }
            LOGGER.info("Created the temporary working question-word table.");
                
            if (useCopy) {
                if (!populateTempQuestionWordTableUsingCopy()) {
                    LOGGER.info("Failed to the populate working question-word table.");
                    return false;
                }
                LOGGER.info("Populated the working question-word table.");                
            } else {
                if (!populateTempQuestionWordTable()) {
                    LOGGER.info("Failed to the populate working question-word table.");
                    return false;
                }
                LOGGER.info("Populated the working question-word table.");
            }
            
            if (!createVocabularyTable()) {
                LOGGER.info("Cannot create the working vocabulary table.");
                return false;
            }
            LOGGER.info("Created working vocabulary table.");
            
            if (!createQuestionWordTable()) {
                LOGGER.info("Cannot create the working question-word table.");
                return false;
            }
            LOGGER.info("Created working question-word table.");
            
            if (useCopy) {
                if (!polulateQuestionWordAndVocabularyTablesUsingCopy()) {
                    LOGGER.info("Cannot populate the question-word and the vocabulary tables using the Copy-Table method.");
                    return false;
                }                
            } else {
                if (!polulateQuestionWordAndVocabularyTables()) {
                    LOGGER.info("Cannot populate the question-word and the vocabulary tables.");
                    return false;
                }
            }
            LOGGER.info("Populated populate the question-word and the vocabulary tables.");
            
            
            if (!createVocabularyTableIndicesAndTriggers()) {
                LOGGER.info("Cannot create indices and triggers for the vocabulary table.");
                return false;
            }
            LOGGER.info("Create indices and triggers for the vocabulary table.");
            
            
            if (!createQuestionWordTableIndices()) {
                LOGGER.info("Cannot index the working question-word table.");
                return false;
            }
            LOGGER.info("Created indices for working question-word table.");            

            // End transaction
            conn.commit();
            return true;
        } catch (SQLException e) {
            LOGGER.error("Failed to build word-question table.", e);
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.rollback();
                    conn.close();
                } catch (SQLException e) {
                    LOGGER.error("Failed to release database connection resources.", e);
                }
            }
        }
    }
    
    private boolean createTempQuestionWordTable() {
        return DbUtils.createTable(conn, DbUtils.getSqlMkTmpQWTable(), null, null);
    }

    private boolean createVocabularyTable() {
        return DbUtils.createTable(conn, DbUtils.getSqlMkVocabTable(), null, null);    
    }    
    
    private boolean createVocabularyTableIndicesAndTriggers() {
        return DbUtils.createTable(conn, null, DbUtils.getSqlMkVocabTblTrigger(), DbUtils.getSqlMkVocabTblIndices());   
    }    
    
    private boolean createQuestionWordTable() {
        return DbUtils.createTable(conn, DbUtils.getSqlMkQwTable(), null, null);
    }

    private boolean createQuestionWordTableIndices() {
        return DbUtils.createTable(conn, null, null, DbUtils.getSqlMkQwTblIndices());
    }

    private boolean populateTempQuestionWordTable() {    
        PreparedStatement pstmt_slct_titlebodies = null;
        PreparedStatement pstmt_inst_tmp_qw_row = null;
        ResultSet rs = null;
        try {
            if (conn.getAutoCommit()) {
                LOGGER.error("The database connection's autocommit mode is true. It must be false to use cursor to query a large table");
                throw new RuntimeException("The database connection's autocommit mode must be false.");
            }
            pstmt_inst_tmp_qw_row = conn.prepareStatement(DbUtils.getSqlInsertTmpQuestionWordRow());

            /* 
             * attempt to limit heap memory use by setting forward-only, read-only and non-holdable cursor.
             * note that the following statement would not work because the cursor is holdable.
             * pstmt = conn.prepareStatement(sql, 
             *                               ResultSet.TYPE_FORWARD_ONLY, 
             *                               ResultSet.CONCUR_READ_ONLY, 
             *                               ResultSet.HOLD_CURSORS_OVER_COMMIT); // cancels fetch_size
             */
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
                Map<String, QuestionWord> questionWordMap = buildQuestionWordMap(questionId, wordSequence, ignoreCase);
                for (Map.Entry<String, QuestionWord> entry : questionWordMap.entrySet()) {
                    pstmt_inst_tmp_qw_row.setLong(1, entry.getValue().questionId);    
                    pstmt_inst_tmp_qw_row.setString(2, entry.getValue().word);
                    pstmt_inst_tmp_qw_row.setLong(3, entry.getValue().nOccurrences);
                    // LOGGER.info(">>>>>>>>>>>>>>>>>>>question id, word, nOccurrences: " + entry.getValue().questionId + " " + entry.getValue().word + " " + entry.getValue().nOccurrences);
                    pstmt_inst_tmp_qw_row.addBatch();    
                    batchCount ++;
                    
                    if (batchCount % DbUtils.TRANSACTION_BATCH_SIZE == 0) {
                        pstmt_inst_tmp_qw_row.executeBatch();
                        LOGGER.info("Processed " + batchCount + " question-word tuples.");
                    }                    
                }
                // questionWordMap.forEach((word, questionWord)->{/* */});

                processedQuestions ++;
                if (processedQuestions % DbUtils.FETCH_BATCH_SIZE == 0) {
                    LOGGER.info("Processed " + processedQuestions + " questions.");
                    monitor.write(processedQuestions);
                }                
            }    
            
            if (batchCount % DbUtils.TRANSACTION_BATCH_SIZE != 0) {
                pstmt_inst_tmp_qw_row.executeBatch();
                LOGGER.info("Completed processing " + batchCount + " words.");                
            }            
            LOGGER.info("Processed all questions: " + processedQuestions + " questions.");
            return true;
        } catch (SQLException e) {
            LOGGER.error("Failed to compute vocabulary.", e);
            return false;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (pstmt_slct_titlebodies != null) {
                    pstmt_slct_titlebodies.close();
                }
                if (pstmt_inst_tmp_qw_row != null) {
                    pstmt_inst_tmp_qw_row.close();
                }                
            } catch (SQLException e) {
                LOGGER.info("Failed to compute vocabulary.", e);
            }
        }            
    }
    
    private boolean populateTempQuestionWordTableUsingCopy() {    
        PreparedStatement pstmt_slct_titlebodies = null;
        ResultSet rs = null;
        try {
            if (conn.getAutoCommit()) {
                LOGGER.error("The database connection's autocommit mode is true. It must be false to use cursor to query a large table");
                throw new RuntimeException("The database connection's autocommit mode must be false.");
            }
            
            File qwFile = new File("tmpfile_tmpqw.csv");
            PrintWriter qwPrintWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(qwFile), StandardCharsets.UTF_8));
            

            /* 
             * attempt to limit heap memory use by setting forward-only, read-only and non-holdable cursor.
             * note that the following statement would not work because the cursor is holdable.
             * pstmt = conn.prepareStatement(sql, 
             *                               ResultSet.TYPE_FORWARD_ONLY, 
             *                               ResultSet.CONCUR_READ_ONLY, 
             *                               ResultSet.HOLD_CURSORS_OVER_COMMIT); // cancels fetch_size
             */
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
                // String[] wordSequence = SimplePostBodyWordExtractionParser.getWordsFromPostText(questionText);
                String[] wordSequence = SimplePostBodyWordExtractionParser.getWordsFromPostText(questionText, 2);
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
                    monitor.write(processedQuestions);
                }                
            }    
            
            if (batchCount % DbUtils.TRANSACTION_BATCH_SIZE != 0) {
                LOGGER.info("Completed processing " + batchCount + " words.");                
            }
            
            qwPrintWriter.close();
            
            CopyManager copyManager = new CopyManager((BaseConnection)conn);
            LOGGER.info("Copying tmpfile_tmpqw.csv to table " + DbUtils.getTmpQuestionWordTable());
            InputStreamReader fileStreramReader = new InputStreamReader(new FileInputStream(qwFile), StandardCharsets.UTF_8);
            copyManager.copyIn("COPY " + DbUtils.getTmpQuestionWordTable() + " FROM STDIN WITH CSV", fileStreramReader);
            LOGGER.info("Processed all questions: " + processedQuestions + " questions.");
            
            qwFile.delete();
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


    private boolean polulateQuestionWordAndVocabularyTables() {     
        PreparedStatement pstmt_slct_tmp_qw_tbl = null;
        PreparedStatement pstmt_inst_qw_to_qw = null;
        PreparedStatement pstmt_inst_w_to_vocab = null;
        ResultSet rs = null;
        try {
            if (conn.getAutoCommit()) {
                LOGGER.error("The database connection's autocommit mode is true. It must be false to use cursor to query a large table");
                throw new RuntimeException("The database connection's autocommit mode must be false.");
            }
            
            pstmt_slct_tmp_qw_tbl = conn.prepareStatement(DbUtils.getSqlSelectTmpQuestionWordRow(),
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            pstmt_slct_tmp_qw_tbl.setFetchSize(DbUtils.FETCH_BATCH_SIZE);
            LOGGER.info("Running query " + pstmt_slct_tmp_qw_tbl.toString());
            rs = pstmt_slct_tmp_qw_tbl.executeQuery();
            
            pstmt_inst_qw_to_qw = conn.prepareStatement(DbUtils.getSqlInsertQuestionWordToQuestionWordTable());
            long batchCountInsertQw = 0;
            pstmt_inst_w_to_vocab = conn.prepareStatement(DbUtils.getSqlInsertWordToVocabularyTable());
            long batchCountInsertW = 0;
            String prevWord = null;
            String word = null;
            long wordId = 0;
            long nDocuments = 0;
            long nOccurrences = 0;
            long questionId;
            while (rs.next()) {
                questionId = rs.getLong("postid");
                word = rs.getString("word");
                long countInQuestion = rs.getLong("count");
                
                if (prevWord != null && !word.equals(prevWord)) { // word change
                    // for the old word
                    pstmt_inst_w_to_vocab.setLong(1, wordId);
                    pstmt_inst_w_to_vocab.setString(2, prevWord);
                    // pstmt_inst_w_to_vocab.setString(3, prevWord.substring(0, prevWord.length()>DbUtils.VOCAB_TBL_WORD_INDEX_LENGTH?(int)DbUtils.VOCAB_TBL_WORD_INDEX_LENGTH:prevWord.length()));
                    pstmt_inst_w_to_vocab.setString(3, StringUtils.head(prevWord, toIntExact(DbUtils.VOCAB_TBL_WORD_INDEX_LENGTH), StandardCharsets.UTF_8));
                    pstmt_inst_w_to_vocab.setLong(4, nDocuments);
                    pstmt_inst_w_to_vocab.setLong(5, nOccurrences);
                    pstmt_inst_w_to_vocab.addBatch();
                    batchCountInsertW ++;
                    if (batchCountInsertW % DbUtils.TRANSACTION_BATCH_SIZE == 0) {
                        pstmt_inst_w_to_vocab.executeBatch();
                        LOGGER.info("processed " + batchCountInsertW + "words.");
                        monitor.write(batchCountInsertW);
                    }       
                    LOGGER.info("wrote " + word + " wordId = " + wordId);
                    // for the new word
                    nDocuments = 1;
                    nOccurrences = countInQuestion;
                    wordId ++;
                    
                } else {
                    nDocuments ++;
                    nOccurrences += countInQuestion;
                }               
                
                pstmt_inst_qw_to_qw.setLong(1, questionId);
                pstmt_inst_qw_to_qw.setLong(2, wordId);
                pstmt_inst_qw_to_qw.setLong(3, countInQuestion);
                //LOGGER.info("questionid, wordid[word], len(word) = " + questionId + "," + wordId + "[" + word + "]" + "," + word.length());
                pstmt_inst_qw_to_qw.addBatch();
                batchCountInsertQw ++;
                if (batchCountInsertQw % DbUtils.QW_TO_QW_TRANSACTION_BATCH_SIZE == 0) {
                    pstmt_inst_qw_to_qw.executeBatch();
                    LOGGER.info("processed " + batchCountInsertQw + "question-words tuples.");
                }
                            
                prevWord = word;
            }
            pstmt_inst_qw_to_qw.executeBatch();
            LOGGER.info("Completed processing " + batchCountInsertQw + " question-words tuples.");              
            // for the last word
            if (word != null && word.equals(prevWord)) {
                pstmt_inst_w_to_vocab.setLong(1, wordId);
                pstmt_inst_w_to_vocab.setString(2, word);
                // pstmt_inst_w_to_vocab.setString(3, word.substring(0, prevWord.length()>DbUtils.VOCAB_TBL_WORD_INDEX_LENGTH?(int)DbUtils.VOCAB_TBL_WORD_INDEX_LENGTH:prevWord.length()));
                pstmt_inst_w_to_vocab.setString(3, StringUtils.head(word, toIntExact(DbUtils.VOCAB_TBL_WORD_INDEX_LENGTH), StandardCharsets.UTF_8));
                pstmt_inst_w_to_vocab.setLong(4, nDocuments);
                pstmt_inst_w_to_vocab.setLong(5, nOccurrences);
                pstmt_inst_w_to_vocab.addBatch();
                batchCountInsertW ++;
            }
            pstmt_inst_w_to_vocab.executeBatch();
            LOGGER.info("Completed processing " + batchCountInsertW + " words.");
            return true;
        } catch (SQLException e) {
            LOGGER.error("Failed to populate the vocabulary and question-word tables", e);
            return false;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (pstmt_slct_tmp_qw_tbl != null) {
                    pstmt_slct_tmp_qw_tbl.close();
                }
                if (pstmt_inst_qw_to_qw != null) {
                    pstmt_inst_qw_to_qw.close();
                }
                if (pstmt_inst_w_to_vocab != null) {
                    pstmt_inst_w_to_vocab.close();
                }
            } catch (SQLException e) {
                LOGGER.error("Failed to close prepared statements.", e);
            }
        }
    }  
    
    private boolean polulateQuestionWordAndVocabularyTablesUsingCopy() {     
        PreparedStatement pstmt_slct_tmp_qw_tbl = null;
        ResultSet rs = null;
        try {
            if (conn.getAutoCommit()) {
                LOGGER.error("The database connection's autocommit mode is true. It must be false to use cursor to query a large table");
                throw new RuntimeException("The database connection's autocommit mode must be false.");
            }

            File qwFile = new File("tmpfile_qw.csv");
            File vocabFile = new File("tmpfile_vocab.csv");
            PrintWriter qwPrintWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(qwFile), StandardCharsets.UTF_8));
            PrintWriter vocabPrintWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(vocabFile), StandardCharsets.UTF_8));


            pstmt_slct_tmp_qw_tbl = conn.prepareStatement(DbUtils.getSqlSelectTmpQuestionWordRow(),
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            pstmt_slct_tmp_qw_tbl.setFetchSize(DbUtils.FETCH_BATCH_SIZE);
            LOGGER.info("Running query " + pstmt_slct_tmp_qw_tbl.toString());
            rs = pstmt_slct_tmp_qw_tbl.executeQuery();
            
            long batchCountInsertQw = 0;
            String prevWord = null;
            String word = null;
            long wordId = 0;
            long nDocuments = 0;
            long nOccurrences = 0;
            long questionId;
            while (rs.next()) {
                questionId = rs.getLong("postid");
                word = rs.getString("word");
                long countInQuestion = rs.getLong("count");
                
                if (prevWord != null && !word.equals(prevWord)) { // word change
                    // for the old word
                    vocabPrintWriter.println(wordId 
                            + "," + prevWord  /* this is safe, since "," is used to extract words  and never appears within words, so we choose not to quote strings, this is default for postgresql copy function*/
                            // + "," + prevWord.substring(0, prevWord.length()>DbUtils.VOCAB_TBL_WORD_INDEX_LENGTH?(int)DbUtils.VOCAB_TBL_WORD_INDEX_LENGTH:prevWord.length())
                            + "," + StringUtils.head(prevWord, toIntExact(DbUtils.VOCAB_TBL_WORD_INDEX_LENGTH), StandardCharsets.UTF_8)
                            + "," + nDocuments 
                            + "," + nOccurrences);
                    // for the new word
                    nDocuments = 1;
                    nOccurrences = countInQuestion;
                    wordId ++;
                } else {
                    nDocuments ++;
                    nOccurrences += countInQuestion;
                }               
                
                qwPrintWriter.println(questionId + "," + wordId + "," + countInQuestion);
                batchCountInsertQw ++;
                if (batchCountInsertQw % 10000000l == 0) {
                    LOGGER.info("processed " + wordId + " words and " + batchCountInsertQw + " question-word pairs.");
                }
                prevWord = word;
            }
            // for the last word
            if (word != null && word.equals(prevWord)) {
                vocabPrintWriter.println(wordId 
                        + "," + prevWord  /* this is safe, since "," is used to extract words  and never appears within words, so we choose not to quote strings, this is default for postgresql copy function*/
                        // + "," + prevWord.substring(0, prevWord.length()>DbUtils.VOCAB_TBL_WORD_INDEX_LENGTH?(int)DbUtils.VOCAB_TBL_WORD_INDEX_LENGTH:prevWord.length())
                        + "," + StringUtils.head(prevWord, toIntExact(DbUtils.VOCAB_TBL_WORD_INDEX_LENGTH), StandardCharsets.UTF_8)
                        + "," + nDocuments 
                        + "," + nOccurrences);                
            }
            qwPrintWriter.close();
            vocabPrintWriter.close();
            LOGGER.info("processed " + wordId + " words and " + batchCountInsertQw + " question-word pairs.");

            
            CopyManager copyManager = new CopyManager((BaseConnection)conn);
            LOGGER.info("Copying tmpfile_qw.csv to table " + DbUtils.getQuestionWordTable());
            InputStreamReader fileStreamReader = new InputStreamReader(new FileInputStream(qwFile), StandardCharsets.UTF_8);
            copyManager.copyIn("COPY " + DbUtils.getQuestionWordTable() + " FROM STDIN WITH CSV", fileStreamReader);            

            LOGGER.info("Copying tmpfile_vocab.csv to table " + DbUtils.getVocabularyTable());            
            fileStreamReader = new InputStreamReader(new FileInputStream(vocabFile), StandardCharsets.UTF_8);
            copyManager.copyIn("COPY " + DbUtils.getVocabularyTable() + " FROM STDIN WITH CSV", fileStreamReader);                        
            
            LOGGER.info("Completed processing " + wordId + " words and " + batchCountInsertQw + " question-word pairs.");
            qwFile.delete();
            vocabFile.delete();
            return true;
        } catch (SQLException | IOException e) {
            LOGGER.error("Failed to populate the vocabulary and question-word tables", e);
            return false;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (pstmt_slct_tmp_qw_tbl != null) {
                    pstmt_slct_tmp_qw_tbl.close();
                }
            } catch (SQLException e) {
                LOGGER.error("Failed to close prepared statements.", e);
            }
        }
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
    
    private void updateSqlSelectTitleBodies() {
        if (startDate != null && endDate == null) {
            sqlSelectTitleBodies = DbUtils.getSqlSelectTitleBodies() + " AND p.creationdate >= to_timestamp('" + startDate + "', 'yyyy-mm-dd hh24:mi:ss')";
        } else if (startDate != null && endDate != null) {
            sqlSelectTitleBodies 
                = DbUtils.getSqlSelectTitleBodies() 
                    + " AND p.creationdate >= to_timestamp('" + startDate + "', 'yyyy-mm-dd hh24:mi:ss')"
                    + " AND p.creationdate < to_timestamp('" + endDate + "', 'yyyy-mm-dd hh24:mi:ss')";            
        } else if (startDate == null && endDate != null) {
            sqlSelectTitleBodies 
                = DbUtils.getSqlSelectTitleBodies() 
                    + " AND p.creationdate < to_timestamp('" + endDate + "', 'yyyy-mm-dd hh24:mi:ss')";
        }
    }    

    public void setEndDate(String endDate) {
        this.endDate = endDate;
        this.updateSqlSelectTitleBodies();
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
        this.updateSqlSelectTitleBodies();
    }    

    public boolean isTimeStampValid(String inputString)
    { 
        SimpleDateFormat format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try{
           format.parse(inputString);
           return true;
        } catch (ParseException e)  {
            return false;
        }
    }
    
    
    public static void main(String[] args) {
        boolean useCopy = false;
        boolean ignoreCase = false;
        ArrayList<String> clArgs = new ArrayList<String>();
        for (String arg:args) {
            if (arg.equals("--usecopy")) {
                useCopy = true;
            } else if (arg.equals("--ignorecase")) {
                ignoreCase = true;
            } else {
                clArgs.add(arg);
            }
        }
        LOGGER.info("Flag useCopy is " + useCopy);
        LOGGER.info("Flag ignoreCase is " + ignoreCase);
        QuestionTextProcessor builder = new QuestionTextProcessor(useCopy, ignoreCase);
        
        
        if (clArgs.size() == 2) {
            if (builder.isTimeStampValid(clArgs.get(1))) {
                builder.setStartDate(clArgs.get(1));
                LOGGER.info("Starting date is " + clArgs.get(1));
            } else {
                LOGGER.error("Timestamp must be in yyyy-mm-dd hh24:mi:ss format. " + args[0] + " is invalid." );
                return;
            }
        } else if (clArgs.size() == 3) {
            if (builder.isTimeStampValid(clArgs.get(1))) {
                builder.setStartDate(clArgs.get(1));
                LOGGER.info("Starting date is " + clArgs.get(1));
            } else {
                LOGGER.error("Timestamp must be in yyyy-mm-dd hh24:mi:ss format. " + args[0] + " is invalid." );
                return;
            }
            if (builder.isTimeStampValid(clArgs.get(2))) {
                builder.setEndDate(clArgs.get(2));
                LOGGER.info("Ending date is " + clArgs.get(2));
            } else {
                LOGGER.error("Timestamp must be in yyyy-mm-dd hh24:mi:ss format. " + args[0] + " is invalid." );
                return;
            } 
        } else {
            LOGGER.info("Usage: QuestionTextProcessor <db_table_prefix> <start_time_stamp> <end_time_stamp> [--usecopy]");
            return;
        }
        String tablePrefix = clArgs.get(0);
        LOGGER.info("Table Prefix is " + tablePrefix);
        
        DbUtils.setWorkingTablePrefix(tablePrefix);
        if (builder.buildWordQuestionTables()) {
            LOGGER.info("completed.");
        } else {
            LOGGER.info("failed.");
        }
    }    
}
