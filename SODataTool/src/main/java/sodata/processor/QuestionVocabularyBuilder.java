/**
 * This is slow. It should be faster to do it within the database using SQL procedures
 * because avoids transferring a large amount of data over the network. Perhaps, we
 * can avoid memory limit. 
 */
package sodata.processor;

import java.io.BufferedReader;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.Word;
import sodata.database.DbUtils;
import sodata.parser.SimplePostBodyWordExtractionParser;
import sodata.utils.monitor.Monitor;

public class QuestionVocabularyBuilder {
    private final int MAX_WARNING_WORD_LENGTH = 256;
    private final String VOCABULARY_CSV_FN = "../SOResults/simple_question_vocabulary.csv";    

    private final static Logger LOGGER = LoggerFactory.getLogger(QuestionVocabularyBuilder.class);
    
    private Connection conn;
    /**
     * This data structure will consume most memory. At present, 4 - 6GB heap memory is sufficient to
     * run queries and hold this data structure in memory for the stack overflow data dump. If the stack
     * overflow data dump grows significantly we need to consider to process words in batches, and use 
     * database table to hold words and statistics.
     */
    private Map<String, Word> vocabulary;
    private int maxWordLength;
    
    private final Monitor monitor;
    
    public QuestionVocabularyBuilder() {
        vocabulary = null;
        conn = null;
        maxWordLength = 0;
        monitor = new Monitor("memory_usage");
    }
    
    public void cleanup() {
        if (monitor != null) {
            monitor.cleanup();
        }
    }
    
    public boolean buildVocabulary() {
        try {
            if ((conn = DbUtils.connect(DbUtils.SODUMPDB_PROPERTIES_FN)) == null) {
                LOGGER.info("Failed to establish database connection.");
                return false;
            }
            LOGGER.info("Estabished database connection");
            
            if (vocabularyFileExists()) {
                if (!loadVocabularyFromFile()) {
                    LOGGER.info("Failed to load question vocabulary from " + VOCABULARY_CSV_FN + ".");
                    return false;
                }
                LOGGER.info("Loaded question vocabulary from " + VOCABULARY_CSV_FN + ".");
            } else {
                if (!computeVocabulary()) {
                    LOGGER.info("Failed to compute question vocabulary.");
                    return false;
                }
                LOGGER.info("Computed question vocabulary.");
            
                if (!writeVocabularyToFile()) {
                    LOGGER.info("Failed to write the vocabulary to file.");
                    return false;
                }
                LOGGER.info("Wrote the vocabulary to file.");
            }
    
            if (!populateVocabularyTable()) {
                LOGGER.info("Failed to populate the vocabulary table.");
                return false;
            }
            
            return true;
        } finally {
            if (vocabulary != null) {
                vocabulary.clear();
                vocabulary = null;
            }
            if (conn != null) {
                try {
                    conn.close();
                    conn = null;
                } catch (SQLException e) {
                    LOGGER.error("Failed to release database connection resources.", e);
                }
            }
        }
    }    
    
    private boolean computeVocabulary() {
        PreparedStatement pstmt = null;
        String sql 
            = " SELECT "
            +    "p.id as id, p.body as body"
            + " FROM "
            +    "posts as p, posttypes as pt"
            + " WHERE "
            +    "pt.name='Question' AND p.posttypeid=pt.id";

        try {
            vocabulary = new LinkedHashMap<String, Word>();
            
            conn.setAutoCommit(false);
            /* 
             * attempt to limit heap memory use by setting forward-only, read-only and non-holdable cursor.
             * note that the following statement would not work because the cursor is holdable.
             * pstmt = conn.prepareStatement(sql, 
             *                               ResultSet.TYPE_FORWARD_ONLY, 
             *                               ResultSet.CONCUR_READ_ONLY, 
             *                               ResultSet.HOLD_CURSORS_OVER_COMMIT); // cancels fetch_size
             */
            pstmt = conn.prepareStatement(sql,  ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            
            pstmt.setFetchSize(DbUtils.FETCH_BATCH_SIZE);
            LOGGER.info("Running query " + pstmt.toString());
            
            if (pstmt.getResultSetHoldability() == ResultSet.CLOSE_CURSORS_AT_COMMIT) {
                LOGGER.info("Holdability = ResultSet.CLOSE_CURSORS_AT_COMMIT" );
            } else {
                LOGGER.info("Holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT" );
            }
            ResultSet rs = pstmt.executeQuery();
            
            LOGGER.info("Start processing questions.");
            long processedQuestions = 0;
            while (rs.next()) {
                String body = rs.getString("body");
                long id = rs.getLong("id");
                String[] wordSequence = SimplePostBodyWordExtractionParser.getWordsFromPostText(body);
                Set<String> wordSet = new LinkedHashSet<String>();
                for (String w:wordSequence) {
                    if (vocabulary.containsKey(w)) {
                        Word word = vocabulary.get(w);
                        word.nOccurrences ++;
                        
                        if (!wordSet.contains(w)) {
                            word.nDocuments ++;
                            wordSet.add(w);
                        }
                    } else {
                        Word word = new Word(w, 1, 1);
                        vocabulary.put(w, word);
                        if (w.length() > maxWordLength) {
                            maxWordLength = w.length();
                        }
                        if (w.length() > MAX_WARNING_WORD_LENGTH) {
                            LOGGER.info("Post " + id + " has awefully long word: " + w.substring(0,  64) + "...");
                        }
                    }
                }
                processedQuestions ++;
                if (processedQuestions % DbUtils.FETCH_BATCH_SIZE == 0) {
                    LOGGER.info("Processed " + processedQuestions + " questions.");
                    monitor.write(processedQuestions);
                }
            }
            LOGGER.info("Processed all questions: " + processedQuestions + " questions.");
            return true;
        } catch (SQLException e) {
            LOGGER.error("Failed to compute vocabulary.", e);
            return false;
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                    pstmt =  null;
                }
            } catch (SQLException e) {
                LOGGER.info("Failed to compute vocabulary.", e);
            }
        }                
    }
    
    private boolean populateVocabularyTable() {
        String sql_mk_tbl
            = " CREATE TABLE "
            +     DbUtils.getVocabularyTable() 
            + " ( "
            +     " id BIGSERIAL PRIMARY KEY, "
            +     " word VARCHAR(?), "
            +     " ndocuments BIGINT, "
            +     " noccurrences BIGINT "
            + " ) ";
        final String[] SQL_MK_INDICES
            = {
              " CREATE INDEX " 
            +     DbUtils.getVocabularyTable() + "_noccurrences_idx"
            + " ON " 
            +     DbUtils.getVocabularyTable() 
            + " USING btree (noccurrences) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
              " CREATE INDEX " 
            +     DbUtils.getVocabularyTable() + "_ndocuments_idx"
            + " ON " 
            +     DbUtils.getVocabularyTable() 
            + " USING btree (ndocuments) WITH (FILLFACTOR = 100)" // the table is seldom change, use fillfactor 100            
              };
        final String SQL_INSERT_ROW
            = " INSERT INTO "
            +     DbUtils.getVocabularyTable()
            + " (word, ndocuments, noccurrences) "
            + " VALUES "
            + " (?, ?, ?) ";
        
        if (vocabulary == null || vocabulary.isEmpty()) {
            LOGGER.info("Nothing to do because the vocabulary is empty.");
            return true;
        }
        
        if (conn == null) {
            LOGGER.info("No database connection. Cannot continue.");
            return false;
        }
        
        PreparedStatement pstmt = null;
        try {
            conn.setAutoCommit(false);
            sql_mk_tbl = sql_mk_tbl.replace("?", Integer.toString(maxWordLength));
            LOGGER.info("The maximum word length is " + maxWordLength);
            pstmt = conn.prepareStatement(sql_mk_tbl);        
            LOGGER.info("Executing prepared SQL statement: " + sql_mk_tbl);
            //pstmt.setInt(1, maxWordLength); //cannot be used for SQL data type
            pstmt.execute();
            pstmt.close();
            
            for (String sql_mk_index : SQL_MK_INDICES) {
                pstmt = conn.prepareStatement(sql_mk_index);
                LOGGER.info("Executing prepared SQL statement: " + sql_mk_index);
                pstmt.execute();
                pstmt.close();
            }
            
            pstmt = conn.prepareStatement(SQL_INSERT_ROW);
            long batchCount = 0;
            for (Map.Entry<String, Word> entry : vocabulary.entrySet()) {
                Word word = entry.getValue();
                if (word.word.length() > 0) {
                    pstmt.setString(1, word.word);
                    pstmt.setLong(2, word.nDocuments);
                    pstmt.setLong(3, word.nOccurrences);
                    pstmt.addBatch();
                    batchCount ++;
                    if (batchCount % DbUtils.TRANSACTION_BATCH_SIZE == 0) {
                        pstmt.executeBatch();
                        LOGGER.info("Processed " + batchCount + " words.");
                    }
                }
            }
            if (batchCount % DbUtils.TRANSACTION_BATCH_SIZE != 0) {
                pstmt.executeBatch();
                LOGGER.info("Completed processing " + batchCount + " words.");                
            }
            conn.commit();
            return true;
        } catch (SQLException e) {
            LOGGER.error("Failed to populate " + DbUtils.getVocabularyTable(), e);
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    LOGGER.info("Failed to rollback.", ex);
                }
            }
            return false;
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    LOGGER.info("Failed to close PreparedStatement.", e);
                }
            }
        }
    }
    
    private boolean writeVocabularyToFile() {
        PrintWriter out = null;
        try {
            out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(VOCABULARY_CSV_FN), StandardCharsets.UTF_8));
            
            out.println("word,documents,occurrences");    
            for (Map.Entry<String, Word> entry : vocabulary.entrySet()) {
                Word word = entry.getValue();
                if (word.word.length() > 0) {
                    out.println(word.word + "," + word.nDocuments + "," + word.nOccurrences);
                }
            }
            LOGGER.info("Wrote simple question vocabulary to " + VOCABULARY_CSV_FN);    
            return true;
        } catch (IOException e) {
            LOGGER.error("Failed to write the vocabulary to " + VOCABULARY_CSV_FN + ".", e);
            return false;
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }
    
    private boolean vocabularyFileExists() {
        File f = new File(VOCABULARY_CSV_FN);
        return f.exists() && !f.isDirectory();
    }
    
    private boolean loadVocabularyFromFile() {
        BufferedReader reader = null;
        try {
            if (vocabulary != null) {
                vocabulary.clear();
            }
            vocabulary = new LinkedHashMap<String, Word>();
            
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(VOCABULARY_CSV_FN), StandardCharsets.UTF_8));
            String line;
            String[] fields;
            reader.readLine(); // skip header line
            long nWords = 0;
            Word word;
            while((line = reader.readLine()) != null) {
                fields = line.split(",");
                word = new Word(fields[0], Long.parseLong(fields[1]), Long.parseLong(fields[2]));
                vocabulary.put(Long.toString(nWords), word);
                if (fields[0].length() > maxWordLength) {
                    maxWordLength = fields[0].length();
                }
                nWords ++;
                if (nWords % DbUtils.WORDS_LOAD == 0) {
                    LOGGER.info("Loaded " + nWords + " words.");
                }
            }
            if (nWords % DbUtils.WORDS_LOAD != 0) {
                LOGGER.info("Loaded " + nWords + " words.");
            }
            return true;
        } catch (IOException e) {
            LOGGER.error("Failed to read " + VOCABULARY_CSV_FN + ".", e);
            return false;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOGGER.error("Failed to close file " + VOCABULARY_CSV_FN + ".", e);
                }
            }
        }
    }

    public static void main(String[] args) {
        LOGGER.info("starts.");

        QuestionVocabularyBuilder builder = new QuestionVocabularyBuilder();
        try {
            if (builder.buildVocabulary()) {
                LOGGER.info("completed.");
            } else {
                LOGGER.info("failed.");
            }
        } finally {
            builder.cleanup();
        }
    }

}
