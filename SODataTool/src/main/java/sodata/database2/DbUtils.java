package sodata.database2;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbUtils extends DbUtilsBase {
    private final static Logger LOGGER = LoggerFactory.getLogger(DbUtils.class);

    public final static int FETCH_BATCH_SIZE = 1000000;
    public final static int TRANSACTION_BATCH_SIZE = 1000000;
    public final static int QW_TO_QW_TRANSACTION_BATCH_SIZE = 4000000;
    public final static int WORDS_LOAD = 1000000;

    public final static String SODUMPDB_PROPERTIES_FN = "sodumpdb182.properties";

    // this part needs to be refactored to reflect good coding style
    // working tables
    private static String workingTablePrefix = "Wk_";
    private final static String DEFAULT_TMP_QUESTION_WORD_TABLE = "Wk_Tmp_QuestionWord";
    private final static String DEFAULT_TMP_TAG_TABLE = "Wk_Tmp_Tag";
    private final static String DEFAULT_VOCABULARY_TABLE = "Wk_QVocab";
    private final static String DEFAULT_QUESTION_WORD_TABLE = "Wk_QuestionWord";
    private final static String DEFAULT_QUESTION_ID_TABLE = "Wk_QuestionId";
    private final static String DEFAULT_QUESTION_TAG_TABLE = "Wk_QuestionTag";
    private final static String DEFAULT_TAG_TABLE = "Wk_Tags";
    private final static String DEFAULT_WORD_QUESTION_COUNT_TABLE = "Wk_WordQuestionCount";
    private final static String DEFAULT_TAG_QUESTION_COUNT_TABLE = "Wk_TagQuestionCount";

    private static String tmpQuestionWordTable = null;
    private static String tmpTagTable = null;
    private static String vocabularyTable = null;
    private static String questionWordTable = null;
    private static String questionIdTable = null;
    private static String questionTagTable = null;
    private static String tagTable = null;
    private static String wordQuestionCountTable = null;
    private static String tagQuestionCountTable = null;

    public static void setWorkingTablePrefix(String tblPrefix) {
        if (tblPrefix != null && tblPrefix.length() > 0) {
            workingTablePrefix = tblPrefix;
            tmpQuestionWordTable = workingTablePrefix + "Tmp_QuestionWord";
            tmpTagTable = workingTablePrefix + "Tmp_Tag";
            vocabularyTable = workingTablePrefix + "QVocab";
            questionWordTable = workingTablePrefix + "QuestionWord";
            questionIdTable = workingTablePrefix + "QuestionId";
            questionTagTable = workingTablePrefix + "QuestionTag";
            tagTable = workingTablePrefix + "Tags";
            wordQuestionCountTable = workingTablePrefix + "WordQuestionCount";
            tagQuestionCountTable = workingTablePrefix + "TagQuestionCount";
        }
    }

    public static String getTmpQuestionWordTable() {
        if (tmpQuestionWordTable == null || tmpQuestionWordTable.length() == 0)
            return DEFAULT_TMP_QUESTION_WORD_TABLE;
        else
            return tmpQuestionWordTable;
    }

    public static String getVocabularyTable() {
        if (vocabularyTable == null || vocabularyTable.length() == 0)
            return DEFAULT_VOCABULARY_TABLE;
        else
            return vocabularyTable;    
    }

    public static String getQuestionIdTable() {
        if (questionIdTable == null || questionIdTable.length() == 0)
            return DEFAULT_QUESTION_ID_TABLE;
        else
            return questionIdTable;
    }

    public static String getQuestionWordTable() {
        if (questionWordTable == null || questionWordTable.length() == 0)
            return DEFAULT_QUESTION_WORD_TABLE;
        else
            return questionWordTable; 
    }

    public static String getQuestionTagTable() {
        if (questionTagTable == null || questionTagTable.length() == 0) 
            return DEFAULT_QUESTION_TAG_TABLE;
        else
            return questionTagTable;
    }

    public static String getTagTable() {
        if (tagTable == null || tagTable.length() == 0) 
            return DEFAULT_TAG_TABLE;
        else
            return tagTable;
    }

    public static String getWordQuestionCountTable() {
        if (wordQuestionCountTable == null || wordQuestionCountTable.length() == 0) 
            return DEFAULT_WORD_QUESTION_COUNT_TABLE;
        else
            return wordQuestionCountTable;  
    }    

    public static String getTagQuestionCountTable() {
        if (tagQuestionCountTable == null || tagQuestionCountTable.length() == 0) 
            return DEFAULT_TAG_QUESTION_COUNT_TABLE;
        else
            return tagQuestionCountTable;  
    }

    // SQL Statements
    public static String getSqlMkTmpQWTable() {
        return 
            " CREATE TEMPORARY TABLE " + DbUtils.getTmpQuestionWordTable()
                +   " ( "
                +       " postid BIGINT, " 
                +       " word TEXT, "
                +       " count BIGINT "
                +   " ) "
                + " ON COMMIT DROP";    
    }

    public final static long VOCAB_TBL_WORD_INDEX_LENGTH = 64;

    public static String getSqlMkVocabTable() {
        return 
            " CREATE TABLE " + DbUtils.getVocabularyTable()
            +   " ( "
            +       " id BIGINT,"
            +       " word TEXT,"
            +       " word_index VARCHAR(" + Long.toString(VOCAB_TBL_WORD_INDEX_LENGTH) + "), " // less than 1% longer than 64 characters
            +       " ndocuments BIGINT,"
            +       " noccurrences BIGINT"
            +   " ) ";
    }

    public static String[] getSqlMkVocabTblTrigger()
    {
        String[] sqlMkVocabTblTrigger = {
             " CREATE OR REPLACE FUNCTION compute_word_index_from_word_text() "
           + " RETURNS trigger "
           + " LANGUAGE plpgsql "
           + " SECURITY DEFINER "
           + " AS $BODY$ "
           + "    BEGIN "
           + "   NEW.word_index = NEW.word::VARCHAR(" + Long.toString(VOCAB_TBL_WORD_INDEX_LENGTH) + "); "
           + "   RETURN NEW; "
           + " END "
           + " $BODY$ "
           ,
             " CREATE TRIGGER compute_word_index_from_word_text_trigger "
           + " BEFORE INSERT OR UPDATE "
           + " ON " + DbUtils.getVocabularyTable()
           + " FOR EACH ROW " 
           + " EXECUTE PROCEDURE compute_word_index_from_word_text() "           
         };
        return sqlMkVocabTblTrigger;
    }

    public static String[] getSqlMkVocabTblIndices() {
        
            String[] sqlMkVocabTblIndices
            = {
              " ALTER TABLE "
            +     DbUtils.getVocabularyTable()
            + " ADD "
            + "   PRIMARY KEY (id)",
              " CREATE INDEX " 
            +     DbUtils.getVocabularyTable() + "_noccurrences_idx"
            + " ON " 
            +     DbUtils.getVocabularyTable() 
            + " USING btree (noccurrences) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
              " CREATE INDEX " 
            +     DbUtils.getVocabularyTable() + "_ndocuments_idx"
            + " ON " 
            +     DbUtils.getVocabularyTable() 
            + " USING btree (ndocuments) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
              " CREATE INDEX "
            +     DbUtils.getVocabularyTable() + "_word_index_idx"
            + " ON " 
            +     DbUtils.getVocabularyTable() 
            + " USING btree (word_index) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
              };    
         return sqlMkVocabTblIndices;
    }

    public static String getSqlMkQwTable() {
        return 
            " CREATE TABLE " + DbUtils.getQuestionWordTable()
                +   " ( "
                +       " postid BIGINT,"
                +       " wordid BIGINT,"
                +       " wordcount BIGINT"
                +   " ) ";
    }

    public static String[] getSqlMkQwTblIndices() {
        String[] sqlMkQwTblIndices
            = {
              "ALTER TABLE "
            +      DbUtils.getQuestionWordTable() 
            + " ADD "
            + "    PRIMARY KEY (postid, wordid)",
              " CREATE INDEX " 
            +     DbUtils.getQuestionWordTable() + "_posdid_idx"
            + " ON " 
            +     DbUtils.getQuestionWordTable() 
            + " USING btree (postid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
              " CREATE INDEX " 
            +     DbUtils.getQuestionWordTable() + "_wordid_idx"
            + " ON " 
            +     DbUtils.getQuestionWordTable() 
            + " USING btree (wordid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
              " CREATE INDEX "
            +     DbUtils.getQuestionWordTable() + "_wordcount_idx"
            + " ON " 
            +     DbUtils.getQuestionWordTable() 
            + " USING btree (wordcount) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
              };
            return sqlMkQwTblIndices;
    }

    public static String getSqlSelectTitleBodies() {
        return 
                " SELECT "
            +    "p.id as id, p.title as title, p.body as body"
            + " FROM "
            +    "posts as p, posttypes as pt"
            + " WHERE "
            +    "pt.name='Question' AND p.posttypeid=pt.id";
            // +    " AND p.creationdate >= to_date('2017-03-13', 'yyyy-mm-dd')";
    }

    public static String getSqlInsertTmpQuestionWordRow() {
        return
            " INSERT INTO " + DbUtils.getTmpQuestionWordTable() 
            +    " (postid, word, count) "
            + " VALUES "
            +    " (?, ?, ?)";    
    }

    public static String getSqlSelectTmpQuestionWordRow() {
        return 
                " SELECT postid, word, count FROM " + DbUtils.getTmpQuestionWordTable()
                + " ORDER BY word, postid";
    }

    public static String getSqlInsertWordToVocabularyTable() {
        return
                " INSERT INTO " + DbUtils.getVocabularyTable()
            +   " (id, word, word_index, ndocuments, noccurrences) "
            + " VALUES "
            +   " (?, ?, ?, ?, ?) ";
    }
    
    public static String getSqlInsertQuestionWordToQuestionWordTable() {
        return
                " INSERT INTO " + DbUtils.getQuestionWordTable()
            +   " (postid, wordid, wordcount) "
            + " VALUES "
            +   " (?, ?, ?) ";
    }

    public static String getSqlMkQuestionIdTableFromQuestionWordTable() {
        return "SELECT DISTINCT postid INTO " 
                + DbUtils.getQuestionIdTable()
                + " FROM "
                +     DbUtils.getQuestionWordTable()
                + " ORDER BY postid";        
    }

    public static String[] getSqlMkQuestionIdTblIndices() {
        
        String[] sqlMkVocabTblIndices
        = {
          " CREATE INDEX " 
        +     DbUtils.getQuestionIdTable() + "_postid_idx"
        + " ON " 
        +     DbUtils.getQuestionIdTable() 
        + " USING btree (postid) WITH (FILLFACTOR = 100)" // the table is seldom change, use fillfactor 100
          };    
        return sqlMkVocabTblIndices;
    }

    public static String getMkPostTagTableFromPosttagsQuestionidTables() {
        return 
              " SELECT "
            +     " pt.postid, pt.tagid "
            + " INTO "
            +     DbUtils.getQuestionTagTable()
            + " FROM "
            +     " posttags as pt," + DbUtils.getQuestionIdTable() + " as q " 
            + " WHERE " 
            +     " pt.postid = q.postid "
            + " ORDER BY "
            +     " pt.postid,pt.tagid";
    }   

    public static String getMkTagsTableFromQuestiontagTagsTables() {
        /* which of these two is faster? */
        /*
        return
              " SELECT "
            +     " qt.tagid, t.tagname "
            + " INTO "
            +     DbUtils.getTagTable()
            + " FROM "
            +     DbUtils.getQuestionTagTable() + " as qt, Tags as t "
            + " WHERE "
            +     " qt.tagid=t.tagid "
            + " ORDER BY qt.tagid ";
        */
        
        return
                " SELECT "
              +     " qt.tagid, t.tagname "
              + " INTO "
              +     DbUtils.getTagTable()
              + " FROM "
              +     " ( SELECT DISTINCT tagid FROM " + DbUtils.getQuestionTagTable() + " ) as qt, Tags as t "
              + " WHERE "
              +     " qt.tagid=t.id "
              + " ORDER BY qt.tagid ";       
    }

    public static String getSqlQuestionCount() {
        String sql = "SELECT COUNT(*) FROM " + DbUtils.getQuestionIdTable();
        return sql;
    }        

    public static Connection connect(String dbPropertiesFilename)
            throws IOException, ClassNotFoundException, SQLException {
        Connection conn = null;
        Properties properties = new Properties();

        try (InputStream in = new FileInputStream(dbPropertiesFilename)) {
            properties.load(in);
            Class.forName(properties.getProperty("driver"));
            conn = DriverManager.getConnection(properties.getProperty("url"), properties);
        }
        return conn;
    }

    public static boolean createTable(Connection conn, String sql_mk_tbl, String[] sql_mk_triggers, String[] sql_mk_indices) {
        PreparedStatement pstmt = null;
        try {
            if (sql_mk_tbl != null) {
                pstmt = conn.prepareStatement(sql_mk_tbl);
                pstmt.executeUpdate();
                LOGGER.debug("Executing prepared SQL statement: " + sql_mk_tbl);
                pstmt.close();
            }

            if (sql_mk_triggers != null) {
                for (String sql_mk_function_or_trigger : sql_mk_triggers) {
                    pstmt = conn.prepareStatement(sql_mk_function_or_trigger);
                    LOGGER.debug("Executing prepared SQL statement: " + sql_mk_function_or_trigger);
                    pstmt.executeUpdate();
                    pstmt.close();
                }        
            }

            if (sql_mk_indices != null) {
                for (String sql_mk_tbl_index : sql_mk_indices) {
                    pstmt = conn.prepareStatement(sql_mk_tbl_index);
                    LOGGER.debug("Executing prepared SQL statement: " + sql_mk_tbl_index);
                    pstmt.executeUpdate();
                    pstmt.close();
                }        
            }
            return true;
        } catch (SQLException e) {
            LOGGER.error("Failed to create table, indices, or triggers:" 
                    + " sql =  " + sql_mk_tbl
                    + " sql_mk_triggers = " + (sql_mk_triggers == null?"null":String.join(",", sql_mk_triggers))
                    + " sql_mk_indices = " + (sql_mk_indices == null?"null":String.join(",", sql_mk_indices))
                    , e);
            return false;
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    LOGGER.error("Failed to release prepared statement.", e);
                }
            }
        }
    }
    
    public static boolean createTableIndices(Connection conn, String[] sql_mk_indices) {
        return createTable(conn, null, null, sql_mk_indices);
    }

    public static boolean tableExists(Connection conn, String tableName) throws SQLException {
        boolean exist = false;
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, tableName.toLowerCase(), new String[] { "TABLE" })) {
            exist = rs.next();
        }
        return exist;
    }

    public static boolean dropTable(Connection conn, String tableName) {
        String sql = "DROP TABLE " + tableName;
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            LOGGER.error("Failed to drop table " + tableName, e);
            return false;
        }
    }

    public static String getTmpTagTable() {
        if (tmpTagTable == null || tmpTagTable.length() == 0)
            return DEFAULT_TMP_TAG_TABLE;
        else
            return tmpTagTable;
    }
}
