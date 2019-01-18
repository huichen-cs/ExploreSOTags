package sodata.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterDbUtils extends DbUtilsBase {
    private final static Logger LOGGER = LoggerFactory.getLogger(FilterDbUtils.class);
    
    public final static int FETCH_BATCH_SIZE = 10000;
    public final static int LOG_WORK_COUNTER = 100000;

    private static String filteredWorkingTableSuffix = "_F";
    private final static String DEFAULT_FILTERED_QUESTION_ID_TABLE = "Wk_QuestionId_F";
    private final static String DEFAULT_FILTERED_WORD_QUESTION_COUNT_TABLE = "Wk_WordQuestionCount_F";
    private final static String DEFAULT_FILTERED_TAG_QUESTION_COUNT_TABLE = "Wk_TagQuestionCount_F";
    private final static String DEFAULT_FILTERED_QUESTION_WORD_TABLE = "Wk_QuestionWord_F";
    private final static String DEFAULT_FILTERED_VOCABULARY_TABLE = "Wk_QVocab_F";
    private final static String DEFAULT_FILTERED_TAG_TABLE = "Wk_Tags_F";
    private final static String DEFAULT_FILTERED_QUESTION_TAG_TABLE = "Wk_QuestionTag_F";
    private static String filteredQuestionIdTable = null;
    private static String filteredWordQuestionCountTable = null; 
    private static String filteredQuestionWordTable = null;   
    private static String filteredVocabularyTable = null;
    private static String filteredTagQuestionCountTable = null;
    private static String filteredTagTable = null;
    private static String filteredQuestionTagTable = null;
       
    public static void setFilteredWorkingTablePrefix(String tblSuffix) {
        if (tblSuffix != null && tblSuffix.length() > 0) {
            filteredWorkingTableSuffix = tblSuffix;
            filteredQuestionIdTable = DbUtils.getQuestionIdTable() + filteredWorkingTableSuffix;
            filteredWordQuestionCountTable =  DbUtils.getWordQuestionCountTable() + filteredWorkingTableSuffix;
            filteredTagQuestionCountTable = DbUtils.getTagQuestionCountTable() + filteredWorkingTableSuffix;
            filteredQuestionWordTable = DbUtils.getQuestionWordTable() + filteredWorkingTableSuffix;
            filteredVocabularyTable = DbUtils.getVocabularyTable() + filteredWorkingTableSuffix;
            filteredTagTable = DbUtils.getTagTable() + filteredWorkingTableSuffix;
            filteredQuestionTagTable = DbUtils.getQuestionTagTable() + filteredWorkingTableSuffix;
        }       
    }

    
    public static String getFilteredWordQuestionCountTable() {
        if (filteredWordQuestionCountTable == null || filteredWordQuestionCountTable.length() == 0) 
            return DEFAULT_FILTERED_WORD_QUESTION_COUNT_TABLE;
        else
            return filteredWordQuestionCountTable;
    }
    
    public static String getFilteredTagQuestionCountTable() {
        if (filteredTagQuestionCountTable == null || filteredTagQuestionCountTable.length() == 0) 
            return DEFAULT_FILTERED_TAG_QUESTION_COUNT_TABLE;
        else
            return filteredTagQuestionCountTable;
    }
    
    public static String getFilteredQuestionWordTable() {
        if (filteredQuestionWordTable == null || filteredQuestionWordTable.length() == 0) 
            return DEFAULT_FILTERED_QUESTION_WORD_TABLE;
        else
            return filteredQuestionWordTable;
    }
    
    public static String getFilteredVocabularyTable() {
        if (filteredQuestionWordTable == null || filteredQuestionWordTable.length() == 0) 
            return DEFAULT_FILTERED_VOCABULARY_TABLE;
        else
            return filteredVocabularyTable;
    }
    
    public static String getFilteredTagTable() {
        if (filteredTagTable == null || filteredTagTable.length() == 0) 
            return DEFAULT_FILTERED_TAG_TABLE;
        else
            return filteredTagTable;
    }


    public static String getFilteredQuestionTagTable() {
        if (filteredQuestionTagTable == null || filteredQuestionTagTable.length() == 0) 
            return DEFAULT_FILTERED_QUESTION_TAG_TABLE;
        else
            return filteredQuestionTagTable;
    }      
    
    
    public static String getMkFilteredWordQuestionCountTableFromSelect(Class<? extends DbUtilsBase> DbUtilsClass) {
        String qwTable;
        if (DbUtilsClass.getName().equals(DbUtils.class.getName())) {
            qwTable = DbUtils.getQuestionWordTable();
        } else if (DbUtilsClass.getName().equals(DbTagSelectorUtils.class.getName())) {
            qwTable = DbTagSelectorUtils.getQuestionWordTable();
        } else {
            LOGGER.error("unsupported class: " + DbUtilsClass.getName());
            qwTable = "Non_Existing_Table";
        }
        return  
              " SELECT questioncount, wordid "
            + " INTO " + FilterDbUtils.getFilteredWordQuestionCountTable()
            + " FROM "
            +      " ( "
            +          " SELECT count(postid) AS questioncount,wordid AS wordid "
            +          " FROM " + qwTable + " GROUP BY wordid ORDER BY wordid "
            +      " ) AS qw "
            + " WHERE qw.questioncount >= ? and qw.questioncount <= ? ORDER BY wordid";
    }
    
    public static String[] getMkFilteredWordQuestionCountTblIndices() {
        String[] sqlMkFilteredWordQuestionCountTblIndices
        = {
              " CREATE INDEX " 
            +     FilterDbUtils.getFilteredWordQuestionCountTable() + "_wordid_idx"
            + " ON " 
            +     FilterDbUtils.getFilteredWordQuestionCountTable() 
            + " USING btree (wordid) WITH (FILLFACTOR = 100)" // the table is seldom change, use fillfactor 100
          };    
        return sqlMkFilteredWordQuestionCountTblIndices;        
    }
    
    public static String getMkFilteredTagQuestionCountTableFromSelect(Class<? extends DbUtilsBase> DbUtilsClass) {
        String qtTable;
        if (DbUtilsClass.getName().equals(DbUtils.class.getName())) {
            qtTable = DbUtils.getQuestionTagTable();
        } else if (DbUtilsClass.getName().equals(DbTagSelectorUtils.class.getName())) {
            qtTable = DbTagSelectorUtils.getQuestionTagTable();
        } else {
            LOGGER.error("unsupported class: " + DbUtilsClass.getName());
            qtTable = "Non_Existing_Table";
        }    
        return  
              " SELECT questioncount, tagid "
            + " INTO " + FilterDbUtils.getFilteredTagQuestionCountTable()
            + " FROM "
            +      " ( "
            +          " SELECT count(postid) AS questioncount,tagid AS tagid "
            +          " FROM " + qtTable + " GROUP BY tagid ORDER BY tagid "
            +      " ) AS qt "
            + " WHERE qt.questioncount >= ? and qt.questioncount <= ? ORDER BY tagid";
    }
    
    public static String[] getMkFilteredTagQuestionCountTblIndices() {
        String[] sqlMkFilteredTagQuestionCountTblIndices
        = {
              " CREATE INDEX " 
            +     FilterDbUtils.getFilteredTagQuestionCountTable() + "_tagid_idx"
            + " ON " 
            +     FilterDbUtils.getFilteredTagQuestionCountTable() 
            + " USING btree (tagid) WITH (FILLFACTOR = 100)" // the table is seldom change, use fillfactor 100
          };    
        return sqlMkFilteredTagQuestionCountTblIndices;        
    }    
    
    public static String getMkFilteredQuestionWordTableFromSelect(Class<? extends DbUtilsBase> DbUtilsClass) {
        String qwTable;
        if (DbUtilsClass.getName().equals(DbUtils.class.getName())) {
            qwTable = DbUtils.getQuestionWordTable();
        } else if (DbUtilsClass.getName().equals(DbTagSelectorUtils.class.getName())) {
            qwTable = DbTagSelectorUtils.getQuestionWordTable();
        } else {
            LOGGER.error("unsupported class: " + DbUtilsClass.getName());
            qwTable = "Non_Existing_Table";
        }        
        return 
              " SELECT "
            +    " qw.postid AS postid, qc.wordid AS wordid, qw.wordcount AS wordcount "
            + " INTO " + FilterDbUtils.getFilteredQuestionWordTable()
            + " FROM " 
            +        qwTable + " AS qw, "
            +        FilterDbUtils.getFilteredWordQuestionCountTable() + " AS qc "
            + " WHERE qw.wordid=qc.wordid ";             
    }
    
    
    public static String[] getMkFilteredQuestionWordTblIndices() {
        String[] sqlMkFilteredQuestionWordTblIndices
        = {
              " CREATE INDEX " 
            +     FilterDbUtils.getFilteredQuestionWordTable() + "_postid_idx"
            + " ON " 
            +     FilterDbUtils.getFilteredQuestionWordTable() 
            + " USING btree (postid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
              " CREATE INDEX " 
            +      FilterDbUtils.getFilteredQuestionWordTable()  + "_wordid_idx"
            + " ON " 
            +     FilterDbUtils.getFilteredQuestionWordTable() 
            + " USING btree (wordid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
              " CREATE INDEX " 
            +      FilterDbUtils.getFilteredQuestionWordTable()  + "_postidwordid_idx"
            + " ON " 
            +     FilterDbUtils.getFilteredQuestionWordTable() 
            + " USING btree (postid,wordid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
          };    
        return sqlMkFilteredQuestionWordTblIndices;          
    }    

    public static String getMkFilteredVocabularyTableFromSelect(Class<? extends DbUtilsBase> DbUtilsClass) {
        String vocabTable;
        if (DbUtilsClass.getName().equals(DbUtils.class.getName())) {
            vocabTable = DbUtils.getVocabularyTable();
        } else if (DbUtilsClass.getName().equals(DbTagSelectorUtils.class.getName())) {
            vocabTable = DbTagSelectorUtils.getVocabularyTable();
        } else {
            LOGGER.error("unsupported class: " + DbUtilsClass.getName());
            vocabTable = "Non_Existing_Table";
        }    
        return 
              " SELECT "
            +    " qc.wordid AS id,v.word,v.word_index,v.ndocuments,v.noccurrences "
            + " INTO " + FilterDbUtils.getFilteredVocabularyTable()
            + " FROM "
            +    vocabTable + " AS v,"
            +    FilterDbUtils.getFilteredWordQuestionCountTable() + " AS qc "
            + " WHERE qc.wordid=v.id";        
    }
    
    public static String[] getMkFilteredVocabularyTblIndices() {
        String[] sqlMkFilteredVocabularyTblIndices
        = {
              " CREATE INDEX " 
            +     FilterDbUtils.getFilteredVocabularyTable() + "_id_idx"
            + " ON " 
            +     FilterDbUtils.getFilteredVocabularyTable() 
            + " USING btree (id) WITH (FILLFACTOR = 100)" // the table is seldom change, use fillfactor 100
          };    
        return sqlMkFilteredVocabularyTblIndices;
    }
    
    public static String getMkFilteredTagTableFromSelect(Class<? extends DbUtilsBase> DbUtilsClass) {
        String tagTable;
        if (DbUtilsClass.getName().equals(DbUtils.class.getName())) {
            tagTable = DbUtils.getTagTable();
        } else if (DbUtilsClass.getName().equals(DbTagSelectorUtils.class.getName())) {
            tagTable = DbTagSelectorUtils.getTagTable();
        } else {
            LOGGER.error("unsupported class: " + DbUtilsClass.getName());
            tagTable = "Non_Existing_Table";
        }
        return
              " SELECT "
            +    " tq.tagid AS tagid, t.tagname "
            + " INTO " + FilterDbUtils.getFilteredTagTable()
            + " FROM "
            +      FilterDbUtils.getFilteredTagQuestionCountTable() + " AS tq, "
            +      tagTable + " AS t "
            + " WHERE tq.tagid=t.tagid";
    }
    
    public static String[] getMkFilteredTagTblIndices() {
        String[] sqlMkFilteredTagTblIndices
        = {
              " CREATE INDEX " 
            +     FilterDbUtils.getFilteredTagTable() + "_tagid_idx"
            + " ON " 
            +     FilterDbUtils.getFilteredTagTable() 
            + " USING btree (tagid) WITH (FILLFACTOR = 100)" // the table is seldom change, use fillfactor 100
          };    
        return sqlMkFilteredTagTblIndices;
    }    
    
    public static String getMkFilteredQuestionTagTableFromSelect(Class<? extends DbUtilsBase> DbUtilsClass) {
        String qtTable;
        if (DbUtilsClass.getName().equals(DbUtils.class.getName())) {
            qtTable = DbUtils.getQuestionTagTable();
        } else if (DbUtilsClass.getName().equals(DbTagSelectorUtils.class.getName())) {
            qtTable = DbTagSelectorUtils.getQuestionTagTable();
        } else {
            LOGGER.error("unsupported class: " + DbUtilsClass.getName());
            qtTable = "Non_Existing_Table";
        }
        
        return 
              " SELECT "
            +    " qt.postid AS postid, qc.tagid AS tagid "
            + " INTO " + FilterDbUtils.getFilteredQuestionTagTable()
            + " FROM " 
            +        qtTable + " AS qt, "
            +        FilterDbUtils.getFilteredTagQuestionCountTable() + " AS qc "
            + " WHERE qt.tagid=qc.tagid ";             
    }
    
    public static String[] getMkFilteredQuestionTagTblIndices() {
        String[] sqlMkFilteredQuestionTagTblIndices
        = {
              " CREATE INDEX " 
            +     FilterDbUtils.getFilteredQuestionTagTable() + "_postid_idx"
            + " ON " 
            +     FilterDbUtils.getFilteredQuestionTagTable() 
            + " USING btree (postid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
              " CREATE INDEX " 
            +     FilterDbUtils.getFilteredQuestionTagTable() + "_tagid_idx"
            + " ON " 
            +     FilterDbUtils.getFilteredQuestionTagTable() 
            + " USING btree (tagid) WITH (FILLFACTOR = 100)" // the table is seldom change, use fillfactor 100
          };    
        return sqlMkFilteredQuestionTagTblIndices;
    }     
    
    
    public static boolean purgeFilteredWorkingTables(String tableSuffix, String dbPropertiesFilename) {
        try (Connection conn = DbUtils.connect(dbPropertiesFilename)) {
            FilterDbUtils.setFilteredWorkingTablePrefix(tableSuffix);
            
            String[] workingTables = {
                    FilterDbUtils.getFilteredWordQuestionCountTable(),
                    FilterDbUtils.getFilteredQuestionWordTable(),
                    FilterDbUtils.getFilteredVocabularyTable(),
                    FilterDbUtils.getFilteredTagQuestionCountTable(),
                    FilterDbUtils.getFilteredTagTable(),
                    FilterDbUtils.getFilteredQuestionTagTable()
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
        } catch (SQLException e1) {
            return false;
        }
    }


    public static String getFilteredQuestionIdTable() {
        if (filteredQuestionIdTable == null || filteredQuestionIdTable.length() == 0) 
            return DEFAULT_FILTERED_QUESTION_ID_TABLE;
        else
            return filteredQuestionIdTable;
    }
 
}
