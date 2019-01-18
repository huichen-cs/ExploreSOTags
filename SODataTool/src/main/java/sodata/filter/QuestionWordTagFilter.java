package sodata.filter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.database.FilterDbUtils;
import sodata.database.DbUtils;
import sodata.database.DbUtilsBase;

public class QuestionWordTagFilter {
    private final static Logger LOGGER = LoggerFactory.getLogger(QuestionWordTagFilter.class);
    
    private String dbPropertiesFilename;
    
    public QuestionWordTagFilter(String tablePrefix, String dbPropertiesFilename) {
        if (tablePrefix != null) 
            FilterDbUtils.setFilteredWorkingTablePrefix(tablePrefix);
        this.dbPropertiesFilename = dbPropertiesFilename;
    }
    
    /**
     * Filter out words that are
     * 1. less than noBelow questions, or
     * 2. more than noAbove questions.
     * After filtering, word ids may be renumbered that they always start from 0, and
     * no gap in between. 
     * 
     * @param wordNoBelow threshold of the number of questions. Words that appear in questions less than the threshold
     *                are filtered out. 
     * @param wordNoAbove threshold of the number of questions. Words that appear in questions  more than the threshold
     *                are filtered out. 
     */
    public boolean filterExtremeWords(long wordNoBelow
            , long wordNoAbove
            , long tagNoBelow
            , long tagNoAbove
            , Class<? extends DbUtilsBase> DbUtilsClass) {
        Connection conn = null;
        
        try { 
            conn = DbUtils.connect(dbPropertiesFilename);
            
            conn.setAutoCommit(false);
            
            LOGGER.info("Making Filtered-Word-Questoin-Count table ...");
            String sql = FilterDbUtils.getMkFilteredWordQuestionCountTableFromSelect(DbUtilsClass);
            LOGGER.info("Sql: " + sql);
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setLong(1, wordNoBelow);
                pstmt.setLong(2, wordNoAbove);
                pstmt.executeUpdate();
            } 
            LOGGER.info("Created Filtered-Word-Questoin-Count table.");
            LOGGER.info("Making Filtered-Word-Questoin-Count table indices ...");
            DbUtils.createTable(conn, null,  null, FilterDbUtils.getMkFilteredWordQuestionCountTblIndices());
            LOGGER.info("Created Filtered-Word-Questoin-Count table indices.");
            
            
            LOGGER.info("Making Filtered-Question-Word table ...");
            sql = FilterDbUtils.getMkFilteredQuestionWordTableFromSelect(DbUtilsClass);
            LOGGER.info("Sql: " + sql);
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.executeUpdate();
            }
            LOGGER.info("Created Filtered-Question-Word table.");
            LOGGER.info("Making Filtered-Question-Word table indices ...");
            DbUtils.createTable(conn, null,  null, FilterDbUtils.getMkFilteredQuestionWordTblIndices());
            LOGGER.info("Created Filtered-Question-Word table indices.");
            
            
            LOGGER.info("Making Filtered-Vocabulary table ...");
            sql = FilterDbUtils.getMkFilteredVocabularyTableFromSelect(DbUtilsClass);
            LOGGER.info("Sql: " + sql);
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.executeUpdate();
            }
            LOGGER.info("Created Filtered-Vocabulary table.");
            LOGGER.info("Making Filtered-Vocabulary table indices ...");
            DbUtils.createTable(conn, null,  null, FilterDbUtils.getMkFilteredVocabularyTblIndices());
            LOGGER.info("Created Filtered-Vocabulary table indices.");
            
            
            LOGGER.info("Making Filtered-Tag-Question-Count table ...");
            sql = FilterDbUtils.getMkFilteredTagQuestionCountTableFromSelect(DbUtilsClass);
            LOGGER.info("Sql: " + sql);
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setLong(1, tagNoBelow);
                pstmt.setLong(2, tagNoAbove);               
                LOGGER.info("Execute sql: " + sql + ".");
                pstmt.executeUpdate();
            }
            LOGGER.info("Created Filtered-Tag-Question-Count table.");
            LOGGER.info("Making Filtered-Tag-Question-Count table indices...");
            DbUtils.createTable(conn, null,  null, FilterDbUtils.getMkFilteredTagQuestionCountTblIndices());
            LOGGER.info("Created Filtered-Tag-Question-Count table indices.");            
            
            
            LOGGER.info("Making Filtered-Tag table ...");
            sql = FilterDbUtils.getMkFilteredTagTableFromSelect(DbUtilsClass);
            LOGGER.info("Sql: " + sql);
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                LOGGER.info("Execute sql: " + sql + ".");
                pstmt.executeUpdate();
            } 
            LOGGER.info("Created Filtered-Tag table.");
            LOGGER.info("Making Filtered-Tag table indices ...");
            DbUtils.createTable(conn, null,  null, FilterDbUtils.getMkFilteredTagTblIndices());            
            LOGGER.info("Created Filtered-Tag table indices.");
            
            
            LOGGER.info("Making Filtered-Question-Tag table ...");
            sql = FilterDbUtils.getMkFilteredQuestionTagTableFromSelect(DbUtilsClass);
            LOGGER.info("Sql: " + sql);
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                LOGGER.info("Execute sql: " + sql + ".");
                pstmt.executeUpdate();
            }  
            LOGGER.info("Created Filtered-Question-Tag table.");
            LOGGER.info("Making Filtered-Question-Tag table indices ...");
            DbUtils.createTable(conn, null,  null, FilterDbUtils.getMkFilteredQuestionTagTblIndices());            
            LOGGER.info("Created Filtered-Question-Tag table indices.");         

            conn.commit();
            LOGGER.info("Filtered extremes.");
            return true;
        } catch (SQLException e) {
            LOGGER.error("Cannot create filtered tables.", e);
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.rollback();
                    conn.close();
                } catch (SQLException e) {
                    LOGGER.error("Failed to roll back transaction or close db connection.", e);
                }
            }
        }
        
    }
}
