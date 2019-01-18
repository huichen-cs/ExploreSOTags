package sodata.processor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.database.DbUtils;

public class QuestionTagProcessor {
    private final static Logger LOGGER = LoggerFactory.getLogger(QuestionTextProcessor.class);
    
    public String dbPropertiesFilename;
    
    public QuestionTagProcessor() {
        dbPropertiesFilename = DbUtils.SODUMPDB_PROPERTIES_FN;
    }
    
    public QuestionTagProcessor(String dbPropertiesFilename) {
        this.dbPropertiesFilename = dbPropertiesFilename;
    }
    
    private boolean buildQuestionIdTable(Connection conn) {
        if (!DbUtils.tableExists(conn, DbUtils.getQuestionWordTable())) {
            LOGGER.error("Table " 
                    + DbUtils.getQuestionWordTable() 
                    + " does not exist. "
                    + " Run QuestionTextProcessor or QuestionTextProcessorHighMemory to build the table");
            return false;
        }
        LOGGER.info("Table " + DbUtils.getQuestionWordTable() + " exists.");
        
        try (PreparedStatement pstmt = conn.prepareStatement(DbUtils.getSqlMkQuestionIdTableFromQuestionWordTable())) {
            pstmt.executeUpdate();
            return DbUtils.createTable(conn, null, DbUtils.getSqlMkQuestionIdTblIndices(), null);
        } catch (SQLException e) {
            LOGGER.error("Failed to create " + DbUtils.getQuestionIdTable(), e);
            return false;
        }
    }
    
    private boolean buildQuestionTagTable(Connection conn) {
        try (PreparedStatement pstmt = conn.prepareStatement(DbUtils.getMkPostTagTableFromPosttagsQuestionidTables())) {
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
          LOGGER.error("Failed to build question-tag table.", e);  
          return false;
        }
    }
    
    private boolean buildTagTable(Connection conn) {
        try (PreparedStatement pstmt = conn.prepareStatement(DbUtils.getMkTagsTableFromQuestiontagTagsTables())) {
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
          LOGGER.error("Failed to build question-tag table.", e);  
          return false;
        }        
    }
    
    public boolean buildTagTables() {
        Connection conn = null;
        try {
            conn = DbUtils.connect(dbPropertiesFilename);
            LOGGER.info("Established db connection using " + dbPropertiesFilename);
            
            conn.setAutoCommit(false);
            
            if (!buildQuestionIdTable(conn)) {
                LOGGER.error("Failed to create Question-Id table.");
                return false;
            }
            LOGGER.info("Created Question-Id table.");
            
            if (!buildQuestionTagTable(conn)) {
                LOGGER.error("Failed to create Question-Tag table.");
                return false;
            }
            LOGGER.info("Created Question-Tag table.");
            
            if (!buildTagTable(conn)) {
                LOGGER.error("Failed to create Tags table.");
                return false;
            }
            LOGGER.info("Created Tags table.");
            
            conn.commit();
            return true;
        } catch (SQLException e) {
            LOGGER.error("Cannot establish db connection with " + dbPropertiesFilename, e);          
            return false;
        } finally {
            try {
                if (conn != null) {
                    conn.rollback(); // no effect on committed transactions
                    conn.commit();
                }
            } catch (SQLException e) {
                LOGGER.error("failed to rollback or close db.", e);
            }
        }
    }
    
    public static void main(String[] args) {

        if (args.length != 1) {
            LOGGER.info("Usage: QuestionTagProcessor <db_table_prefix>");
            return;
        }
        String tablePrefix = args[0];
        DbUtils.setWorkingTablePrefix(tablePrefix);
        
        QuestionTagProcessor processor = new QuestionTagProcessor();
        
        if (processor.buildTagTables()) {
            LOGGER.info("Build tags tables.");
        } else {
            LOGGER.error("Failed to build tags tables.");
        }
                
    }
}
