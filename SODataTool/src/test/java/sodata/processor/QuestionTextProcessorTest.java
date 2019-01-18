package sodata.processor;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.database.DbUtils;
import sodata.testpreparation.TestDbBuilder;

public class QuestionTextProcessorTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(QuestionTextProcessorTest.class);
    private String dbPropertiesFilename = "sodumptestdb181.properties";
    private final String test_tbl_suffix = "_tst";

    @Test
    public void testCreateTempQuestionWordTable() {
        Connection conn = DbUtils.connect(dbPropertiesFilename);
        try {
            conn.setAutoCommit(false);

            String test_table_name = DbUtils.getTmpQuestionWordTable() + test_tbl_suffix;

            String sql_tmp_qw_tbl = DbUtils.getSqlMkTmpQWTable().replace(DbUtils.getTmpQuestionWordTable(), test_table_name);

            LOGGER.info("Sql = " + sql_tmp_qw_tbl);
            if (!DbUtils.tableExists(conn, DbUtils.getTmpQuestionWordTable())) {
                assertTrue(DbUtils.createTable(conn, sql_tmp_qw_tbl, null, null));
            }
        } catch (SQLException e) {
        } finally {
            try {
                conn.rollback();
            } catch (SQLException e) {
            }
        }

    }

    @Test
    public void testCreateVocabularyTable() {
        Connection conn = DbUtils.connect(dbPropertiesFilename);

        try {
            conn.setAutoCommit(false);

            String test_table_name = DbUtils.getVocabularyTable() + test_tbl_suffix;

            String sql_vocab_tbl = DbUtils.getSqlMkVocabTable().replaceAll(DbUtils.getVocabularyTable(), test_table_name);

            String[] sql_vocab_trigger = new String[DbUtils.getSqlMkVocabTblTrigger().length];
            for (int i = 0; i < DbUtils.getSqlMkVocabTblTrigger().length; i++) {
                String sql = DbUtils.getSqlMkVocabTblTrigger()[i];
                sql = sql.replace(DbUtils.getVocabularyTable(), test_table_name);
                sql_vocab_trigger[i] = sql;
            }

            String[] sql_vocab_tbl_indices = new String[DbUtils.getSqlMkVocabTblIndices().length];
            for (int i = 0; i < DbUtils.getSqlMkVocabTblIndices().length; i++) {
                String sql = DbUtils.getSqlMkVocabTblIndices()[i];
                sql = sql.replaceAll(DbUtils.getVocabularyTable(), test_table_name);
                sql_vocab_tbl_indices[i] = sql;
            }

            if (!DbUtils.tableExists(conn, test_table_name)) {
                assertTrue(DbUtils.createTable(conn, sql_vocab_tbl, sql_vocab_trigger, sql_vocab_tbl_indices));
            }
        } catch (SQLException e) {
        } finally {
            try {
                conn.rollback();
            } catch (SQLException e) {
            }
        }
    }

    @Test
    public void testCreateQuestionWordTable() {
        Connection conn = DbUtils.connect(dbPropertiesFilename);

        try {
            conn.setAutoCommit(false);
            String test_table_name = DbUtils.getQuestionWordTable() + test_tbl_suffix;

            String sql_qw_tbl = DbUtils.getSqlMkQwTable().replaceAll(DbUtils.getQuestionWordTable(), test_table_name);

            String[] sql_qw_tbl_indices = new String[DbUtils.getSqlMkQwTblIndices().length];
            for (int i = 0; i < DbUtils.getSqlMkQwTblIndices().length; i++) {
                String sql = DbUtils.getSqlMkQwTblIndices()[i];
                sql = sql.replaceAll(DbUtils.getQuestionWordTable(), test_table_name);
                sql_qw_tbl_indices[i] = sql;
                LOGGER.info(sql);
            }

            if (!DbUtils.tableExists(conn, test_table_name)) {
                assertTrue(DbUtils.createTable(conn, sql_qw_tbl, null, sql_qw_tbl_indices));
            }
        } catch (SQLException e) {

        } finally {
            try {
                conn.rollback();
            } catch (SQLException e) {
            }
        }
    }
    
    @Test
    public void testBuildWordQuestionTablesIgnoreCase() {       
        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1WORD_FILENAMES,
                    TestDbBuilder.TEST_1WORD_POSTIDS, 
                    TestDbBuilder.TEST_1WORD_TAGS_FILENAME,
                    TestDbBuilder.TEST_1WORD_POSTTAGS_FILENAMES,                 
                    TestDbBuilder.TEST_1WORD_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, true);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1WORD_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1WORD_QW));
        }

        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1W2DS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_POSTIDS, 
                    TestDbBuilder.TEST_1W2DS_TAGS_FILENAME,
                    TestDbBuilder.TEST_1W2DS_POSTTAGS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, true);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_QW));
        }

        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1REPETITIVE_WORD_FILENAMES,
                    TestDbBuilder.TEST_1REPETITIVE_WORD_POSTIDS, 
                    TestDbBuilder.TEST_1REPETITIVE_WORD_TAGS_FILENAME,
                    TestDbBuilder.TEST_1REPETITIVE_WORD_POSTTAGS_FILENAMES,
                    TestDbBuilder.TEST_1REPETITIVE_WORD_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, true);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1REPETITIVE_WORD_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1REPETITIVE_WORD_QW));
        }

        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_2W2DS_FILENAMES,
                    TestDbBuilder.TEST_2W2DS_POSTIDS, 
                    TestDbBuilder.TEST_2WORDS_TAGS_FILENAME,
                    TestDbBuilder.TEST_2WORDS_POSTTAGS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, true);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_2W2DS_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_2W2DS_QW));
        }

        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_2WORDS_FILENAMES,
                    TestDbBuilder.TEST_2WORDS_POSTIDS,
                    TestDbBuilder.TEST_2WORDS_TAGS_FILENAME,
                    TestDbBuilder.TEST_2WORDS_POSTTAGS_FILENAMES,                    
                    TestDbBuilder.TEST_2WORDS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, true);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_2WORDS_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_2WORDS_QW));
        }

        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_SODATA_FILENAMES,
                    TestDbBuilder.TEST_SODATA_POSTIDS, 
                    TestDbBuilder.TEST_SODATA_TAGS_FILENAME,
                    TestDbBuilder.TEST_SODATA_POSTTAGS_FILENAMES,
                    TestDbBuilder.TEST_SODATA_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, true);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_VOCAB_IGNORECASE));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_QW_IGNORECASE));
        }
        

    }
   
    @Test
    public void testBuildWordQuestionTables() {       
        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1WORD_FILENAMES,
                    TestDbBuilder.TEST_1WORD_POSTIDS, 
                    TestDbBuilder.TEST_1WORD_TAGS_FILENAME,
                    TestDbBuilder.TEST_1WORD_POSTTAGS_FILENAMES,                 
                    TestDbBuilder.TEST_1WORD_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, false, false);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1WORD_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1WORD_QW));
        }

        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1W2DS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_POSTIDS, 
                    TestDbBuilder.TEST_1W2DS_TAGS_FILENAME,
                    TestDbBuilder.TEST_1W2DS_POSTTAGS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, false, false);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_QW));
        }

        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1REPETITIVE_WORD_FILENAMES,
                    TestDbBuilder.TEST_1REPETITIVE_WORD_POSTIDS, 
                    TestDbBuilder.TEST_1REPETITIVE_WORD_TAGS_FILENAME,
                    TestDbBuilder.TEST_1REPETITIVE_WORD_POSTTAGS_FILENAMES,
                    TestDbBuilder.TEST_1REPETITIVE_WORD_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, false, false);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1REPETITIVE_WORD_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1REPETITIVE_WORD_QW));
        }

        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_2W2DS_FILENAMES,
                    TestDbBuilder.TEST_2W2DS_POSTIDS, 
                    TestDbBuilder.TEST_2WORDS_TAGS_FILENAME,
                    TestDbBuilder.TEST_2WORDS_POSTTAGS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, false, false);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_2W2DS_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_2W2DS_QW));
        }

        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_2WORDS_FILENAMES,
                    TestDbBuilder.TEST_2WORDS_POSTIDS,
                    TestDbBuilder.TEST_2WORDS_TAGS_FILENAME,
                    TestDbBuilder.TEST_2WORDS_POSTTAGS_FILENAMES,                    
                    TestDbBuilder.TEST_2WORDS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, false, false);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_2WORDS_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_2WORDS_QW));
        }

        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_SODATA_FILENAMES,
                    TestDbBuilder.TEST_SODATA_POSTIDS, 
                    TestDbBuilder.TEST_SODATA_TAGS_FILENAME,
                    TestDbBuilder.TEST_SODATA_POSTTAGS_FILENAMES,
                    TestDbBuilder.TEST_SODATA_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, false, false);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_QW));
        }
        

    } 
    
    @Test
    public void testBuildWordQuestionTablesUsingCopy() {       
        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1WORD_FILENAMES,
                    TestDbBuilder.TEST_1WORD_POSTIDS, 
                    TestDbBuilder.TEST_1WORD_TAGS_FILENAME,
                    TestDbBuilder.TEST_1WORD_POSTTAGS_FILENAMES,                 
                    TestDbBuilder.TEST_1WORD_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, false);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1WORD_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1WORD_QW));
        }

        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1W2DS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_POSTIDS, 
                    TestDbBuilder.TEST_1W2DS_TAGS_FILENAME,
                    TestDbBuilder.TEST_1W2DS_POSTTAGS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, false);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_QW));
        }

        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1REPETITIVE_WORD_FILENAMES,
                    TestDbBuilder.TEST_1REPETITIVE_WORD_POSTIDS, 
                    TestDbBuilder.TEST_1REPETITIVE_WORD_TAGS_FILENAME,
                    TestDbBuilder.TEST_1REPETITIVE_WORD_POSTTAGS_FILENAMES,
                    TestDbBuilder.TEST_1REPETITIVE_WORD_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, false);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1REPETITIVE_WORD_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1REPETITIVE_WORD_QW));
        }

        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_2W2DS_FILENAMES,
                    TestDbBuilder.TEST_2W2DS_POSTIDS, 
                    TestDbBuilder.TEST_2WORDS_TAGS_FILENAME,
                    TestDbBuilder.TEST_2WORDS_POSTTAGS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, false);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_2W2DS_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_2W2DS_QW));
        }

        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_2WORDS_FILENAMES,
                    TestDbBuilder.TEST_2WORDS_POSTIDS,
                    TestDbBuilder.TEST_2WORDS_TAGS_FILENAME,
                    TestDbBuilder.TEST_2WORDS_POSTTAGS_FILENAMES,                    
                    TestDbBuilder.TEST_2WORDS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, false);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_2WORDS_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_2WORDS_QW));
        }

        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_SODATA_FILENAMES,
                    TestDbBuilder.TEST_SODATA_POSTIDS, 
                    TestDbBuilder.TEST_SODATA_TAGS_FILENAME,
                    TestDbBuilder.TEST_SODATA_POSTTAGS_FILENAMES,
                    TestDbBuilder.TEST_SODATA_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, false);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_QW));
        }
        

    }        
    
    @Test
    public void testCreateVocabularyTableWithStartDate() {    
        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1WORD_FILENAMES,
                    TestDbBuilder.TEST_1WORD_POSTIDS,
                    TestDbBuilder.TEST_1WORD_TAGS_FILENAME,
                    TestDbBuilder.TEST_1WORD_POSTTAGS_FILENAMES,                     
                    TestDbBuilder.TEST_1WORD_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, false);
            builder.setStartDate(TestDbBuilder.TEST_1WORD_STARTDATE_1);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1WORD_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1WORD_QW));
        }    
        
        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1W2DS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_POSTIDS, 
                    TestDbBuilder.TEST_1W2DS_TAGS_FILENAME,
                    TestDbBuilder.TEST_1W2DS_POSTTAGS_FILENAMES,                    
                    TestDbBuilder.TEST_1W2DS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, false);
            builder.setStartDate(TestDbBuilder.TEST_1W2DS_STARTDATE_1);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_VOCAB_SD_1));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_QW_SD_1));
        }        
    }
    
    @Test
    public void testCreateVocabularyTableWithEndDate() {    
        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1W2DS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_POSTIDS, 
                    TestDbBuilder.TEST_1W2DS_TAGS_FILENAME,
                    TestDbBuilder.TEST_1W2DS_POSTTAGS_FILENAMES,                    
                    TestDbBuilder.TEST_1W2DS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, false);
            builder.setEndDate(TestDbBuilder.TEST_1W2DS_ENDDATE_1);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_VOCAB_ED_1));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_QW_ED_1));
        }    
        
        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1W2DS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_POSTIDS, 
                    TestDbBuilder.TEST_1W2DS_TAGS_FILENAME,
                    TestDbBuilder.TEST_1W2DS_POSTTAGS_FILENAMES,                    
                    TestDbBuilder.TEST_1W2DS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, false);
            builder.setEndDate(TestDbBuilder.TEST_1W2DS_ENDDATE_0);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_VOCAB));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_QW));
        }        
    }   
    
    @Test
    public void testCreateVocabularyTableWithStartEndDates() {    
        
        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1W2DS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_POSTIDS, 
                    TestDbBuilder.TEST_1W2DS_TAGS_FILENAME,
                    TestDbBuilder.TEST_1W2DS_POSTTAGS_FILENAMES,                    
                    TestDbBuilder.TEST_1W2DS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, false);
            builder.setStartDate(TestDbBuilder.TEST_1W2DS_STARTDATE_1);
            builder.setEndDate(TestDbBuilder.TEST_1W2DS_ENDDATE_0);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_VOCAB_SD_1));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_QW_SD_1));
        }   
        
        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1W2DS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_POSTIDS, 
                    TestDbBuilder.TEST_1W2DS_TAGS_FILENAME,
                    TestDbBuilder.TEST_1W2DS_POSTTAGS_FILENAMES,                    
                    TestDbBuilder.TEST_1W2DS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, false);
            builder.setStartDate(TestDbBuilder.TEST_1W2DS_STARTDATE_0);
            builder.setEndDate(TestDbBuilder.TEST_1W2DS_ENDDATE_1);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_VOCAB_ED_1));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_QW_ED_1));
        }           
    }  
    
 
    @Test
    public void testCreateVocabularyTableWithStartEndDatesIgnoreCase() {    
        
        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1W2DS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_POSTIDS, 
                    TestDbBuilder.TEST_1W2DS_TAGS_FILENAME,
                    TestDbBuilder.TEST_1W2DS_POSTTAGS_FILENAMES,                    
                    TestDbBuilder.TEST_1W2DS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, false);
            builder.setStartDate(TestDbBuilder.TEST_1W2DS_STARTDATE_1);
            builder.setEndDate(TestDbBuilder.TEST_1W2DS_ENDDATE_0);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_VOCAB_SD_1));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_QW_SD_1));
        }   
        
        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1W2DS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_POSTIDS, 
                    TestDbBuilder.TEST_1W2DS_TAGS_FILENAME,
                    TestDbBuilder.TEST_1W2DS_POSTTAGS_FILENAMES,                    
                    TestDbBuilder.TEST_1W2DS_CREATIONDATES));
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename, true, false);
            builder.setStartDate(TestDbBuilder.TEST_1W2DS_STARTDATE_0);
            builder.setEndDate(TestDbBuilder.TEST_1W2DS_ENDDATE_1);
            assertTrue(builder.buildWordQuestionTables());
            assertTrue(TestDbBuilder.uniqWords(dbPropertiesFilename));
            assertTrue(TestDbBuilder.correctVocab(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_VOCAB_ED_1));
            assertTrue(TestDbBuilder.correctQW(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_QW_ED_1));
        }           
    }      
    
}


