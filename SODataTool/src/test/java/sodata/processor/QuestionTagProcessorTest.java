package sodata.processor;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sodata.testpreparation.TestDbBuilder;

import static org.junit.Assert.assertTrue;

public class QuestionTagProcessorTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(QuestionTagProcessorTest.class);

    @Test
    public void testbuildTagTables() {
        final String dbPropertiesFilename = "sodumptestdb181.properties";
        
        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_SODATA_FILENAMES,
                    TestDbBuilder.TEST_SODATA_POSTIDS, 
                    TestDbBuilder.TEST_SODATA_TAGS_FILENAME,
                    TestDbBuilder.TEST_SODATA_POSTTAGS_FILENAMES,                    
                    TestDbBuilder.TEST_SODATA_CREATIONDATES));        
            
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename);
            //assertTrue(builder.buildWordQuestionTablesHighMemory());        
            assertTrue(builder.buildWordQuestionTables()); 
            
            QuestionTagProcessor processor = new QuestionTagProcessor(dbPropertiesFilename);
            assertTrue(processor.buildTagTables());
            assertTrue(TestDbBuilder.correctQuestionId(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_QID));
            assertTrue(TestDbBuilder.correctQuestionTag(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_QUESTIONTAG));
            assertTrue(TestDbBuilder.correctTag(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_TAG));

            
            LOGGER.info("Completed for TEST_SODATA_*.");
        }
        
        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1WORD_FILENAMES,
                    TestDbBuilder.TEST_1WORD_POSTIDS, 
                    TestDbBuilder.TEST_1WORD_TAGS_FILENAME,
                    TestDbBuilder.TEST_1WORD_POSTTAGS_FILENAMES,                    
                    TestDbBuilder.TEST_1WORD_CREATIONDATES));        
            
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename);
            // assertTrue(builder.buildWordQuestionTablesHighMemory());        
            assertTrue(builder.buildWordQuestionTables());        

            
            QuestionTagProcessor processor = new QuestionTagProcessor(dbPropertiesFilename);
            assertTrue(processor.buildTagTables());
            assertTrue(TestDbBuilder.correctQuestionId(dbPropertiesFilename, TestDbBuilder.TEST_1WORD_QID));
            assertTrue(TestDbBuilder.correctQuestionTag(dbPropertiesFilename, TestDbBuilder.TEST_1WORD_QUESTIONTAG));
            assertTrue(TestDbBuilder.correctTag(dbPropertiesFilename, TestDbBuilder.TEST_1WORD_TAG));
            
            LOGGER.info("Completed for TEST_1WORD_*.");
        }     
        
        {
            assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                    TestDbBuilder.TEST_1W2DS_FILENAMES,
                    TestDbBuilder.TEST_1W2DS_POSTIDS, 
                    TestDbBuilder.TEST_1W2DS_TAGS_FILENAME,
                    TestDbBuilder.TEST_1W2DS_POSTTAGS_FILENAMES,                    
                    TestDbBuilder.TEST_1W2DS_CREATIONDATES));        
            
            QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename);
            // assertTrue(builder.buildWordQuestionTablesHighMemory());     
            assertTrue(builder.buildWordQuestionTables());  
            
            QuestionTagProcessor processor = new QuestionTagProcessor(dbPropertiesFilename);
            assertTrue(processor.buildTagTables());
            assertTrue(TestDbBuilder.correctQuestionId(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_QID));
            assertTrue(TestDbBuilder.correctQuestionTag(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_QUESTIONTAG));
            assertTrue(TestDbBuilder.correctTag(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_TAG));
            
            LOGGER.info("Completed for TEST_1W2DS_*.");
        }          
    }
}
