package sodata.filter;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.database.DbUtils;
import sodata.database.FilterDbUtils;
import sodata.processor.QuestionTagProcessor;
import sodata.processor.QuestionTextProcessor;
import sodata.testpreparation.TestDbBuilder;

public class QuestionWordTagFilterTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(QuestionWordTagFilterTest.class);    

    @Test
    public void testFilterExtremeWords() {
        String dbPropertiesFilename = "sodumptestdb181.properties";
        String tableSuffix = "_F"; 
        
        assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                TestDbBuilder.TEST_SODATA_FILENAMES,
                TestDbBuilder.TEST_SODATA_POSTIDS, 
                TestDbBuilder.TEST_SODATA_TAGS_FILENAME,
                TestDbBuilder.TEST_SODATA_POSTTAGS_FILENAMES,                    
                TestDbBuilder.TEST_SODATA_CREATIONDATES));        
        
        QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename);
        // assertTrue(builder.buildWordQuestionTablesHighMemory());        
        assertTrue(builder.buildWordQuestionTables());            
        
        QuestionTagProcessor processor = new QuestionTagProcessor(dbPropertiesFilename);
        assertTrue(processor.buildTagTables());
        assertTrue(TestDbBuilder.correctQuestionId(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_QID));
        assertTrue(TestDbBuilder.correctQuestionTag(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_QUESTIONTAG));
        assertTrue(TestDbBuilder.correctTag(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_TAG));

        QuestionWordTagFilter filter = new QuestionWordTagFilter(tableSuffix, dbPropertiesFilename);
        FilterDbUtils.purgeFilteredWorkingTables(tableSuffix, dbPropertiesFilename);
        assertTrue(filter.filterExtremeWords(2, 4, 2, 4, DbUtils.class));
        
        LOGGER.info("Completed for TEST_SODATA_*.");           
    }  

}
