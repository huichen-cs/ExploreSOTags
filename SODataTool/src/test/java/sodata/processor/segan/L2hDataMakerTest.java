package sodata.processor.segan;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.processor.QuestionTagProcessor;
import sodata.processor.QuestionTextProcessor;
import sodata.testpreparation.TestDbBuilder;

public class L2hDataMakerTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(L2hDataMakerTest.class);

    @Test
    public void testMakeL2HDataset1Word() {
        
        String datasetName = "t1word"; 
        String datasetFolder = "../SOResults/testdata/t1word";
        String tablePrefix = "wk_"; 
        String dbPropertiesFilename = "sodumptestdb181.properties";
        
        assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                TestDbBuilder.TEST_1WORD_FILENAMES,
                TestDbBuilder.TEST_1WORD_POSTIDS, 
                TestDbBuilder.TEST_1WORD_TAGS_FILENAME,
                TestDbBuilder.TEST_1WORD_POSTTAGS_FILENAMES,                    
                TestDbBuilder.TEST_1WORD_CREATIONDATES));        
        
        QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename);
        assertTrue(builder.buildWordQuestionTables());        
        
        QuestionTagProcessor processor = new QuestionTagProcessor(dbPropertiesFilename);
        assertTrue(processor.buildTagTables());
        assertTrue(TestDbBuilder.correctQuestionId(dbPropertiesFilename, TestDbBuilder.TEST_1WORD_QID));
        assertTrue(TestDbBuilder.correctQuestionTag(dbPropertiesFilename, TestDbBuilder.TEST_1WORD_QUESTIONTAG));
        assertTrue(TestDbBuilder.correctTag(dbPropertiesFilename, TestDbBuilder.TEST_1WORD_TAG));
        
        assertTrue(L2hDataMaker.makeDatasetFolder(datasetFolder));
        assertTrue(L2hDataMaker.makeL2HDataset(datasetName, datasetFolder, tablePrefix, dbPropertiesFilename));

        assertTrue(identicalFileContent(L2hDataMaker.getWordVocabularyFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_1WORD_WVOC_FILE_LINES));
        assertTrue(identicalFileContent(L2hDataMaker.getTagVocabularyFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_1WORD_LVOC_FILE_LINES));
        assertTrue(identicalFileContent(L2hDataMaker.getQuestionWordFreqFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_1WORD_DOCTERMFREQ_FILE_LINES));
        assertTrue(identicalFileContent(L2hDataMaker.getQuestionTagFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_1WORD_DOCLABEL_FILE_LINES));
        
        assert(L2hDataMaker.cleanDatasetFolder(datasetFolder));
        LOGGER.info("Completed for TEST_1WORD_*.");
    }
    
    @Test
    public void testMakeL2HDataset1W2DS() {
        
        String datasetName = "t1w2ds"; 
        String datasetFolder = "../SOResults/testdata/t1w2ds";
        String tablePrefix = "wk_"; 
        String dbPropertiesFilename = "sodumptestdb181.properties";
        
        assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                TestDbBuilder.TEST_1W2DS_FILENAMES,
                TestDbBuilder.TEST_1W2DS_POSTIDS, 
                TestDbBuilder.TEST_1W2DS_TAGS_FILENAME,
                TestDbBuilder.TEST_1W2DS_POSTTAGS_FILENAMES,
                TestDbBuilder.TEST_1W2DS_CREATIONDATES));
        
        QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename);
        assertTrue(builder.buildWordQuestionTables());
        
        QuestionTagProcessor processor = new QuestionTagProcessor(dbPropertiesFilename);
        assertTrue(processor.buildTagTables());
        assertTrue(TestDbBuilder.correctQuestionId(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_QID));
        assertTrue(TestDbBuilder.correctQuestionTag(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_QUESTIONTAG));
        assertTrue(TestDbBuilder.correctTag(dbPropertiesFilename, TestDbBuilder.TEST_1W2DS_TAG));
        
        assertTrue(L2hDataMaker.makeDatasetFolder(datasetFolder));
        assertTrue(L2hDataMaker.makeL2HDataset(datasetName, datasetFolder, tablePrefix, dbPropertiesFilename));

        assertTrue(identicalFileContent(L2hDataMaker.getWordVocabularyFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_1W2DS_WVOC_FILE_LINES));
        assertTrue(identicalFileContent(L2hDataMaker.getTagVocabularyFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_1W2DS_LVOC_FILE_LINES));
        assertTrue(identicalFileContent(L2hDataMaker.getQuestionWordFreqFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_1W2DS_DOCTERMFREQ_FILE_LINES));
        assertTrue(identicalFileContent(L2hDataMaker.getQuestionTagFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_1W2DS_DOCLABEL_FILE_LINES));
        
        assert(L2hDataMaker.cleanDatasetFolder(datasetFolder));        
        LOGGER.info("Completed for TEST_1W2DS_*.");
    }
    
    @Test
    public void testMakeL2HDataset2W2DS() {
        
        String datasetName = "t1w2ds"; 
        String datasetFolder = "../SOResults/testdata/t2w2ds";
        String tablePrefix = "wk_"; 
        String dbPropertiesFilename = "sodumptestdb181.properties";
        
        assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                TestDbBuilder.TEST_2W2DS_FILENAMES,
                TestDbBuilder.TEST_2W2DS_POSTIDS, 
                TestDbBuilder.TEST_2W2DS_TAGS_FILENAME,
                TestDbBuilder.TEST_2W2DS_POSTTAGS_FILENAMES, 
                TestDbBuilder.TEST_2W2DS_CREATIONDATES));
        
        QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename);
        assertTrue(builder.buildWordQuestionTables());
        
        QuestionTagProcessor processor = new QuestionTagProcessor(dbPropertiesFilename);
        assertTrue(processor.buildTagTables());
        assertTrue(TestDbBuilder.correctQuestionId(dbPropertiesFilename, TestDbBuilder.TEST_2W2DS_QID));
        assertTrue(TestDbBuilder.correctQuestionTag(dbPropertiesFilename, TestDbBuilder.TEST_2W2DS_QUESTIONTAG));
        assertTrue(TestDbBuilder.correctTag(dbPropertiesFilename, TestDbBuilder.TEST_2W2DS_TAG));
        
        assertTrue(L2hDataMaker.makeDatasetFolder(datasetFolder));
        assertTrue(L2hDataMaker.makeL2HDataset(datasetName, datasetFolder, tablePrefix, dbPropertiesFilename));

        assertTrue(identicalFileContent(L2hDataMaker.getWordVocabularyFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_2W2DS_WVOC_FILE_LINES));
        assertTrue(identicalFileContent(L2hDataMaker.getTagVocabularyFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_2W2DS_LVOC_FILE_LINES));
        assertTrue(identicalFileContent(L2hDataMaker.getQuestionWordFreqFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_2W2DS_DOCTERMFREQ_FILE_LINES));
        assertTrue(identicalFileContent(L2hDataMaker.getQuestionTagFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_2W2DS_DOCLABEL_FILE_LINES));
        
        assert(L2hDataMaker.cleanDatasetFolder(datasetFolder));        
        LOGGER.info("Completed for TEST_2W2DS_*.");
    }
    
    @Test
    public void testMakeL2HDatasetSODATA() {
        
        String datasetName = "sodata"; 
        String datasetFolder = "../SOResults/testdata/sodata";
        String tablePrefix = "wk_"; 
        String dbPropertiesFilename = "sodumptestdb181.properties";
        
        assertTrue(TestDbBuilder.populateTestDb(dbPropertiesFilename, 
                TestDbBuilder.TEST_SODATA_FILENAMES,
                TestDbBuilder.TEST_SODATA_POSTIDS, 
                TestDbBuilder.TEST_SODATA_TAGS_FILENAME,
                TestDbBuilder.TEST_SODATA_POSTTAGS_FILENAMES,                    
                TestDbBuilder.TEST_SODATA_CREATIONDATES));        
        
        QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename);
        assertTrue(builder.buildWordQuestionTables());        
        
        QuestionTagProcessor processor = new QuestionTagProcessor(dbPropertiesFilename);
        assertTrue(processor.buildTagTables());
        assertTrue(TestDbBuilder.correctQuestionId(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_QID));
        assertTrue(TestDbBuilder.correctQuestionTag(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_QUESTIONTAG));
        assertTrue(TestDbBuilder.correctTag(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_TAG));
        
        assertTrue(L2hDataMaker.makeDatasetFolder(datasetFolder));
        assertTrue(L2hDataMaker.makeL2HDataset(datasetName, datasetFolder, tablePrefix, dbPropertiesFilename));

        assertTrue(identicalFileContent(L2hDataMaker.getWordVocabularyFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_SODATA_WVOC_FILE_LINES));
        assertTrue(identicalFileContent(L2hDataMaker.getTagVocabularyFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_SODATA_LVOC_FILE_LINES));
        assertTrue(identicalFileContent(L2hDataMaker.getQuestionWordFreqFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_SODATA_DOCTERMFREQ_FILE_LINES));
        assertTrue(identicalFileContent(L2hDataMaker.getQuestionTagFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_SODATA_DOCLABEL_FILE_LINES));
        
        //assert(L2hDataMaker.cleanDatasetFolder(datasetFolder));        
        LOGGER.info("Completed for TEST_SODATA_*.");
    }
    
    @Test
    public void testMakeL2HDatasetSODATALive() {
        
        long[] postIds = {2041, 17054, 17612, 23063, 66542};
        String datasetName = "sodatalive"; 
        String datasetFolder = "../SOResults/testdata/sodatalive";
        String tablePrefix = "wk_"; 
        String dbPropertiesFilename = "sodumptestdb181.properties";
        String srcDbPropertiesFilename = "sodumpdb181.properties";
        
        assertTrue(TestDbBuilder.populatesTestDb(dbPropertiesFilename, 
                srcDbPropertiesFilename, postIds));        
        
        QuestionTextProcessor builder = new QuestionTextProcessor(dbPropertiesFilename);
        assertTrue(builder.buildWordQuestionTables());        
        
        QuestionTagProcessor processor = new QuestionTagProcessor(dbPropertiesFilename);
        assertTrue(processor.buildTagTables());
        assertTrue(TestDbBuilder.correctQuestionId(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_LIVE_QID));
        assertTrue(TestDbBuilder.correctQuestionTag(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_LIVE_QUESTIONTAG));
        assertTrue(TestDbBuilder.correctTag(dbPropertiesFilename, TestDbBuilder.TEST_SODATA_LIVE_TAG));
        
        assertTrue(L2hDataMaker.makeDatasetFolder(datasetFolder));
        assertTrue(L2hDataMaker.makeL2HDataset(datasetName, datasetFolder, tablePrefix, dbPropertiesFilename));

        assertTrue(identicalFileContent(L2hDataMaker.getWordVocabularyFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_SODATA_LIVE_WVOC_FILE_LINES));
        assertTrue(identicalFileContent(L2hDataMaker.getTagVocabularyFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_SODATA_LIVE_LVOC_FILE_LINES));
        assertTrue(identicalFileContent(L2hDataMaker.getQuestionWordFreqFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_SODATA_LIVE_DOCTERMFREQ_FILE_LINES));
        assertTrue(identicalFileContent(L2hDataMaker.getQuestionTagFilePath(datasetFolder, datasetName).toString(), TestDbBuilder.TEST_SODATA_LIVE_DOCLABEL_FILE_LINES));
        
        //assert(L2hDataMaker.cleanDatasetFolder(datasetFolder));        
        LOGGER.info("Completed for TEST_SODATA_LIVE_*.");
    }
    
    private boolean identicalFileContent(String fn, String[] wantedFileContent) {
        Path filePath = Paths.get(fn);
        File file = new File(filePath.toString());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            String line = null;
            int i = 0;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().equals(wantedFileContent[i].trim())) {
                    LOGGER.error("expected \n[" + wantedFileContent[i].trim() + "], encountered \n[" + line.trim() + "]");
                    return false;
                }
                i ++;
            }
            LOGGER.info("Identical " + fn);
            return true;
        } catch (IOException e) {
            LOGGER.error("Cannot locate file " + filePath.toString());
            return false;
        }
    }
}
