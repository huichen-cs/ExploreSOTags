package sodata.lldamodel.docmodel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.TextDataset;

public class TestTextDataset extends TextDataset {
	private final static Logger LOGGER = LoggerFactory.getLogger(TestTextDataset.class);
	
	private String testFilename;
	
	public TestTextDataset(String dataset, String formatFolder) throws FileNotFoundException, IOException, Exception {
		super(dataset);
		testFilename  = Paths.get(formatFolder, dataset + "_test.data").toString();
		LOGGER.debug("testFilename: " + testFilename);
		File testFile = new File(testFilename);
		inputTextData(testFile);
	}
	
	public int[][] getWords() {
		return words;
	}
}
