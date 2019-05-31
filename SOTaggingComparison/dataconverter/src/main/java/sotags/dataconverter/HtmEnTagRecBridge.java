package sotags.dataconverter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HtmEnTagRecBridge {
    private final static Logger LOGGER = LoggerFactory.getLogger(HtmEnTagRecBridge.class);
    private final static String TRAIN_DATA_FILENAME = "trainDataset_distr.csv";
    private final static String TEST_DATA_FILENAME = "testDataset.csv";
//    private final static String GOLD_DATA_FILENAME = "goldenSet.csv";
    private final static String HTM_DATASET_NAME = "l2h";
    
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("SVModelUtil root num_of_folds num_of_repetitions");
            return;
        }

        String root = args[0];
        int numFolds = Integer.parseInt(args[1]);
        int numRepetitions = Integer.parseInt(args[2]);
        LOGGER.info("CML args (root, num_of_folds) = (" + root + ", " + numFolds + ")");

        try {
            for (int i = 0; i < numRepetitions; i++) {
                Path trainDirPath = Paths.get(root, "testcase_repeat" + i);
                for (int j = 0; j < numFolds; j++) {
                    convertEntagrecDataForSVL2hModel(j, trainDirPath);
                }
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
    
    public static void convertEntagrecDataForSVL2hModel(int crossNumber, Path trainDirPath) throws IOException {
        List<Doc> trainDocList = loadEnTagRecDataSet(crossNumber, trainDirPath, TRAIN_DATA_FILENAME, true);
        List<Doc> testDocList = loadEnTagRecDataSet(crossNumber, trainDirPath, TEST_DATA_FILENAME, false);

        Set<String> wordSet = getItemSet(trainDocList, doc -> doc.getWordIterator());
        LOGGER.info("Got " + wordSet.size() + " words.");
        L2hDataSet.makeWordVocabularyFile(crossNumber, HTM_DATASET_NAME, trainDirPath, wordSet);

        Set<String> tagSet = getItemSet(trainDocList, doc -> doc.getTagIterator());
        LOGGER.info("Got " + tagSet.size() + " tags.");
        L2hDataSet.makeTagVocabularyFile(crossNumber, HTM_DATASET_NAME, trainDirPath, tagSet);
        
        LinkedHashMap<String, Integer> wordIdMap = L2hDataSet.initWordIdMapFromWordVocFile(crossNumber, HTM_DATASET_NAME, trainDirPath);
        L2hDataSet.makeQuestionWordFreqFiles(crossNumber, HTM_DATASET_NAME, trainDirPath, wordIdMap, trainDocList, testDocList);

        L2hDataSet.makeQuestionTagFile(crossNumber, HTM_DATASET_NAME, trainDirPath, trainDocList);
    }

    private static List<Doc> loadEnTagRecDataSet(int crossNumber, Path dataDirPath, String dataSetFilename, boolean hasTag) throws IOException {
        List<Doc> docList = new LinkedList<Doc>();
        
        Path dataFilePath = Paths.get(dataDirPath.toString(), Integer.toString(crossNumber),  dataSetFilename);
        List<String> lines = Files.readAllLines(dataFilePath);
        LOGGER.info("Read " + lines.size() + " from " + dataFilePath.toString());
        for (String line: lines) {
            docList.add(Doc.fromString(line, hasTag));
        }
        return docList;
    }
    
    private static Set<String> getItemSet(List<Doc> docList, Function<Doc, Iterator<String>> processor) {
        Set<String> set = new LinkedHashSet<String>();
        docList.forEach(doc -> {
            Iterator<String> iter = processor.apply(doc);
            if (iter == null) {
                throw new IllegalStateException("Document " + doc.getId() + " doesn't have tags.");
            }
            while (iter.hasNext()) {
                set.add(iter.next());
            }
        });
        return set;
    }
}
