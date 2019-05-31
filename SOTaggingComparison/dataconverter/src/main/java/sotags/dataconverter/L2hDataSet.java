package sotags.dataconverter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class L2hDataSet {
    private final static Logger LOGGER = LoggerFactory.getLogger(L2hDataSet.class);

    public static void makeWordVocabularyFile(int crossNumber, String dataSetName, Path testDirPath,
            Set<String> wordSet) throws IOException {
        Path dataSetPath = getDataSetPath(crossNumber, dataSetName, testDirPath);

        Path vocFilePath = getWordVocabularyFilePath(dataSetPath.toString(), dataSetName);
        File vocFile = new File(vocFilePath.toString());
        LOGGER.info("L2H vocabulary will be written to " + vocFile.getAbsolutePath());

        try (PrintWriter writer = new PrintWriter(
                new OutputStreamWriter(new FileOutputStream(vocFile), StandardCharsets.UTF_8))) {
            wordSet.forEach(word -> writer.println(word));
            LOGGER.info("Wrote word vocabulary to " + vocFilePath.toString());
        }
    }

    public static void makeTagVocabularyFile(int crossNumber, String dataSetName, Path testDirPath, Set<String> tagSet)
            throws IOException {
        Path dataSetPath = getDataSetPath(crossNumber, dataSetName, testDirPath);
        Path tagFilePath = getTagVocabularyFilePath(dataSetPath.toString(), dataSetName);
        File tagFile = new File(tagFilePath.toString());
        LOGGER.info("L2H Label Vocaublary will be written to " + tagFile.getAbsolutePath());

        try (PrintWriter writer = new PrintWriter(
                new OutputStreamWriter(new FileOutputStream(tagFile), StandardCharsets.UTF_8))) {
            tagSet.forEach(tag -> writer.println(tag));
            LOGGER.info("Wrote tag vocabulary to " + tagFilePath.toString());
        }
    }

    public static LinkedHashMap<String, Integer> initWordIdMapFromWordVocFile(int crossNumber, String dataSetName,
            Path testDirPath) throws IOException {
        Path dataSetPath = getDataSetPath(crossNumber, dataSetName, testDirPath);
        return initIdMapFromVocFile(crossNumber, dataSetName, testDirPath,
                () -> getWordVocabularyFilePath(dataSetPath.toString(), dataSetName));
    }
    
    public static void makeQuestionWordFreqFiles(int crossNumber, String dataSetName, Path testDirPath,
            LinkedHashMap<String, Integer> wordIdMap, List<Doc> trainDocList, List<Doc> testDocList) throws IOException {
        Path dataSetPath = getDataSetPath(crossNumber, dataSetName, testDirPath);

        // freqListSize wordId:Freq wordId:Freq ...

        Path datFilePath = getQuestionWordFreqFilePath(dataSetPath.toString(), dataSetName);
        LOGGER.info("L2H word-frequency (data) file will be written to " + datFilePath.toString());
        makeQuestionWordFreqFile(crossNumber, testDirPath, wordIdMap, trainDocList, datFilePath);
        

        datFilePath = getQuestionWordFreqTestFilePath(dataSetPath.toString(), dataSetName);
        LOGGER.info("L2H word-frequency (data) file will be written to " + datFilePath.toString());
        makeQuestionWordFreqFile(crossNumber, testDirPath, wordIdMap, testDocList, datFilePath);
    }

    
    public static void makeQuestionWordFreqFile(int crossNumber, Path testDirPath,
            LinkedHashMap<String, Integer> wordIdMap, List<Doc> docList, Path datFilePath) throws IOException {
        File datFile = new File(datFilePath.toString());
        LOGGER.info("L2H word-frequency (data) file will be written to " + datFile.getAbsolutePath());

        // freqListSize wordId:Freq wordId:Freq ...
        try (PrintWriter writer = new PrintWriter(
                new OutputStreamWriter(new FileOutputStream(datFile), StandardCharsets.UTF_8))) {
            for (Doc doc : docList) {
                TreeMap<Integer, Integer> idFreqMap = new TreeMap<Integer, Integer>();
                doc.forEachWord((word, freq) -> {
                    if (wordIdMap.containsKey(word)) {
                        idFreqMap.put(wordIdMap.get(word), doc.getWordFreq(word));
                    }
                });

                writer.print(idFreqMap.size());
                idFreqMap.forEach((id, freq) -> writer.print(" " + id + ":" + freq));
                writer.println();
            }
            LOGGER.info("Wrote document word-frequency data to " + datFilePath.toString());
        }
    }

    public static void makeQuestionTagFile(int crossNumber, String dataSetName, Path testDirPath, List<Doc> docList)
            throws FileNotFoundException, IOException {
        LinkedHashMap<String, Integer> tagIdMap = initTagIdMapFromTagVocFile(crossNumber, dataSetName, testDirPath);

        Path dataSetPath = getDataSetPath(crossNumber, dataSetName, testDirPath);
        Path docTagFilePath = getQuestionTagFilePath(dataSetPath.toString(), dataSetName);
        File docTagFile = new File(docTagFilePath.toString());
        LOGGER.info("L2H doc-tag file will be written to " + docTagFile.getAbsolutePath());

        // docId\tTagId1\tTagId2...
        try (PrintWriter writer = new PrintWriter(
                new OutputStreamWriter(new FileOutputStream(docTagFile), StandardCharsets.UTF_8))) {
            for (Doc doc : docList) {
                TreeSet<Integer> idSet = new TreeSet<Integer>();
                doc.forEachTag(tag -> idSet.add(tagIdMap.get(tag)));

                writer.print(doc.getId());
                idSet.forEach(id -> writer.print("\t" + id));
                writer.println();
            }
        }
    }

    public static Path getDataSetPath(int crossNumber, String dataSetName, Path testDirPath) throws IOException {
        Path dataSetPath = Paths.get(testDirPath.toString(), Integer.toString(crossNumber), dataSetName);
        if (!Files.exists(dataSetPath)) {
            Files.createDirectories(dataSetPath);
            LOGGER.info("created " + dataSetPath.toString());
        }
        return dataSetPath;
    }

    private static Path getWordVocabularyFilePath(String datasetFolder, String datasetName) {
        return Paths.get(datasetFolder, datasetName + ".wvoc");
    }

    private static Path getTagVocabularyFilePath(String datasetFolder, String datasetName) {
        return Paths.get(datasetFolder, datasetName + ".lvoc");
    }

    private static Path getQuestionWordFreqFilePath(String datasetFolder, String datasetName) {
        return Paths.get(datasetFolder, datasetName + ".dat");
    }
    
    private static Path getQuestionWordFreqTestFilePath(String datasetFolder, String datasetName) {
        return Paths.get(datasetFolder, datasetName + "_testetr.dat");
    }

    private static Path getQuestionTagFilePath(String datasetFolder, String datasetName) {
        return Paths.get(datasetFolder, datasetName + ".docinfo");
    }

    private interface VocFilePathComposer {
        Path get();
    }

    private static LinkedHashMap<String, Integer> initIdMapFromVocFile(int crossNumber, String dataSetName,
            Path testDirPath, VocFilePathComposer composer) throws IOException {
        Path vocFilePath = composer.get();
        if (!Files.exists(vocFilePath)) {
            throw new IllegalStateException("Expect " + vocFilePath.toString() + " to exist");
        }
        List<String> lines = Files.readAllLines(vocFilePath);
        LOGGER.info("Read vocabulary from " + vocFilePath.toString());
        LinkedHashMap<String, Integer> idMap = new LinkedHashMap<String, Integer>();
        int id = 0;
        for (String s : lines) {
            if (idMap.containsKey(s)) {
                throw new IllegalStateException("Expect string to be uniq in " + vocFilePath.toString());
            }
            idMap.put(s, id);
            id++;
        }

        return idMap;
    }

    private static LinkedHashMap<String, Integer> initTagIdMapFromTagVocFile(int crossNumber, String dataSetName,
            Path testDirPath) throws IOException {
        Path dataSetPath = getDataSetPath(crossNumber, dataSetName, testDirPath);
        return initIdMapFromVocFile(crossNumber, dataSetName, testDirPath,
                () -> getTagVocabularyFilePath(dataSetPath.toString(), dataSetName));
    }

}
