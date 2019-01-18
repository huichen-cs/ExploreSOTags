package sodata.processor.segan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;

public class LabeledTextDatasetDictionaries {
    private final static String wordVocabExt = ".wvoc";
    private final static String labelVocabExt = ".lvoc";

    private Charset charset;
    
    private String datasetFolder;

    private String datasetName;

    private ArrayList<String> labelList;
    private ArrayList<String> wordList;
    
    public LabeledTextDatasetDictionaries(String datasetFolder, String datasetName) throws FileNotFoundException, IOException {
        this(datasetFolder, datasetName, Charset.defaultCharset());
    }

    public LabeledTextDatasetDictionaries(String datasetFolder, String datasetName, Charset charset) throws FileNotFoundException, IOException {
        this.charset = Charset.defaultCharset();
        this.datasetFolder = datasetFolder;
        this.datasetName = datasetName;
        this.labelList = inputLabelVocab();
        this.wordList = inputWordVocab();
    }
    
    public ArrayList<String> getLabelList() {
        return labelList;
    }

    public ArrayList<String> getWordList() {
        return wordList;
    }

    private ArrayList<String> inputLabelVocab() throws FileNotFoundException, IOException {
        return inputLabelVocab(new File(datasetFolder, datasetName + labelVocabExt));
    }
    
    private ArrayList<String> inputLabelVocab(File file) throws FileNotFoundException, IOException {
        ArrayList<String> labelVocab = new ArrayList<String>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset))) {
            String line;
            while ((line = reader.readLine()) != null) {
                labelVocab.add(line);
            }
        }
        return labelVocab;
    }

    private ArrayList<String> inputWordVocab() throws FileNotFoundException, IOException {
        return inputWordVocab(new File(datasetFolder, datasetName + wordVocabExt));
    }

    private ArrayList<String> inputWordVocab(File file) throws FileNotFoundException, IOException {
        ArrayList<String> wordVocab = new ArrayList<String>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset))) {
            String line;
            while ((line = reader.readLine()) != null) {
                wordVocab.add(line);
            }
        }
        return wordVocab;
    }

}
