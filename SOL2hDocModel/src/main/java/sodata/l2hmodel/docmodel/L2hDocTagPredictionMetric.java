package sodata.l2hmodel.docmodel;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Based on two data set: (tag 1 in unseen should be 1 in training)
 *
 */
public class L2hDocTagPredictionMetric {
    private final static Logger LOGGER = LoggerFactory.getLogger(L2hDocTagPredictionMetric.class);

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: L2hDocTagPredictionMetric <predicted_tag_file> <input_doc_label_file> <input_label_file> <input_doc_word_file>");
            System.err.println("Example: L2hDocTagPredictionMetric \\\n" 
                    + "\t tagpredict2/so2016p5000w300_out/PRESET_L2H_K-369_B-250_M-500_L-25_opt-false_MAXIMAL-10-1000-90-10-mst-false-false/iter-predictions/iter-500-topic-pp.txt \\\n"
                    + "\t tagpredict2/so2016p5000w300unseen/so2016p5000w300docs.docinfo \\\n"
                    + "\t tagpredict2/so2016p5000w300unseen/so2016p5000w300docs.lvoc \\\n"
                    + "\t tagpredict2/so2016p5000w300unseen/so2016p5000w300docs.dat");
            System.exit(1);
        }
        L2hDocTagPredictionMetric metric = new L2hDocTagPredictionMetric();
        try {
            ArrayList<LabeledDocument> predictedLabeledDocList = metric.loadPredictedTagsFile(args[0], StandardCharsets.UTF_8);
            ArrayList<LabeledDocument> inputLabelDocList = metric.loadInputTagsFile(args[1], args[2], StandardCharsets.UTF_8);
            ArrayList<Integer> docLengthList = metric.loadDocLength(args[3], StandardCharsets.UTF_8);
            if (metric.compareDocList(predictedLabeledDocList, inputLabelDocList)) {
                LOGGER.info("Document lists are identical.");
            }
            for (int minDocLength = 50; minDocLength <= 300; minDocLength += 50) {
                int numDocs = 0;
                for (int len : docLengthList) {
                    if (len >= minDocLength)
                        numDocs++;
                }
                for (int i = 1; i <= 20; i++) {
                    Metrics metrics = metric.computePredictionAccuracy(predictedLabeledDocList, inputLabelDocList, i,
                            docLengthList, minDocLength);
                    System.out.println("numTopTags: " + i + "\t" + metrics.tagAccurarcy + "\t" + metrics.docAccurarcy
                            + "\t" + minDocLength + "\t" + numDocs);
                }
            }
        } catch (FileNotFoundException e) {
            LOGGER.error("Failed to open file: " + e.getMessage(), e);
            System.exit(1);
        } catch (IOException e) {
            LOGGER.error("Failed to read file: " + e.getMessage(), e);
            System.exit(1);
        } 
    }
    

    private class Metrics {
        public double tagAccurarcy;
        public double docAccurarcy;
    }
    
    private Metrics computePredictionAccuracy(ArrayList<LabeledDocument> predictedLabeledDocList,
            ArrayList<LabeledDocument> inputLabelDocList, int topNumTags, ArrayList<Integer> docLengthList,
            int minDocLength) {
        if (predictedLabeledDocList.size() != inputLabelDocList.size()) {
            throw new RuntimeException("List sizes are not equal.");
        }
        
        Iterator<LabeledDocument> predictedLabeledDocListIter = predictedLabeledDocList.iterator();
        Iterator<LabeledDocument> inputLabelDocListIter = inputLabelDocList.iterator();
        Iterator<Integer> docLengthIter = docLengthList.iterator();
        long matchingTags = 0;
        long docsWithMatchingTags = 0;
        long expectedMaxMatchingTags = 0;
        long expectedMaxDocsWithMatchingTags = 0;
        while (predictedLabeledDocListIter.hasNext() && inputLabelDocListIter.hasNext() && docLengthIter.hasNext()) {
            LabeledDocument predictedLabeledDoc = predictedLabeledDocListIter.next();
            LabeledDocument inputLabelDoc = inputLabelDocListIter.next();
            int docLength = docLengthIter.next();

            if (predictedLabeledDoc.getAltDocId() != inputLabelDoc.getAltDocId()) {
                throw new RuntimeException("predictedLabeledDocListIter.next().getAltDocId(): "
                        + predictedLabeledDocListIter.next().getAltDocId()
                        + " != inputLabelDocListIter.next().getAltDocId(): "
                        + inputLabelDocListIter.next().getAltDocId());
            }

            if (docLength < minDocLength) continue;

            Set<WeightedLabel> predictedWeightedLabelSet = predictedLabeledDoc.getTopWeightedLabelSet(topNumTags);
            Set<WeightedLabel> inputWeightedLabelSet = inputLabelDoc.getWeightedLabelSet();
            
//            LOGGER.debug(">>>" + predictedLabeledDoc.getAltDocId() + " <-> " + inputLabelDoc.getAltDocId());
//            predictedWeightedLabelSet.forEach(label -> {
//                LOGGER.debug(">>>\t" + label.toString());
//            });
//            LOGGER.debug(">>>\t-------");
//            inputWeightedLabelSet.forEach(label -> {
//                LOGGER.debug(">>>\t" + label.toString());
//            });
            
            boolean foundMatch = false;
            for(WeightedLabel label: inputWeightedLabelSet) {
                if (predictedWeightedLabelSet.contains(label)) {
                    matchingTags ++;
                    foundMatch = true;
                    // LOGGER.debug(">>>\tfound a match");
                }
            }
            if (foundMatch) docsWithMatchingTags ++;
            expectedMaxMatchingTags += inputWeightedLabelSet.size();
            expectedMaxDocsWithMatchingTags ++;
        }
        Metrics metrics = new Metrics();
        metrics.tagAccurarcy = (double)matchingTags/(double)expectedMaxMatchingTags;
        metrics.docAccurarcy = (double)docsWithMatchingTags/(double)expectedMaxDocsWithMatchingTags;
//        LOGGER.debug("Number of documents processed: " + expectedMaxDocsWithMatchingTags);
        return metrics;
    }
    
    private boolean compareDocList(ArrayList<LabeledDocument> predictedLabeledDocList, ArrayList<LabeledDocument> inputLabelDocList) {
        if (predictedLabeledDocList.size() != inputLabelDocList.size()) {
            LOGGER.info("List sizes are not equal.");
            return false;
        }
        
        Iterator<LabeledDocument> predictedLabeledDocListIter = predictedLabeledDocList.iterator();
        Iterator<LabeledDocument> inputLabelDocListIter = inputLabelDocList.iterator();
        while (predictedLabeledDocListIter.hasNext() && inputLabelDocListIter.hasNext()) {
            if (predictedLabeledDocListIter.next().getAltDocId() != inputLabelDocListIter.next().getAltDocId()) {
                LOGGER.debug("predictedLabeledDocListIter.next().getAltDocId(): "
                        + predictedLabeledDocListIter.next().getAltDocId()
                        + " != inputLabelDocListIter.next().getAltDocId(): "
                        + inputLabelDocListIter.next().getAltDocId());
                return false;
            }
        }
        return true;
    }
    
    private ArrayList<LabeledDocument> loadInputTagsFile(String docLabelFilename, String labelFilename, Charset charset)
            throws FileNotFoundException, IOException {
        Map<Integer, WeightedLabel> weightedLabelMap = new HashMap<Integer, WeightedLabel>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(labelFilename), charset))) {
            String labelName;
            int labelId = 0;
            while ((labelName = reader.readLine()) != null) {
                WeightedLabel label = new WeightedLabel(labelId, null, labelName, 0.0);
                weightedLabelMap.put(labelId, label);
                labelId ++;
            }
            LOGGER.debug("read " + labelId + " labels.");
        }

        ArrayList<LabeledDocument> docList = new ArrayList<LabeledDocument>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(docLabelFilename), charset))) {

            long altDocId = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\t");
                long docId = Long.parseLong(fields[0]);
                Set<WeightedLabel> docLabelSet = new HashSet<WeightedLabel>();
                for (int i=1; i<fields.length; i++) {
                    int id = Integer.parseInt(fields[i]);
                    WeightedLabel label = weightedLabelMap.get(id);
                    if (label == null) {
                        LOGGER.error("Failed to retrieve label with id " + id);
                        throw new RuntimeException("Failed to retrieve label with id " + id);
                    }
                    docLabelSet.add(label);
                }
                LabeledDocument document = new LabeledDocument(docId, altDocId, docLabelSet);
                docList.add(document);
                altDocId ++;
            }
        }

        return docList;
    }


    private ArrayList<LabeledDocument> loadPredictedTagsFile(String predictedTagFilename, Charset charset) throws IOException, FileNotFoundException {
        ArrayList<LabeledDocument> docList = new ArrayList<LabeledDocument>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(predictedTagFilename), charset))) {
            String line = null;
            long altDocId = 0;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\t");
                if (fields.length < 2) {
                    throw new RuntimeException("Current line has fewer than 2 fields: " + line);
                }
                long docId = Long.parseLong(fields[0]);
                Set<WeightedLabel> docLabelSet = new HashSet<WeightedLabel>();
                for (int i=1; i<fields.length; i++) {
                    WeightedLabel label = WeightedLabel.fromString(fields[i]);
                    docLabelSet.add(label);
                }
                LabeledDocument document = new LabeledDocument(docId, altDocId, docLabelSet);
                docList.add(document);
                altDocId ++;
            }
        }
        return docList;
    }
    
    private ArrayList<Integer> loadDocLength(String docDataFilename, Charset charset) throws FileNotFoundException, IOException {
        ArrayList<Integer> docLengthList = new ArrayList<Integer>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(docDataFilename), charset))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(" ");
                int docLength = Integer.parseInt(fields[0]);
                docLengthList.add(docLength);
            }
        }
        return docLengthList;
    }

}

class LabeledDocument {
//    private final static Logger LOGGER = LoggerFactory.getLogger(LabeledDocument.class);
    private long docId;
    private long altDocId;
    private Set<WeightedLabel> weightedLabelSet;
    
    LabeledDocument(long docId, long altDocId, Set<WeightedLabel> weightedLabelSet) {
        this.docId = docId;
        this.altDocId = altDocId;
        this.weightedLabelSet = weightedLabelSet;
    }
    
    public long getDocId() {
        return docId;
    }
    
    public long getAltDocId() {
        return altDocId;
    }

    public Set<WeightedLabel> getWeightedLabelSet() {
        return weightedLabelSet;
    }

    public Set<WeightedLabel> getTopWeightedLabelSet(int topNum) {
        WeightedLabel[] weightedLabels = weightedLabelSet.toArray(new WeightedLabel[weightedLabelSet.size()]);
        Arrays.sort(weightedLabels, new Comparator<WeightedLabel>() {
            @Override
            public int compare(WeightedLabel lhs, WeightedLabel rhs) {
                if (lhs.getLabelWeight() > rhs.getLabelWeight())
                    return -1;
                else if (lhs.getLabelWeight() == rhs.getLabelWeight())
                    return 0;
                else
                    return 1;
            }
        });
        
//        LOGGER.debug(">>>" + altDocId);
//        for (WeightedLabel label: weightedLabels) {
//            LOGGER.debug(">>>\t" + label.toString());
//        }
        

        
        Set<WeightedLabel> selectedLabelSet = new HashSet<WeightedLabel>(
                Arrays.asList(Arrays.copyOfRange(weightedLabels, 0, Math.min(topNum, weightedLabels.length))));
//        LOGGER.debug(">>>" + altDocId);
//        selectedLabelSet.forEach(label -> {
//            LOGGER.debug(">>>\t" + label.toString());
//        });
//        
//        System.exit(1);
        
        return selectedLabelSet;
    }
}
