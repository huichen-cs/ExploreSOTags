/*
 * This program turns out to be a useless exercise. (at least based on the understanding as of
 * May 1, 2018, because it does not really compute what we need to assess the concept hierarchy
 * for the purpose of predicting tags)
 */
package sodata.l2hmodel.docmodel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class L2hDocModelTreeApp {
    private final static Logger LOGGER = LoggerFactory.getLogger(L2hDocModelTreeApp.class);
    
    public static void main(String[] args) throws FileNotFoundException {
        if (args.length < 3) {
            System.out.println("Usage: L2hDocModelTreeApp <tree_file> <doc_label_weight_list_file> <num_top_labels> [<doc_topic_proportion_fn>]");
            return;
        }
        

        File treeFile = new File(args[0]);

        if (!treeFile.exists()) {
            System.out.println(String.format(
                    "Tree file %s does not exist. " + "It is usually located at <output>/mst/tree.txt.", args[0]));
            System.exit(-1);
        }

        File labelWeightFile = new File(args[1]);
        if (!labelWeightFile.exists()) {
            System.out.println(String.format("Label weight file %s does not exists. "
                    + "It is sually located at <output>/<preset>/iter_predictions/iter-xxx.txt", args[1]));
            System.exit(-1);
        }
        
        int numTopLabels = Integer.parseInt(args[2]);

        Map<Integer, WeightedLabel> idLabelMap = loadLabelsFromFile(treeFile);

        List<DocLabels> docLabelList = new LinkedList<DocLabels>();
        try (Scanner weightScanner = new Scanner(labelWeightFile)) {
            int numDocs = weightScanner.nextInt();
            LOGGER.info("Number of documents: " + numDocs);

            for (int i = 0; i < numDocs; i++) {
                Map<Integer, WeightedLabel> idLabelMapCopy = copy(idLabelMap);
                int docId = weightScanner.nextInt();
                String line = weightScanner.nextLine();
                LOGGER.info("Read line: " + line);
                String[] parts = line.trim().split("\\s+");
                for(int id=0; id<parts.length; id++) {
                    //System.out.println(parts[id]);
                    double weight = Double.parseDouble(parts[id]);
                    idLabelMapCopy.get(id).setLabelWeight(weight);
                }
                LOGGER.info("processed doc: " + docId);
                docLabelList.add(new DocLabels(docId, idLabelMapCopy));
            }
        }
        
        docLabelList.forEach(d -> {
            LOGGER.info("DOC: docId: " + d.toSummaryString());
        });

        docLabelList.forEach(d -> {
            LOGGER.info(">>>>>>>>>>>>>>>>> DOC: " + d.getDocId());
            ArrayList<WeightedLabel> list = d.sortedLabelsByWeight();
            int i = 0;
            for(WeightedLabel l: list) {
                if (i>=numTopLabels) break;
                LOGGER.info(l.toString());
                i ++;
            }
        });
        
        LOGGER.debug("args.length = " + args.length);
        if (args.length >= 4) {
            LOGGER.info("Writing out to " + args[3]);
            try (PrintWriter writer = new PrintWriter(
                    new OutputStreamWriter(new FileOutputStream(args[3]), StandardCharsets.UTF_8))) {
                docLabelList.forEach(d -> {
                    ArrayList<WeightedLabel> list = d.sortedLabelsByWeight();
                    int i = 0;
                    writer.print(Integer.toString(i));
                    for(WeightedLabel l: list) {
                        if (i>=numTopLabels) break;
                        writer.print("\t" + l.toString());
                        i ++;
                    }
                    writer.println();
                    i ++;
                });
            }
        }

    }

    private static Map<Integer, WeightedLabel> loadLabelsFromFile(File labelFile) throws FileNotFoundException {
        Map<Integer, WeightedLabel> idLabelMap = new HashMap<Integer, WeightedLabel>();

        try (Scanner scanner = new Scanner(labelFile)) {

            String path = null;
            int id = -1;
            String name = null;
            WeightedLabel label;

            while (scanner.hasNext()) {
                if (scanner.hasNext()) {
                    path = scanner.next();
                }
                if (scanner.hasNextInt()) {
                    id = scanner.nextInt();
                }
                if (scanner.hasNext()) {
                    name = scanner.next();
                }
                label = new WeightedLabel(id, path, name);
                idLabelMap.put(id,  label);
                LOGGER.debug("read label: " + label.toString());
            }
        }
        return idLabelMap;
    }
    
    private static Map<Integer, WeightedLabel> copy(Map<Integer, WeightedLabel> source) {
        Map<Integer, WeightedLabel> copy = new HashMap<Integer, WeightedLabel>();
        source.forEach((k, v) -> copy.put(k, v.getCopy()));
        return copy;
    }
}

class WeightedLabel {
//    private final static Logger LOGGER = LoggerFactory.getLogger(WeightedLabel.class);
    
    private String labelPath;
    private String labelName;
    private int labelId;
    private double labelWeight;

    public WeightedLabel(int id, String path, String name) {
        labelId = id;
        labelPath = path;
        labelName = name;
        labelWeight = Double.NEGATIVE_INFINITY;
    }
    
    public WeightedLabel(int id, String path, String name, double weight) {
        labelId = id;
        labelPath = path;
        labelName = name;
        labelWeight = weight;
    }    

    public String getLabelPath() {
        return labelPath;
    }

    public void setLabelPath(String labelPath) {
        this.labelPath = labelPath;
    }

    public String getLabelName() {
        return labelName;
    }

    public void setLabelName(String labelName) {
        this.labelName = labelName;
    }

    public int getLabelId() {
        return labelId;
    }

    public void setLabelId(int labelId) {
        this.labelId = labelId;
    }

    public double getLabelWeight() {
        return labelWeight;
    }

    public void setLabelWeight(double labelWeight) {
        this.labelWeight = labelWeight;
    }
    
    public WeightedLabel getCopy() {
        return new WeightedLabel(labelId, labelPath, labelName, labelWeight);
    }
    
    public String toString() {
        return "{id=" + labelId 
                + ", name=" + labelName
                + ", weight=" + labelWeight
                + "}";
    }
    
    private final static Pattern LABEL_PATTERN = Pattern.compile("\\{id=.*, name=.*, weight=.*\\}");
    
    public static WeightedLabel fromString(String labelString) {
        Matcher m = LABEL_PATTERN.matcher(labelString);
        if (!m.matches()) {
            throw new IllegalArgumentException("Unexpected format in the label string: " + labelString);
        }
        String[] fields = labelString.split(", ");
        if (fields.length != 3) {
            throw new RuntimeException("The label string has two fewer fields: " + labelString);
        }

        String[] attribute = fields[0].split("=");
        int id = Integer.parseInt(attribute[1]);

        attribute = fields[1].split("=");
        String name = attribute[1];
        
        attribute = fields[2].split("=");
        double weight = Double.parseDouble(attribute[1].substring(0,  attribute[1].length()-1));

        WeightedLabel weightedLabel = new WeightedLabel(id, null, name, weight);
        // LOGGER.debug("Parsed label: " + weightedLabel.toString());

        return weightedLabel;
    }
    
    @Override
    public boolean equals(Object theOther) {
        if (!(theOther instanceof WeightedLabel)) return false;
//        LOGGER.debug(this.toString() + "<-->" + ((WeightedLabel)theOther).toString());
        WeightedLabel otherLabel = (WeightedLabel)theOther;
        return ((labelName == otherLabel.labelName)
                || (labelName != null && otherLabel.labelName != null && labelName.equals(otherLabel.labelName)));
//        return ((labelPath == otherLabel.labelPath)
//                || (labelPath != null && otherLabel.labelPath != null && labelPath.equals(otherLabel.labelPath)))
//            && ((labelName == otherLabel.labelName)
//                || (labelName != null && otherLabel.labelName != null && labelName.equals(otherLabel.labelName)))
//            && (labelId == otherLabel.labelId);
    }
    
    @Override
    public int hashCode() {
        // LOGGER.debug("Label: " + labelName + " hash code: " + labelName.hashCode());
        return labelName.hashCode();
    }
}

class DocLabels {
    private Map<Integer, WeightedLabel> idLabelMap;
    private int docId;
    
    public DocLabels(int docId, Map<Integer, WeightedLabel> idLabelMap) {
        this.docId = docId;
        this.idLabelMap = idLabelMap;
    }

    public String toSummaryString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ docId= " + docId + ",");
        int maxLabel = 5;
        int i = 0;
        for (Map.Entry<Integer, WeightedLabel> entry : idLabelMap.entrySet()) {
            if (i >= maxLabel) break;
            int labelId = entry.getKey();
            WeightedLabel label = entry.getValue();
            sb.append(" { labelId= " + labelId + ", label=" + label.toString() + " }");
            i ++;
        }
        sb.append(" }");
        return sb.toString();
    }

    public Map<Integer, WeightedLabel> getIdLabelMap() {
        return idLabelMap;
    }

    public void setIdLabelMap(Map<Integer, WeightedLabel> idLabelMap) {
        this.idLabelMap = idLabelMap;
    }

    public int getDocId() {
        return docId;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }
    
    public ArrayList<WeightedLabel> sortedLabelsByWeight() {
        ArrayList<WeightedLabel> list = new ArrayList<WeightedLabel>();
        idLabelMap.forEach((k, v) -> list.add(v));
        list.sort((lhs, rhs) -> {
            if (lhs.getLabelWeight() > rhs.getLabelWeight())
                return -1;
            else if(lhs.getLabelWeight() < rhs.getLabelWeight())
                return 1;
            else
                return 0;
            });
        return list;
    }
}