package sodata.l2hmodel.docmodel.simple;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LabelCollection {
    private String[] labels;
    private Map<String, Integer> labelIdMap;
    
    public LabelCollection(String[] labels) {
        this.labels = labels;
        this.labelIdMap = new HashMap<String, Integer>();
        for (int i=0; i<labels.length; i++) {
            labelIdMap.put(labels[i], i);
        }
    }

    public String labelTextAt(int index) {
        return labels[index];
    }
    
    
    public static LabelCollection fromLabelVocFile(String labelVocFilename) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(labelVocFilename));
        String[] labels = new String[lines.size()];
        labels = lines.toArray(labels);
        return new LabelCollection(labels);
    }

    public int labelIdFor(String label) {
        return labelIdMap.get(label);
    }
}
