package sodata.l2hmodel.model;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;

public class LabelCollection {
    private final static int NUM_FIELDS_IN_TREE_FILE = 3;
    private final static String ID_PATTERN = "\\d+";
    
    private Label[] labels;
    
    private LabelCollection(Label[] labels) {
        this.labels = labels;
    }
    
    public int getNumLabels() {
        return labels.length;
    }
    
    public static LabelCollection newInstanceFromTreeFile(Path treeFilePath) throws IOException, DataFormatException {
        List<String> lines = Files.readAllLines(treeFilePath, StandardCharsets.UTF_8);
        Pattern idPattern = Pattern.compile(ID_PATTERN);
        Label[] labels = new Label[lines.size()];
        for (String line: lines) {
            String[] parts = line.split("\\s+");
            if (parts.length != NUM_FIELDS_IN_TREE_FILE) {
                throw new DataFormatException("Tree file at path " + treeFilePath.toString() + " is in unexpected format: incorrect number of fields");
            }
            if (!idPattern.matcher(parts[1]).matches()) {
                throw new DataFormatException("Tree file at path " + treeFilePath.toString() + " is in unexpected format: id isn't an integer");
            }
            int id = Integer.parseInt(parts[1]);
            if (labels[id] != null) {
                throw new DataFormatException("Tree file at path " + treeFilePath.toString() + " is in unexpected format: duplicate id number");
            }
            labels[id] = new Label(id, parts[0], parts[2]);
        }
        LabelCollection labelCollection = new LabelCollection(labels);
        return labelCollection;
    }
}
