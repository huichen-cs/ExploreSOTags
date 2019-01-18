package sodata.l2hmodel.docmodel.simple;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class DocLabel {
    private int docId;
    private int[] tags;
    
    public DocLabel(int docId, int[] tags) {
        this.docId = docId;
        this.tags = tags;
    }
    
    public int getNumberOfTags() {
        return tags.length;
    }
    
    public int tagIdAt(int index) {
        return tags[index];
    }
    
    public int getDocId() {
        return docId;
    }
    
    public static DocLabel[] fromDocInfoFile(String docInfoFilename) throws IOException {
        int index = 0;
        List<String> lines = Files.readAllLines(Paths.get(docInfoFilename));
        DocLabel[] docLabels = new DocLabel[lines.size()];
        for (String line: lines) {
            String[] parts = line.split("\\s+");
            int docId = Integer.parseInt(parts[0]);
            
            int[] tags = new int[parts.length-1];
            for (int i=0; i<parts.length-1; i++) {
                tags[i] = Integer.parseInt(parts[i+1]);
            }
            
            DocLabel docLabel = new DocLabel(docId, tags);
            docLabels[index] = docLabel;
            index ++;
        }
        
        return docLabels;
    }

    public static DocLabel[] updateDocLabel(DocLabel[] targetDocLabels, LabelCollection targetLabelCollection,
            LabelCollection filteredLabelCollection) {
        DocLabel[] updatedDocLabels = new DocLabel[targetDocLabels.length];
        for (int i=0; i<updatedDocLabels.length; i++) {
            int id = targetDocLabels[i].getDocId();
            int[] tags = new int[targetDocLabels[i].tags.length];
            for (int j=0; j<tags.length; j++) {
                String label = targetLabelCollection.labelTextAt(targetDocLabels[i].tags[j]);
                int newTag = filteredLabelCollection.labelIdFor(label);
                tags[j] = newTag;
            }
            updatedDocLabels[i] = new DocLabel(id, tags);
        }
        return updatedDocLabels;
    }

}
