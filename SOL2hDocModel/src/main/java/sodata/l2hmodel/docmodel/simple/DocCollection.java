package sodata.l2hmodel.docmodel.simple;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class DocCollection {
    private int[] numWordsOfDocs;
    
    public DocCollection(int[] docLengthInWords) {
        numWordsOfDocs = docLengthInWords;
    }
    
    public int numOfWordsInDoc(int docIndex) {
        return numWordsOfDocs[docIndex];
    }
    
    public int numOfDocs() {
        return numWordsOfDocs.length;
    }
    
    public static DocCollection fromDocDataFile(String filename) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(filename));
        int[] numWordsOfDocs = new int[lines.size()];
        int i = 0;
        for (String line: lines) {
            String[] parts = line.split("\\s+");
            int nWords = Integer.parseInt(parts[0]);
            if (parts.length != nWords + 1) {
                throw new IllegalStateException("Unique words does not match word:word-count pairs.");
            }

            int sumNWords = 0;
            for (int j=1; j<parts.length; j++) {
                String[] wordParts = parts[j].split(":");
                sumNWords += Integer.parseInt(wordParts[1]);
            }
            
            numWordsOfDocs[i] = sumNWords;
            i ++;
        }
        
        return new DocCollection(numWordsOfDocs);
    }

    public static void updateDocTopics(DocTopic[] docTopics, DocCollection docCollection) {
        for (int i=0; i<docTopics.length; i++) {
            docTopics[i].setDocLength(docCollection.numOfWordsInDoc(i));
        }
    }

}
