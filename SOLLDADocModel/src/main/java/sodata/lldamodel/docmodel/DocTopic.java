package sodata.lldamodel.docmodel;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class DocTopic {
    private int index;
    private double[] proportions;
    private int[] rank;
    private Topic[] topics;
    private int docLengthInWords;
    
    public DocTopic(int index, double[] proportions) {
        this.index = index;
        this.proportions = proportions;

        topics = Topic.getTopics(proportions);
        Arrays.sort(topics, (lhs, rhs) -> - lhs.proportion.compareTo(rhs.proportion));
        
        rank = new int[proportions.length];
        for (int i=0; i<topics.length; i++) {
            rank[topics[i].topicId] = i;
        }
        docLengthInWords = -1;
    }
    
    public double proporitionsOf(int topicId) {
        return proportions[topicId];
    }
    
    public boolean hasProportionAt(int topicId) {
        return hasSignificantProportionAt(topicId, 0.);
    }
    
    public boolean hasSignificantProportionAt(int topicId, double threshold) {
        return Double.isFinite(proportions[topicId]) && proportions[topicId] > threshold;
    }
    
    public int getDocLengthInWords() {
        return this.docLengthInWords;
    }
    
    public int getNumberOfImportantTopics(double threshold) {
        int n = 0;

        for (int i=0; i<proportions.length; i++) {
            if (hasSignificantProportionAt(i, threshold)) n ++;
        }
        
        return n;
    }
    
    public int getIndex() {
        return index;
    }
    
    
    public int getRank(int topicId) {
        return rank[topicId];
    }
    
    public String toRankedString() {
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<rank.length; i++) {
            sb.append("(").append(rank[i]).append(",").append(i).append(",").append(proportions[i]).append("),");
        }
        return sb.toString().substring(0, sb.toString().length() - 1);
    }
    
    public String toSortedString() {
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<topics.length; i++) {
            sb.append("(").append(topics[i].topicId).append(",").append(topics[i].proportion).append("),");
        }
        return sb.toString().substring(0, sb.toString().length() - 1);
    }
    

    public void setDocLength(int numOfWordsInDoc) {
        docLengthInWords = numOfWordsInDoc;
    }
    
    public static DocTopic[] fromIterReport(String iterReportFilename) throws IOException {
        DocTopic[] docTopics = null;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(iterReportFilename), StandardCharsets.UTF_8)) {
            int rowNumber = 0;
            int nTopics = -1;
            
            int nDocs = Integer.parseInt(reader.readLine());
            docTopics = new DocTopic[nDocs];
            
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+");
                int index = Integer.parseInt(parts[0]);
                if ((index != rowNumber) || (nTopics >= 0 && nTopics != parts.length - 1)) {
                    throw new IllegalStateException(String.format("File is in an unexpected format at line %d: (index, rowNumber, nTopics, numParts) = (%d, %d, %d, %d)" 
                            , rowNumber + 1, index, rowNumber, nTopics, parts.length));
                }

                double[] proportions = new double[parts.length - 1];
                for (int i=0; i<parts.length-1; i++) {
                    proportions[i] = Double.parseDouble(parts[1+i]);
                }
                docTopics[index] = new DocTopic(index, proportions);
                rowNumber ++;
                nTopics = proportions.length;
            }
        }
        return docTopics;
    }

}

class Topic {
    int topicId;
    Double proportion;
    
    public Topic(int topicId, double proportion) {
        this.topicId = topicId;
        this.proportion = proportion;
    }
    
    static Topic[] getTopics(double[] proportions) {
        Topic[] pairs = new Topic[proportions.length];
        for (int i=0; i<proportions.length; i++) {
            pairs[i] = new Topic(i, proportions[i]);
        }
        return pairs;
    }
}
