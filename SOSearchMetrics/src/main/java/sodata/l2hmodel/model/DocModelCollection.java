package sodata.l2hmodel.model;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.DataFormatException;

import sodata.l2hmodel.entropy.Math;
import sodata.l2hmodel.tree.L2hTree;
import sodata.l2hmodel.tree.L2hTreeNode;

public class DocModelCollection {
    private final static String NUM_PATTERN = "\\d+";
    // https://docs.oracle.com/javase/8/docs/api/java/lang/Double.html#valueOf-java.lang.String-
    private final static String DIGITS     = "(\\p{Digit}+)";
    private final static String HEX_DIGITS = "(\\p{XDigit}+)";
    private final static String EXP        = "[eE][+-]?"+DIGITS;
    private final static String FP_REGEX   =
        ("[\\x00-\\x20]*"+ 
         "[+-]?(" + 
         "NaN|" + 
         "Infinity|" + 
         "((("+DIGITS+"(\\.)?("+DIGITS+"?)("+EXP+")?)|"+
         "(\\.("+DIGITS+")("+EXP+")?)|"+
         "((" +
          "(0[xX]" + HEX_DIGITS + "(\\.)?)|" +
          "(0[xX]" + HEX_DIGITS + "?(\\.)" + HEX_DIGITS + ")" +
          ")[pP][+-]?" + DIGITS + "))" +
         "[fFdD]?))" +
         "[\\x00-\\x20]*");

    private DocModel[] docModels;
    private int numTopics;
    private double[] meanProportions;
    private double[] conceptSpecificities;
    private double[] docDivergences;    // need mean proportions & a collection of documents
//    private double[] conceptDivergences;// need a collection of documents, weighted average based on selected concept 
    
    private DocModelCollection(int numTopics, DocModel[] docModels) {
        this.docModels = docModels;
        this.numTopics = numTopics;
        meanProportions = computeMeanTopicProportions(numTopics, docModels);
        conceptSpecificities = computeConceptSpecificities(numTopics, meanProportions, docModels);
        docDivergences= computeDocumentDivergence(numTopics, meanProportions, docModels);
//        conceptDivergences = computeConeptDivergence(numTopics, docModels.length, docDivergences, docModels);
    }
    
    public static DocModelCollection newInstanceFromDocModelFile(int numTopics, Path docModelFilePath) throws IOException, DataFormatException {
        List<String> lines = Files.readAllLines(docModelFilePath);

        DocModel[] docModels = new DocModel[lines.size()-1];

        Pattern numPattern = Pattern.compile(NUM_PATTERN);
        Pattern fpPattern = Pattern.compile(FP_REGEX);
        
        int numDocs = -1;
        int numStoredTopics = -1;
        int lineNo = 0;
        for (String line: lines) {
            if (lineNo == 0) {
                if (!numPattern.matcher(line).matches()) {
                    throw new DataFormatException("Number of documents not an integer: " + line);
                }
                numDocs = Integer.parseInt(line);
            } else {
                String[] parts = line.split("\\s+");
                if (!numPattern.matcher(parts[0]).matches()) {
                    throw new DataFormatException("Document id not an integer");
                }
                int docId = Integer.parseInt(parts[0]);
                if (docModels[docId] != null) {
                    throw new DataFormatException("Encountered duplicative document id");
                }
                if (docId + 1 != lineNo) {
                    throw new DataFormatException("Expected line number to be 1+document id");
                }
                
                if (numStoredTopics >= 0 && numStoredTopics != parts.length - 1) {
                    throw new DataFormatException("Encounted a different number topics in a document model");
                }
                numStoredTopics = parts.length - 1;
                
                if (numTopics != numStoredTopics && numTopics != numStoredTopics + 1) {
                    throw new DataFormatException("Number of topics and number of weights do not match.");
                }
                
                double weight[] = new double[numTopics];
                double rootWeight = 1.0;
                for (int topicId = 0; topicId < parts.length-1; topicId++) {
                    if (!fpPattern.matcher(parts[topicId+1]).matches()) {
                        throw new DataFormatException("Topic proportion should be a double value");
                    } else {
                        weight[topicId] = Double.parseDouble(parts[topicId+1]);
                        if (Double.isFinite(weight[topicId])) {
                            rootWeight -= weight[topicId];
                        }
                    }
                }
                if (numTopics == numStoredTopics + 1) {
                    weight[numTopics-1] = rootWeight;
                }

                docModels[docId] = new DocModel(docId, weight);
            }
            lineNo ++;
        }
        if (numDocs != lineNo - 1) {
            throw new DataFormatException("Line numbers should be 1+number of documents");
        }
        
        return new DocModelCollection(numTopics, docModels);
    }

    public double[] computeCrossDivergence(DocModelCollection anchorCollection) {
        return computeDocumentDivergence(numTopics, anchorCollection.meanProportions, this.docModels);
    }

    public double conceptSpecificityOf(int topicId) {
        return conceptSpecificities[topicId];
    }

    public double diversityOf(int docId) {
        return docModels[docId].getDiversity();
    }
    
//    public double conceptDivergenceOf(int topicId) {
//        return conceptDivergences[topicId];
//    }
    
    public double docDivergenceOf(int docId) {
        return docDivergences[docId];
    }
    

    public List<DocModelTree> getDocTreesAsList(L2hTree globalConceptTree) {
        checkDocTreeLevels(globalConceptTree);
        checkDocTreeBranches(globalConceptTree);
        List<DocModelTree> treeModels = new LinkedList<DocModelTree>();
        
        int numDocsProcessed = 0;
        for (DocModel docModel: docModels) {
            L2hTree tree = new L2hTree(globalConceptTree);
            double[] proportions = IntStream.range(0,  numTopics).mapToDouble(topicId -> docModel.proportionOf(topicId)).toArray();
            tree.prune(proportions);
            treeModels.add(new DocModelTree(docModel, tree));
            numDocsProcessed ++;
            System.out.println("num docs processed: " + numDocsProcessed + ": " + (double)numDocsProcessed/(double)docModels.length);
            if (numDocsProcessed > 10000) break;
        }
        return treeModels;
    }
    
    public List<DocModelTree> getDocTreesAsList(L2hTree globalConceptTree, int[] docNos) {
        List<DocModelTree> treeModels = new LinkedList<DocModelTree>();
        
        for (int i: docNos) {
            DocModel docModel = docModels[i];
            L2hTree tree = new L2hTree(globalConceptTree);
            double[] proportions = IntStream.range(0,  numTopics).mapToDouble(topicId -> docModel.proportionOf(topicId)).toArray();
            tree.prune(proportions);
            treeModels.add(new DocModelTree(docModel, tree));
        }
        return treeModels;
    }

    public int getNumDocuments() {
        return docModels.length;
    }

    public int getNumTopics() {
        return numTopics;
    }
    
    public DocModelCollection getSubcollection(int topicId) {
        DocModel[] subcollection = Stream.of(docModels)
                .filter(d -> d.proportionOf(topicId) > 0.)
                .toArray(size -> new DocModel[size]);
        return new DocModelCollection(getNumTopics(), subcollection);
    }
    
    public void setDocInfoPropertiesFromDocInfoFile(Path docInfoFilePath) throws IOException {
        if (null == docModels) {
            throw new IllegalStateException("Doc Models is null.");
        }
        List<String> lines = Files.readAllLines(docInfoFilePath);
        int i = 0;
        for (String line: lines) {
            String[] parts = line.split("\\s+");
            long docInfoId =  Long.parseLong(parts[0]);
            int[] docInfoTagIds = new int[parts.length - 1];
            for (int j=1; j<parts.length; j++) {
                int tagId = Integer.parseInt(parts[j]);
                docInfoTagIds[j-1] = tagId;
            }
            
            if (i != docModels[i].getId()) {
                throw new IllegalStateException("Doc Info File line number and Doc Modelid do not match.");
            }
            
            docModels[i].setDocInfoId(docInfoId);
            docModels[i].setDocInfoTagIds(docInfoTagIds);
            i ++;
        }
    }
    
    public void setDocWordStatisticsFromDocDataFile(Path docModelDocDataFilePath) throws IOException {
        if (null == docModels) {
            throw new IllegalStateException("Doc Models is null.");
        }
        List<String> lines = Files.readAllLines(docModelDocDataFilePath);
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
            
            docModels[i].setNumWords(sumNWords);
            i ++;
        }
    }


    
    private void checkDocTreeLevels(L2hTree globalConceptTree) {
        final Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        globalConceptTree.bfs(node -> map.put(node.getTopicId(), node.getLevel()));
        int count = 0;
        for (DocModel docModel: docModels) {
            int[] levels = IntStream.range(0, numTopics)
                .filter(topicId -> Double.isFinite(docModel.proportionOf(topicId)) && docModel.proportionOf(topicId) > 0)
                .map(topicId-> map.get(topicId)).toArray();
            int min = Arrays.stream(levels).min().getAsInt();
            int max = Arrays.stream(levels).max().getAsInt();
            // System.out.println(min + "," + max);
            if (min != 0) {
                System.out.println(docModel.getId() + "," + min + "," +  max);
                count ++;
            }
        }
        System.out.println("percent of doc trees not starting at root: " + (double)count/(double)docModels.length);
    }
    
    private void checkDocTreeBranches(L2hTree globalConceptTree) {
        L2hTree tree = new L2hTree(globalConceptTree);
        int count = 0;
        for (DocModel docModel: docModels) {
            double[] proportions = IntStream.range(0,  numTopics).mapToDouble(topicId -> docModel.proportionOf(topicId)).toArray();
            final List<L2hTreeNode> leafNodes = new LinkedList<L2hTreeNode>();
            tree.dfs(node -> {if (node.getChildren() == null || node.getChildren().isEmpty()) leafNodes.add(node);});
            for (L2hTreeNode node: leafNodes) {
                List<L2hTreeNode> pathFromRoot = node.getPathFromRoot();
                boolean down = false;
                boolean up = false;
                double[] newProportions = pathFromRoot.stream().mapToDouble(n -> proportions[n.getTopicId()]).toArray();
                if (newProportions.length != pathFromRoot.size()) {
                    throw new IllegalStateException("algorithm is wrong.");
                }
                for (int i=1; i<newProportions.length; i++) {
                    if (newProportions[i-1] > 0 && newProportions[i] == 0) down = true;
                    if (down && newProportions[i-1] == 0 && newProportions[i] > 0) up = true;
                }
                if (down && up) {
                    count ++;
                    System.out.println(docModel.getId() + ": found v shape");
                }
            }
        }
        System.out.println("Found v shapes: " + count);
    }

    private double[] computeMeanTopicProportions(int numTopics, DocModel[] docModels) {
        double[] meanProportions = new double[numTopics];
        Arrays.fill(meanProportions, 0);

        // TODO: replace it by Arrays.setAll(T[] t, ...)
        for (DocModel docModel : docModels) {
            for (int topicId = 0; topicId < numTopics; topicId++) {
                double proportion = docModel.proportionOf(topicId);
                if (proportion > 0. && !Double.isInfinite(proportion)) {
                    meanProportions[topicId] += proportion;
                }
            }
        }
        Arrays.setAll(meanProportions, index -> meanProportions[index] / docModels.length);

        return meanProportions;
    }
    
    private double[] computeConceptSpecificities(int numTopics, double[] meanProportions, DocModel[] docModels) {
        double[] specificity = new double[numTopics];
        Arrays.fill(specificity, 0);
        
        // TODO: use Arrays.setAll() for performance improvement
        for (DocModel docModel: docModels) {
            for (int topicId = 0; topicId < numTopics; topicId ++) {
                double proportion = docModel.proportionOf(topicId);
                double meanProportion = meanProportions[topicId];
                if (proportion > 0. && !Double.isInfinite(proportion)) {
                    double ratio = proportion / meanProportion;
                    double product = ratio * Math.log2(ratio);
                    specificity[topicId] += product;
                }
            }
        }
        
        Arrays.setAll(specificity, index -> specificity[index] / docModels.length);
        
        return specificity;
    }
    
    private double[] computeDocumentDivergence(int numTopics, double[] meanProportions, DocModel[] docModels) {
        double[] docDivergences = new double[docModels.length];
        for (int i=0; i<docModels.length; i++) {
            docDivergences[i] = docModels[i].computeDivergence(numTopics, meanProportions);
        }
        return docDivergences;
    }


//    private double[] computeConeptDivergence(int numTopics, int numDocs, double[] docDivergences, DocModel[] docModels) {
//        double[] conceptDivergences = new double[numTopics];
//        for (int i=0; i<numTopics; i++) {
//            final int topicId = i;
//            conceptDivergences[topicId] = IntStream.range(0, numDocs)
//                    .mapToDouble(docId -> docDivergences[docId] * docModels[docId].proportionOf(topicId))
//                    .sum();
//        }
//        return conceptDivergences;
//    }
}
