package com.base;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class TagDistributions {
    private String[] tagNames;
    private double[][] tagDistributions;
    private double[][] jsDistances;
    private Map<String, Integer> nameToIndex;

    private double[][] getJSDistances() {
        if(jsDistances == null) {
            jsDistances = TagSynonyms.jensonShannonDistances(tagDistributions);
        }
        return jsDistances;
    }

    public TagDistributions(File file) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
        jsDistances = null;
        nameToIndex = new HashMap<>();
        try {
            String line = br.readLine();
            String[] vals = line.split(",");
            if(vals.length != 2) {
                throw new IOException("Tag distribution file should begin with a line of 'numberOfTags," +
                        "numberOfValuesInTagDistribution'");
            }
            int numTags = Integer.parseInt(vals[0]);
            int numWords = Integer.parseInt(vals[1]);
            tagNames = new String[numTags];
            tagDistributions = new double[numTags][numWords];

            int tagIndex = 0;
            while((line = br.readLine()) != null) {
                String[] values = line.split(",");
                for (int i = 0; i < values.length; i++) {
                    values[i] = values[i].trim();
                }

                tagNames[tagIndex] = values[0];
                nameToIndex.put(values[0], tagIndex);
                tagDistributions[tagIndex] = new double[numWords];
                for (int i = 1; i < values.length; i++) {
                    tagDistributions[tagIndex][i - 1] = Double.parseDouble(values[i]);
                }

                tagIndex++;
            }

            if(tagIndex != numTags) {
                throw new IOException("Number of actual tags is not equal to the number specified at the beginning!");
            }
        } finally {
            br.close();
        }
    }

    public double getJSDistance(String tag1, String tag2) {
        return getJSDistances()[nameToIndex.get(tag1)][nameToIndex.get(tag2)];
    }

    // TODO: Possibly break this up into smaller, testable components?
    private static void generateROCData(TagDistributions tags
            , TagSynonymFile tagSynonyms
            , TagTree tagTree
            , String fileName
            , int dataPoints
            , int treeDistanceMax) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(fileName));
        out.write("Threshold Value, Tree Distance Threshold, True Positives, False Positives, True Negatives, False " +
                "Negatives, True " +
                "Positive " +
                "Rate, True Negative Rate, False Positive Rate, False Negative Rate, Precision, F1 Score");
        out.newLine();

        if(tagTree == null) {
            treeDistanceMax = 1;
        }

        System.out.println("Data loaded!");
        double[][] jsDistances = TagSynonyms.jensonShannonDistances(tags.tagDistributions);
        System.out.println("Synonyms Built!");
        int max = dataPoints;
        for(int k = 0; k <= max; k++) {
            System.out.println("Running threshold " + k);
            double tagThreshold = ((double) k) / ((double) max);
            boolean[][] result = TagSynonyms.detectSynonymsFromJSDistance(jsDistances, tagThreshold);
            System.out.println("Result length " + result.length);
            for(int l = 1; l <= treeDistanceMax; l++) {
                int truePositives = 0;
                int trueNegatives = 0;
                int falsePositives = 0;
                int falseNegatives = 0;
                for (int i = 0; i < result.length; i++) {
                    for (int j = i + 1; j < result.length; j++) {
                        int treeDistance = tagTree == null ? 0 : tagTree.getTreeDistance(tags.tagNames[i], tags
                                .tagNames[j]);
                        boolean isSynonym = tagSynonyms.isSynonym(tags.tagNames[i], tags.tagNames[j]);
                        boolean resultSynonym = result[i][j] && (treeDistance <= l);
                        if (resultSynonym == true && isSynonym == true) {
                            truePositives++;
                        } else if (resultSynonym == false && isSynonym == false) {
                            trueNegatives++;
                        } else if (resultSynonym == true && isSynonym == false) {
                            falsePositives++;
                        } else if (resultSynonym == false && isSynonym == true) {
                            falseNegatives++;
                        }
                    }
                }

                double truePositiveRate = ((double) truePositives) / ((double) (truePositives + falseNegatives));
                double trueNegativeRate = ((double) trueNegatives) / ((double) (trueNegatives + falsePositives));
                double falsePositiveRate = ((double) falsePositives) / ((double) (trueNegatives + falsePositives));
                double falseNegativeRate = ((double) falseNegatives) / ((double) (truePositives + falseNegatives));

                double denom = ((double) (truePositives + falsePositives));
                double precision = denom == 0.0 ? 0.0 : ((double) truePositives) / denom;
                double f1score = precision == 0.0 ? 0.0 : truePositiveRate == 0.0 ? 0.0 : 2.0 / (1.0 / precision + 1.0 / truePositiveRate);

                out.write(tagThreshold + ", " + l + ", " + truePositives + ", " + falsePositives + ", " +
                        trueNegatives +
                        ", " +
                        falseNegatives + ", " + truePositiveRate + ", " + trueNegativeRate + ", " + falsePositiveRate +
                        ", " + falseNegativeRate + ", " + precision + ", " + f1score);
                out.newLine();
            }
        }
        out.close();
    }
    
    private static void markSynomymPairs(TagDistributions tags, TagSynonymFile tagSynonyms, TagTree tagTree,
            String fileName, double tagThreshold, int treeThreshold) throws IOException {
        try (BufferedWriter out = new BufferedWriter(new FileWriter(fileName))) {
            out.write("Tag 1, Tag 2, TP, FP, TN, FN, Distance Violation, Tree Violation");
            out.newLine();

            double[][] jsDistances = TagSynonyms.jensonShannonDistances(tags.tagDistributions);

            boolean[][] result = TagSynonyms.detectSynonymsFromJSDistance(jsDistances, tagThreshold);

            for (int i = 0; i < result.length; i++) {
                for (int j = i + 1; j < result.length; j++) {

                    int truePositive = 0;
                    int trueNegative = 0;
                    int falsePositive = 0;
                    int falseNegative = 0;
                    int distanceViolation = 0;
                    int treeViolation = 0;

                    int treeDistance = tagTree == null ? 0
                            : tagTree.getTreeDistance(tags.tagNames[i], tags.tagNames[j]);
                    boolean isSynonym = tagSynonyms.isSynonym(tags.tagNames[i], tags.tagNames[j]);
                    boolean resultSynonym = result[i][j] && (treeDistance <= treeThreshold);
                    if (resultSynonym == true && isSynonym == true) {
                        truePositive = 1;
                    } else if (resultSynonym == false && isSynonym == false) {
                        trueNegative = 1;
                    } else if (resultSynonym == true && isSynonym == false) {
                        falsePositive = 1;
                    } else if (resultSynonym == false && isSynonym == true) {
                        falseNegative = 1;
                        if (!result[i][j]) distanceViolation = 1;
                        if (!(treeDistance <= treeThreshold)) treeViolation = 1;
                    }

                    out.write(tags.tagNames[i] + "," + tags.tagNames[j] + "," + truePositive + "," + falsePositive + ","
                            + trueNegative + "," + falseNegative + "," + distanceViolation + "," + treeViolation);
                    out.newLine();
                }
            }

        }
    }

    public static void generateTreeCorrelationData(TagDistributions tags
            , TagSynonymFile tagSynonyms
            , TagTree tagTree
            , String fileName
            , double threshold
            , int treeDistanceMax) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(fileName));
        String fileOpener = "TP, TN, FP, FN";
        out.write(fileOpener);
        out.newLine();



        double[][] jsDistances = TagSynonyms.jensonShannonDistances(tags.tagDistributions);
        boolean[][] result = TagSynonyms.detectSynonymsFromJSDistance(jsDistances, threshold);
        int[][] histogram = new int[4][treeDistanceMax];
        for(int i = 0; i < histogram.length; i++) {
            for(int j = 0; j < histogram[0].length; j++) {
                histogram[i][j] = 0;
            }
        }

        for(int i = 0; i < jsDistances.length; i++) {
            for(int j = i+1; j < jsDistances[0].length; j++) {
                boolean isSynonym = tagSynonyms.isSynonym(tags.tagNames[i], tags.tagNames[j]);
                int index = -1;
                if (result[i][j] == true && isSynonym == true) {
                    index = 0;
                } else if (result[i][j] == false && isSynonym == false) {
                    index = 1;
                } else if (result[i][j] == true && isSynonym == false) {
                    index = 2;
                } else if (result[i][j] == false && isSynonym == true) {
                    index = 3;
                }

                String name1 = tags.tagNames[i];
                String name2 = tags.tagNames[j];
                histogram[index][tagTree.getTreeDistance(name1, name2)-1]++;
            }
        }

        int[] denoms = new int[4];
        for(int i = 0; i < histogram.length; i++) {
            denoms[i] = 0;
            for(int j = 0; j < histogram[0].length; j++) {
                denoms[i] += histogram[i][j];
            }
        }

        for (int j = 0; j < histogram[0].length; j++) {
            for (int i = 0; i < histogram.length; i++) {
                double val = (double)histogram[i][j];//((double)histogram[i][j])/((double)denoms[i]);
                out.write(Double.toString(val));
                if(i < histogram.length - 1) {
                    out.write(", ");
                }
            }
            out.newLine();
        }
        out.close();
    }

    private static class SynonymPair {
        public double jsDistance;
        public String tag;
        public String synonym;

        public SynonymPair(double jsDistance, String tag, String synonym) {
            this.jsDistance = jsDistance;
            this.tag = tag;
            this.synonym = synonym;
        }
    }

    public static void generateSortedSynonymList(TagDistributions tags, String fileName)
            throws
            IOException {
        try (BufferedWriter out = new BufferedWriter(new FileWriter(fileName))) {
            out.write("JS Distance, Tag, Synonym");
            out.newLine();
    
            List<SynonymPair> pairs = new ArrayList<>();
            double[][] jsDistances = TagSynonyms.jensonShannonDistances(tags.tagDistributions);
            for(int i = 0; i < jsDistances.length; i++) {
                for(int j = i+1; j < jsDistances.length; j++) {
                    pairs.add(new SynonymPair(jsDistances[i][j],tags.tagNames[i],tags.tagNames[j]));
                }
            }
            Collections.sort(pairs, new Comparator<SynonymPair>() {
                @Override
                public int compare(SynonymPair synonymPair, SynonymPair t1) {
                    if(synonymPair.jsDistance > t1.jsDistance) {
                        return 1;
                    } else if(synonymPair.jsDistance < t1.jsDistance) {
                        return -1;
                    } else {
                        return 0;
                    }
                }
            });
    
            for(SynonymPair pair : pairs) {
                out.write(pair.jsDistance + ", " + pair.tag + ", " + pair.synonym);
                out.newLine();
            }
        }
    }
    
    
    private static void generateDistancesFile(String topicsFilename
            , String tagTreeFilename, String distancesResultFilename) throws IOException {
        TagDistributions tags = new TagDistributions(new File(topicsFilename));         // tags' topic distributions
        TagTree tagTree = new TagTree(new File(tagTreeFilename));                          // tags' tree structure
        double[][] distributionDistances = TagSynonyms.jensonShannonDistances(tags.tagDistributions);
        double[][] treeDistances = new double[tags.tagDistributions.length][tags.tagDistributions.length];

       for(int i = 0; i < tags.tagDistributions.length; i++) {
           for(int j = i; j < tags.tagDistributions.length; j++) {
               treeDistances[i][j] = tagTree.getTreeDistance(tags.tagNames[i], tags.tagNames[j]);
               treeDistances[j][i] = treeDistances[i][j];
           }
       }
       
       try (BufferedWriter writer = new BufferedWriter(new FileWriter(distancesResultFilename))) {
           for(int i = 0; i < tags.tagDistributions.length; i++) {
               for(int j = i; j < tags.tagDistributions.length; j++) {
                   writer.write(i + "," + j + "," 
                           + tags.tagNames[i] + "," + tags.tagNames[j] + "," 
                           + distributionDistances[i][j] + "," + treeDistances[i][j]);
                   writer.newLine();
               }
           }
       }
    }
    
    private static void generateRocFile(String soSynnonymFilename, String topicsFilename, String tagTreeFilename, String rocResultFilename) throws IOException {
        TagSynonymFile tagSynonyms = new TagSynonymFile(new File(soSynnonymFilename));  // ground truth, human labeled 
        TagDistributions tags = new TagDistributions(new File(topicsFilename));         // tags' topic distributions
        TagTree tree = tagTreeFilename == null ? null :new TagTree(new File(tagTreeFilename));
        // tags' tree structure
        /* 100 is the number of data point that makes of the ROC, i.e., 100 different thresholds 
         * 20 is the maximum distance between two tags on the tree. The program uses an exhaustive
         * search, and this number is necessary */
        generateROCData(tags,tagSynonyms,tree,rocResultFilename, 1000, 20);
    }
    
    private static void generateSynonymFile(String soSynnonymFilename, String topicsFilename, String tagTreeFilename, String synonymResultFilename, double tagThreshold, int treeThreshold) throws IOException {
        TagSynonymFile tagSynonyms = new TagSynonymFile(new File(soSynnonymFilename));  // ground truth, human labeled 
        TagDistributions tags = new TagDistributions(new File(topicsFilename));         // tags' topic distributions
        TagTree tree = tagTreeFilename == null ? null :new TagTree(new File(tagTreeFilename));
        // tags' tree structure
        /* 100 is the number of data point that makes of the ROC, i.e., 100 different thresholds 
         * 20 is the maximum distance between two tags on the tree. The program uses an exhaustive
         * search, and this number is necessary */
        markSynomymPairs(tags,tagSynonyms,tree, synonymResultFilename, tagThreshold, treeThreshold);
    }

    public static void main(String[] args) throws IOException {
        if (args.length > 0 && args[0].equals("roc")) {
            generateRocFile("TagSynonyms_20170613.csv",
                    "so2016p5000w300LLDA_iter500_tag_topic_distr.txt",
                    null,//"so2016p5000w300_iter500_tree.txt",
                    "so2016p5000w300LLDA_iter500_roc.csv");
        } else if (args.length > 0 && args[0].equals("distances")) {
            generateDistancesFile("so2016p5000w300_iter500_tag_topic_distr.txt",
                    "so2016p5000w300_iter500_tree.txt",
                    "so2016p5000w300_iter500_distances.csv");
        } else if (args.length > 0 && args[0].equals("pairs")) {
            System.out.println("Generating synonym pair file");
            generateSynonymFile("TagSynonyms_20170613.csv",
                    "so2016p5000w300_iter500_tag_topic_distr.txt",
                    "so2016p5000w300_iter500_tree.txt",
                    "so2016p5000w300L2h_iter500_pairs.csv",
                    0.51,
                    1);
            System.out.println("Generated synonym pair file");
        } else {
            System.out.println("Usage: TagDistributions <roc|distances|pairs>");
        }
    }
}
