package sotags.modelconverter;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class L2hToEntagrecModelConverter {
    private final static Logger LOGGER = LoggerFactory.getLogger(L2hToEntagrecModelConverter.class);
    private static int numOfRepetitions = -1;
    private static int crossNumber = -1;
    private static int l2hMaxIter = -1;

    // private static String trainDataFile = "trainDataset_distr.csv";
    // private static String testDataFile = "testDataset.csv";
    // private static String goldenDataFile = "goldenSet.csv";

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("usage [root path] [entagrec propertities file] [l2h paras shellscript]");
            return;
        }
        String root = args[0];

        try {
            setParametersFromPropertyFile(args[1]);

            setL2hParametersFromPropertyFile(args[2]);

            for (int repeatI = 0; repeatI < numOfRepetitions; repeatI++) {
                Path testDirPath = Paths.get(root, "testcase_repeat" + repeatI);
                LOGGER.info("testDir is " + testDirPath.toString());

                for (int foldIndex = 0; foldIndex < crossNumber; foldIndex++) {
                    Path outputL2hModelDirPath = Paths.get(testDirPath.toString(), Integer.toString(foldIndex),
                            "l2h_out");
                    LOGGER.info("outputL2hModelDirPath is " + outputL2hModelDirPath.toString());
                    
                    Path l2hLabelFilePath = Paths.get(testDirPath.toString(), Integer.toString(foldIndex),
                            "l2h_out", "mst", "labels.voc");
                    LOGGER.info("l2hLabelFilePath is " + l2hLabelFilePath.toString());
                    List<String> tagList = loadL2hTags(l2hLabelFilePath);
  
                    Path trainDocModelFilePath = findDocModelFile(outputL2hModelDirPath, "train");
                    LOGGER.info("Found " + trainDocModelFilePath.toString());
                    
                    Path outputQueryTrainFilePath = Paths.get(testDirPath.toString(), Integer.toString(foldIndex),
                            "query-training-out.csv");
                    LOGGER.info("outputQueryTrainFilePath is " + outputQueryTrainFilePath.toString());
                    loadL2hDocModel(trainDocModelFilePath, tagList, outputQueryTrainFilePath);
                    
                    Path testDocModelFilePath = findDocModelFile(outputL2hModelDirPath, "test");
                    LOGGER.info("Found " + testDocModelFilePath.toString());
                    
                    Path outputQueryTestFilePath = Paths.get(testDirPath.toString(), Integer.toString(foldIndex),
                            "query-testing-out.csv");
                    LOGGER.info("outputQueryTestFilePath is " + outputQueryTestFilePath.toString());
                    loadL2hDocModel(testDocModelFilePath, tagList, outputQueryTestFilePath);
                }
            }
        } catch (FileNotFoundException e) {
            LOGGER.error(e.getMessage(), e);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }

    }
    
    private static List<String>  loadL2hTags(Path tagFilePath) throws IOException  {
        List<String> tagList = new ArrayList<String>();
        Files.lines(tagFilePath, StandardCharsets.UTF_8).forEach(tag -> tagList.add(tag));
        return tagList;
    }
    
    private static void loadL2hDocModel(Path docModelFilePath, List<String> tagList, Path outEntagrecQueryFile)
            throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(outEntagrecQueryFile, StandardCharsets.UTF_8)) {
            int numDocs = Integer.valueOf(Files.lines(docModelFilePath, StandardCharsets.UTF_8)
                    .findFirst().get());
            int counter = 0;
            for (String s: Files.lines(docModelFilePath, StandardCharsets.UTF_8)
                    .skip(1)
                    .map(doc -> convertL2hDocModelLine(doc, tagList))
                    .collect(Collectors.toList())) {
                writer.write(s);
                writer.write("\n");
                counter ++;
            }
            if (counter != numDocs) {
                throw new IllegalStateException("The number of lines of documents in " + docModelFilePath
                        + " isn't " + numDocs);
            }
        }
    }
    
    private static String convertL2hDocModelLine(String doc, List<String> tagList)  {
        String[] parts = doc.split("\t");
        WeightedTag[] tags = new WeightedTag[parts.length - 1];
        for (int i=1; i<parts.length; i++) {
            tags[i-1] = new WeightedTag(tagList.get(i-1), Double.valueOf(parts[i]));
        }
        Arrays.sort(tags, (lhs, rhs) -> - Double.compare(lhs.getWeight(), rhs.getWeight()));
        StringBuffer sb = new StringBuffer();
        sb.append(parts[0]).append(",");
        for (WeightedTag tag: tags) {
            sb.append(tag.toString()).append(",");
        }
        return sb.toString();
    }

    private static Path findDocModelFile(Path root, String type) throws IOException {
        List<Path> pathList = new LinkedList<Path>();
        FileSystem fileSystem = FileSystems.getDefault();
        PathMatcher pathMatcher = fileSystem.getPathMatcher("glob:**/PRESET_L2H_K-*/iter-predictions/iter-" + Integer.toString(l2hMaxIter) + "_" + type + ".txt");
        Files.find(root, 99, (path, attr) -> { 
            if (!attr.isDirectory()) return pathMatcher.matches(path);
            else return false;
        }).forEach(path -> pathList.add(path));
        
        if (pathList.size() > 1 || pathList.size() == 0) {
            throw new IllegalStateException("Expect to find only 1 doc model file, but " + pathList.size() + " found");
        } 
        
        return pathList.get(0);
    }

    private static void setParametersFromPropertyFile(String propertyFile) throws FileNotFoundException, IOException {

        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(propertyFile)) {
            props.load(in);
        }

        String value = props.getProperty("numOfRepetitions");
        if (value == null) {
            throw new IllegalStateException("Property file " + propertyFile + " does not have numOfRepetitions");
        }
        numOfRepetitions = Integer.valueOf(value);

        LOGGER.info("From " + propertyFile + ": numOfRepetitions = " + numOfRepetitions);

        value = props.getProperty("crossNumber");
        if (value == null) {
            throw new IllegalStateException("Property file " + propertyFile + " does not have crossNumber");

        }
        crossNumber = Integer.valueOf(value);

        LOGGER.info("From " + propertyFile + ": crossNumber  = " + crossNumber);
        //
        //
        // value = props.getProperty("trainDataFile");
        // if (value != null) {
        // trainDataFile = value;
        // }
        // LOGGER.info("From " + propertyFile + ": trainDataFile = " + trainDataFile);
        //
        // value = props.getProperty("testDataFile");
        // if (value != null) {
        // testDataFile = value;
        // }
        // LOGGER.info("From " + propertyFile + ": testDataFile = " + testDataFile);
    }

    private static void setL2hParametersFromPropertyFile(String propertyFile)
            throws FileNotFoundException, IOException {

        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(propertyFile)) {
            props.load(in);
        }

        String value = props.getProperty("l2h_maxiter");
        if (value == null) {
            throw new IllegalStateException("Property file " + propertyFile + " does not have l2h_maxiter");
        }
        l2hMaxIter = Integer.valueOf(value);
        LOGGER.info("From " + propertyFile + ": l2h_maxiter = " + l2hMaxIter);
    }
    
    private static class WeightedTag {
        private String tag;
        private double weight;
        
        WeightedTag(String tag, double weight) {
            this.tag = tag;
            this.weight = weight;
        }
        
        double getWeight() {
            return weight;
        }
        
        public String toString() {
            return tag + ":" + Double.toString(weight);
        }
    }
}
