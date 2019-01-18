package sodata.l2hmodel.entropy;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.zip.DataFormatException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.l2hmodel.model.DocModelCollection;
import sodata.l2hmodel.model.LabelCollection;
import sodata.l2hmodel.tree.L2hTree;
import sodata.l2hmodel.tree.L2hTreeNode;

public class GlobalTreeCrossDivergenceApp {
    private final static Logger LOGGER = LoggerFactory.getLogger(GlobalTreeCrossDivergenceApp.class);

    public static void main(String[] args) throws IOException, DataFormatException {
        LOGGER.info("GlobalTreeCrossDivergenceApp started.");
        if (args.length <= 3) {
            System.out.println(
                    "Usage: GlobalTreeCrossDivergenceApp <mst_tree_file> <doc_model_file> <global_tree_file> [specificity_out_file]");
            System.exit(-1);
        }

        Path mstTreeFilePath = Paths.get(args[0]);

        if (!Files.exists(mstTreeFilePath)) {
            System.out.println(String.format(
                    "MST Tree file %s does not exist. " + "It is usually located at <output>/mst/tree.txt.", args[0]));
            System.exit(-1);
        }

        Path docModelFilePath = Paths.get(args[1]);
        if (!Files.exists(docModelFilePath)) {
            System.out.println(String.format("Doc model file %s does not exists. "
                    + "It is sually located at <output>/<preset>/iter_predictions/iter-xxx.txt", args[1]));
            System.exit(-1);
        }
        
        Path globalTreeFilePath = Paths.get(args[2]);
        if (!Files.exists(globalTreeFilePath)) {
            System.out.println(String.format("Doc model file %s does not exists. "
                    + "It is sually located at <output>/<preset>/report/topwords-xxx.txt", args[2]));
            System.exit(-1);
        }
        
        LabelCollection labelCollection = LabelCollection.newInstanceFromTreeFile(mstTreeFilePath);
        DocModelCollection docModelCollection = DocModelCollection.newInstanceFromDocModelFile(labelCollection.getNumLabels(), docModelFilePath);
        L2hTree globalConceptTree = L2hTree.newInstanceFromIndentedTreeFile(globalTreeFilePath);
//        globalConceptTree.setDocModelCollection(docModelCollection);
        globalConceptTree.populatePathFromRoot();

        System.out.println("num documents: " + docModelCollection.getNumDocuments());
        IntStream.range(0, docModelCollection.getNumDocuments())
            .mapToDouble(docId->docModelCollection.docDivergenceOf(docId))
            .min().ifPresent(d -> System.out.println("min divergence of all documents: " + d));
        IntStream.range(0, docModelCollection.getNumDocuments())
            .mapToDouble(docId->docModelCollection.docDivergenceOf(docId))
            .max().ifPresent(d -> System.out.println("max divergence of all documents: " + d));

        globalConceptTree.populateSiblingCollections();
        StringBuilder sb = new StringBuilder();
        List<String> lines = new LinkedList<String>();
        for (int level=1; level<globalConceptTree.getHeight(); level++) {
            L2hTreeNode[] siblings = globalConceptTree.siblingsOnLevelAsArray(level);
            DocModelCollection anchorCollection = docModelCollection.getSubcollection(siblings[0].getTopicId());
            for (int j=0; j<siblings.length; j++) {
                DocModelCollection subcollection = docModelCollection.getSubcollection(siblings[j].getTopicId());
                double[] divergence = subcollection.computeCrossDivergence(anchorCollection);
                double avg = Arrays.stream(divergence).average().getAsDouble();
                double min = Arrays.stream(divergence).min().getAsDouble();
                double max = Arrays.stream(divergence).max().getAsDouble();
                sb.append(level).append(",")
                    .append(siblings[0].getTopicId()).append(",")
                    .append(siblings[j].getTopicId()).append(",")
                    .append(min).append(",")
                    .append(max).append(",")
                    .append(avg);
               lines.add(sb.toString());
               LOGGER.debug(sb.toString());
               sb.setLength(0);
            }
        }
        
        Path outDivergenceCsvFilePath = null;
        if (args.length >= 4) {
            outDivergenceCsvFilePath = Paths.get(args[3]);
            try (BufferedWriter writer = Files.newBufferedWriter(outDivergenceCsvFilePath,  StandardCharsets.UTF_8)) {
                for (String line: lines) {
                    writer.write(String.format("%s%n", line));
                }
            }
            LOGGER.debug("write to " + outDivergenceCsvFilePath);
        }

        LOGGER.info("GlobalTreeCrossDivergenceApp completed.");
    }
}
