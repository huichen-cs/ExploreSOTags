/**
 * This program computes diversity and specificity for the global tree.
 * It requires three input files,
 *  1. Input MST Tree at 
 *      output_folder/mst/tree.txt
 *  2. Document model file (topic proportions of each document) at 
 *      output_folder/PRESET_FOLDER/iter-predictions/iter-###.txt
 *  3. Output global tree file at
 *      output_folder/PRESET_FOLDER/report/topwords-###.txt
 */
package sodata.l2hmodel.entropy;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

public class GlobalTreeSpecificityApp {
    private final static Logger LOGGER = LoggerFactory.getLogger(GlobalTreeSpecificityApp.class);

    public static void main(String[] args) throws IOException, DataFormatException {
        if (args.length < 3) {
            System.out.println(
                    "Usage: GlobalTreeSpecificityApp <mst_tree_file> <doc_model_file> <global_tree_file> [specificity_out_file]");
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
        
        IntStream.range(0, docModelCollection.getNumTopics())
            .forEach(topicId -> LOGGER.debug(topicId + "," + docModelCollection.conceptSpecificityOf(topicId)));

//        IntStream.range(0, docModelCollection.getNumDocuments())
//            .forEach(docId -> LOGGER.debug("docId: " + docId + " entropy: " + docModelCollection.diversityOf(docId)));

        // assign the specificity to the tree when traverse the tree
        globalConceptTree.dfs(node -> System.out.println("At node: " + node.getTopicId() + " with specificity: "
                + docModelCollection.conceptSpecificityOf(node.getTopicId())));
        globalConceptTree.populatePathFromRoot();
        
        final StringBuilder sb = new StringBuilder();
        final List<String> lines = new LinkedList<String>();

        globalConceptTree.dfs(node -> {
            if (node.getChildren() == null || node.getChildren().size() == 0) { // leaf node
                
                for (L2hTreeNode pathNode: node.getPathFromRoot()) {
                    System.out.print("[" + pathNode.getId() + " " + pathNode.getLabel() + " " + docModelCollection.conceptSpecificityOf(pathNode.getTopicId()) + "]:");
                    if (sb.length() > 0) sb.append(",");
                    sb.append(docModelCollection.conceptSpecificityOf(pathNode.getTopicId()));
                }
                System.out.println();
                lines.add(sb.toString());
                sb.setLength(0);
            }
        });
        
        Path outSpecificityCsvFilePath = null;
        if (args.length >= 4) {
            outSpecificityCsvFilePath = Paths.get(args[3]);
            try (BufferedWriter writer = Files.newBufferedWriter(outSpecificityCsvFilePath,  StandardCharsets.UTF_8)) {
                for (String line: lines) {
                    writer.write(String.format("%s%n", line));
                }
            }
        }
    }
}
