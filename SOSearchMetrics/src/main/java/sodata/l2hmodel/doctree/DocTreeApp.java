/* depends on tree parser in the SOSearchMetrics */
package sodata.l2hmodel.doctree;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.zip.DataFormatException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.l2hmodel.model.DocModelCollection;
import sodata.l2hmodel.model.DocModelTree;
import sodata.l2hmodel.model.LabelCollection;
import sodata.l2hmodel.tree.L2hTree;

public class DocTreeApp {
    private final static Logger LOGGER = LoggerFactory.getLogger(DocTreeApp.class);

    public static void main(String[] args) throws IOException, DataFormatException {
        LOGGER.info("DocTreeApp started with argments: " + args.length);
        if (args.length < 5) {
            System.out.println(
                    "Usage: DocTreeApp <mst_tree_file> <doc_model_doc_info_file> <doc_model_doc_data_file> <doc_model_file> <global_tree_file> <n_trees> <doc_tree_folder>");
            System.exit(-1);
        }

        Path mstTreeFilePath = Paths.get(args[0]);

        if (!Files.exists(mstTreeFilePath)) {
            System.out.println(String.format(
                    "MST Tree file %s does not exist. " + "It is usually located at <output>/mst/tree.txt.", args[0]));
            System.exit(-1);
        }
        
        Path docModelDocInfoFilePath = Paths.get(args[1]);
        if (!Files.exists(docModelDocInfoFilePath)) {
            System.out.println(String.format(
                    "Doc Model Doc Info file  %s does not exist. " + "It is usually located at <input>/datasetname.docinfo", args[1]));
            System.exit(-1);
        }
        Path docModelDocDataFilePath = Paths.get(args[2]);
        if (!Files.exists(docModelDocDataFilePath)) {
            System.out.println(String.format(
                    "Doc Model Doc Data file  %s does not exist. " + "It is usually located at <input>/datasetname.docinfo", args[2]));
            System.exit(-1);
        }
        
        Path docModelFilePath = Paths.get(args[3]);
        if (!Files.exists(docModelFilePath)) {
            System.out.println(String.format("Doc model file %s does not exists. "
                    + "It is sually located at <output>/<preset>/iter_predictions/iter-xxx.txt", args[3]));
            System.exit(-1);
        }
        
        Path globalTreeFilePath = Paths.get(args[4]);
        if (!Files.exists(globalTreeFilePath)) {
            System.out.println(String.format("Doc model file %s does not exists. "
                    + "It is sually located at <output>/<preset>/report/topwords-xxx.txt", args[4]));
            System.exit(-1);
        }
        
        LabelCollection labelCollection = LabelCollection.newInstanceFromTreeFile(mstTreeFilePath);
        DocModelCollection docModelCollection = DocModelCollection.newInstanceFromDocModelFile(labelCollection.getNumLabels(), docModelFilePath);
        docModelCollection.setDocInfoPropertiesFromDocInfoFile(docModelDocInfoFilePath);
        docModelCollection.setDocWordStatisticsFromDocDataFile(docModelDocDataFilePath);
        L2hTree globalConceptTree = L2hTree.newInstanceFromIndentedTreeFile(globalTreeFilePath);
        globalConceptTree.populatePathFromRoot();
        globalConceptTree.populateSiblingCollections();

        int numDocTrees = Integer.parseInt(args[5]);
        Random rng = new Random();
        int[] docNos = new int[numDocTrees];
        for (int i=0; i<numDocTrees; i++) {
            docNos[i] = rng.nextInt(docModelCollection.getNumDocuments());
        }
        List<DocModelTree> docTrees = docModelCollection.getDocTreesAsList(globalConceptTree, docNos);

        if (args.length >= 7) {
            for (int i = 0; i < docNos.length; i++) {
                Path docTreeFolderPath = Paths.get(args[6]);
                if (!Files.exists(docTreeFolderPath)) {
                    Files.createDirectories(docTreeFolderPath);
                }
                DocModelTree docTree = docTrees.get(i);
                Path docTreeFilePath = Paths.get(args[6], 
                        Integer.toString(docTree.getDocModel().getNumWords()) + "_"
                                + Long.toString(docTree.getDocModel().getDocInfoId()) + "_" 
                                + Integer.toString(docNos[i]) + ".txt");
                double[] proportions = IntStream.range(0, docModelCollection.getNumTopics())
                        .mapToDouble(topicId -> docTree.getDocModel().proportionOf(topicId)).toArray();
                try (PrintStream writer = new PrintStream(Files.newOutputStream(docTreeFilePath), false, StandardCharsets.UTF_8.name())) {
                    docTree.getTree().printTree(writer, proportions);
                }

                LOGGER.debug("write to " + docTreeFilePath);
            }
        }

        LOGGER.info("DocTreeApp completed.");
    }
    
    
}
