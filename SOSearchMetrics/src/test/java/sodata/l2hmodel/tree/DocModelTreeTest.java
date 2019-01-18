package sodata.l2hmodel.tree;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.zip.DataFormatException;

import org.junit.Test;

import sodata.l2hmodel.model.DocModelCollection;
import sodata.l2hmodel.model.DocModelTree;
import sodata.l2hmodel.model.LabelCollection;

public class DocModelTreeTest {
    private int numNodesTree;

    @Test
    public void testDocModelTree() {
        System.out.println("entered testDocModelTree");
        Path mstTreeFilePath;
        try {
            mstTreeFilePath = Paths.get(getClass().getClassLoader().getResource("testdata/tree.txt").toURI());
            Path docModelFilePath = Paths.get(getClass().getClassLoader().getResource("testdata/iter-500.txt").toURI());
            Path globalTreeFilePath = Paths
                    .get(getClass().getClassLoader().getResource("testdata/topwords-500.txt").toURI());

            LabelCollection labelCollection = LabelCollection.newInstanceFromTreeFile(mstTreeFilePath);
            DocModelCollection docModelCollection = DocModelCollection
                    .newInstanceFromDocModelFile(labelCollection.getNumLabels(), docModelFilePath);
            L2hTree globalConceptTree = L2hTree.newInstanceFromIndentedTreeFile(globalTreeFilePath);
            globalConceptTree.populatePathFromRoot();
            globalConceptTree.populateSiblingCollections();

            assertTrue(docModelCollection.getNumDocuments() > 0);
            List<DocModelTree> docModelTrees = docModelCollection.getDocTreesAsList(globalConceptTree);
            // assertEquals(docModelTrees.size(), docModelCollection.getNumDocuments());
            int numDocsProcessed = 0;
            for (DocModelTree docModelTree : docModelTrees) {
                numNodesTree = 0;
                docModelTree.getTree().dfs(node -> {
                    ++numNodesTree;
                });
                System.out.println(numNodesTree + ":" + docModelCollection.getNumTopics());
                assertTrue(numNodesTree <= docModelCollection.getNumTopics());
                numDocsProcessed ++;
                System.out.println("num docs processed: " + numDocsProcessed + " : " + (double)numDocsProcessed/(double)docModelTrees.size());
            }
        } catch (URISyntaxException e) {
            assertTrue(false);
            e.printStackTrace();
        } catch (IOException e) {
            assertTrue(false);
            e.printStackTrace();
        } catch (DataFormatException e) {
            assertTrue(false);
            e.printStackTrace();
        }
    }
}
