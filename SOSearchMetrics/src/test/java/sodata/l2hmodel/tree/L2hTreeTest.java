package sodata.l2hmodel.tree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

public class L2hTreeTest {

    @Test
    public void testL2hTreeDeepCopy() {
            IndentedTreeParser parser = new IndentedTreeParser();
            InputStream in = getClass().getClassLoader().getResourceAsStream("testdata/topwords-500.txt");
            try {
                L2hTree tree = parser.parseAll(in);
                L2hTree copyOfTree = new L2hTree(tree); // deep copy

                List<L2hTreeNode> originalNodes = new LinkedList<L2hTreeNode>();
                List<L2hTreeNode> clonedNodes = new LinkedList<L2hTreeNode>();
                
                tree.bfs(node -> originalNodes.add(node));
                copyOfTree.bfs(node -> clonedNodes.add(node));

                Iterator<L2hTreeNode> originalIter = originalNodes.iterator();
                Iterator<L2hTreeNode> clonedIter = clonedNodes.iterator();
                
                while(originalIter.hasNext() && clonedIter.hasNext()) {
                    L2hTreeNode original = originalIter.next();
                    L2hTreeNode clone = clonedIter.next();

                    // references must be unequal
                    assertFalse(original == clone);
                    assertEquals(original, clone);
                }
                
            } catch (IOException e) {
                e.printStackTrace();
                assertTrue(false);
            }
    }
}
