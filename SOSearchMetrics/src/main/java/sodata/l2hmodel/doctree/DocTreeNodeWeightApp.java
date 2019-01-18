package sodata.l2hmodel.doctree;

import java.io.IOException;
import java.nio.file.Paths;

import sodata.l2hmodel.tree.L2hTree;
import sodata.l2hmodel.tree.L2hTreeNode;

public class DocTreeNodeWeightApp {
    private static int sumOfCountSum1 = 0, sumOfLabelFrequency1 = 0;
    private static double productSum = 0.0;
    
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Usage: DocTreeNodeWeightApp <doc_tree_file>");
            System.exit(0);
        }
        L2hTree docConceptTree = L2hTree.newInstanceFromIndentedTreeFile(Paths.get(args[0]));
        
        docConceptTree.bfs(node -> accumulate1(node));
        
        docConceptTree.bfs(node -> proudct1(node));
        
        docConceptTree.bfs(node -> divideAndPrintPruduct(node));
    }
    
    public static void accumulate1(L2hTreeNode node) {
        sumOfCountSum1 += node.getCountSum();
        sumOfLabelFrequency1 += node.getLabelFrequency();
    }
    
    public static void proudct1(L2hTreeNode node) {
        productSum += ((double)node.getCountSum()/(double)sumOfCountSum1) * ((double)node.getLabelFrequency()/(double)sumOfLabelFrequency1);
    }

    public static void divideAndPrintPruduct(L2hTreeNode node) {
        System.out.println(node.getLabel() + "," + (((double) node.getCountSum() / (double) sumOfCountSum1)
                * ((double) node.getLabelFrequency() / (double) sumOfLabelFrequency1) / productSum)
                + "," + (double)node.getCountSum()/(double)sumOfCountSum1
                + "," + (double)node.getLabelFrequency()/(double)sumOfLabelFrequency1);
    }
}
