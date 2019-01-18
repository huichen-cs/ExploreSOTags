package sodata.l2hmodel.tree;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;
import java.util.function.Consumer;

//import sodata.l2hmodel.model.DocModelCollection;

public class L2hTree {
    public final static int WEIGHAT_MULTIPLIER = 1000000;    

    private L2hTreeNode root;
    private L2hTreeStat stat;
    
//    private DocModelCollection docModelCollection;
    private ArrayList<List<L2hTreeNode>> siblingCollections;

    public static L2hTree newInstanceFromIndentedTreeFile(Path indentedTextFilePath) throws IOException {
        
        IndentedTreeParser parser = new IndentedTreeParser();
        L2hTree tree = parser.parseAll(indentedTextFilePath);
        return tree;
    }
    

    public L2hTree(L2hTreeNode root, L2hTreeStat stat) {
        this.root = root;
        this.stat = stat;
    }
    
    // make a deep copy of the tree
    public L2hTree(L2hTree tree) {
        this.root = bfsClone(tree.root);
        this.stat = new L2hTreeStat(tree.stat);
        
//        if (tree.docModelCollection != null) {
//            this.docModelCollection = tree.docModelCollection;
//        }
         
        if (tree.siblingCollections != null && !tree.siblingCollections.isEmpty()) {
            this.populateSiblingCollections();
        }
    }

    public void bfs(Consumer<L2hTreeNode> consumer) {
//        validateDocModelCollection();
        
        // Let's do an breadth first tree traversal
        Queue<L2hTreeNode> queue = new LinkedList<L2hTreeNode>();
        queue.add(root);
        while (!queue.isEmpty()) {
            L2hTreeNode node = queue.remove();
            consumer.accept(node); // pre-order
            for (L2hTreeNode child : node.getChildren()) {
                queue.add(child); // in-order (more than two so in-order is ... fuzzy)
                if (child.getLevel() - node.getLevel() != 1) {
                    throw new IllegalStateException("Node level are set at wrong level");
                }
            }
            // post order
        }
    }

    public void dfs(Consumer<L2hTreeNode> consumer) {
//        validateDocModelCollection();

        // Let's do an pre-order tree traversal
        Stack<L2hTreeNode> stack = new Stack<L2hTreeNode>();
        stack.push(root);
        while (!stack.isEmpty()) {
            L2hTreeNode node = stack.pop();
            consumer.accept(node); // pre-order
            for (L2hTreeNode child : node.getChildren()) {
                stack.push(child); // in-order (more than two so in-order is ... fuzzy)
            }
            // post-order
        }
    }

    public void populatePathFromRoot() {
        dfs(node -> {
            if (node.getParent() == null) { // root node
                node.getPathFromRoot().add(node);
//                for (L2hTreeNode pathNode: node.getPathFromRoot()) {
//                    System.out.print(pathNode.getId() + ":");
//                }
//                System.out.println();
            } else {
                node.getPathFromRoot().addAll(node.getParent().getPathFromRoot());
                node.getPathFromRoot().add(node);
//                for (L2hTreeNode pathNode: node.getPathFromRoot()) {
//                    System.out.print(pathNode.getId() + ":");
//                }
//                System.out.println();
            }
        });
    }
    
    public void populateSiblingCollections() {
        final Map<Integer, List<L2hTreeNode>> map = new HashMap<Integer, List<L2hTreeNode>>();
        bfs(node -> {
            List<L2hTreeNode> siblings = map.get(node.getLevel());
            if (siblings == null) {
                siblings = new LinkedList<L2hTreeNode>();
                map.put(node.getLevel(), siblings);
            }
            siblings.add(node);
//            System.out.println("level: " + node.getLevel() + ":" + siblings.size());
        });
        
        final ArrayList<List<L2hTreeNode>> siblingCollections = new ArrayList<List<L2hTreeNode>>(map.size());
        map.keySet().stream().sorted().forEach(k -> siblingCollections.add(map.get(k)));
        this.siblingCollections = siblingCollections;
    }

    public void printTree(PrintStream writer, boolean keepOrder) {
        // Let's do an in-order tree traversal
        Stack<L2hTreeNode> stack = new Stack<L2hTreeNode>();
        stack.push(root);
        while (!stack.isEmpty()) {
            L2hTreeNode node = stack.pop();
            writer.println(getNodeText(node));
            if (keepOrder) {
                for (int i = node.getChildren().size() - 1; i >= 0; i--) {
                    stack.push(node.getChildren().get(i));
                }
            } else {
                for (L2hTreeNode child : node.getChildren()) {
                    stack.push(child);
                }
            }
        }
    }
    
    public void printTree(PrintStream writer, double[] proportions) {
        // Let's do an in-order tree traversal
        Stack<L2hTreeNode> stack = new Stack<L2hTreeNode>();
        stack.push(root);
        while (!stack.isEmpty()) {
            L2hTreeNode node = stack.pop();
            node.setCountSum((int)Math.ceil(proportions[node.getTopicId()]*WEIGHAT_MULTIPLIER));
            writer.println(getNodeText(node));
            for (L2hTreeNode child : node.getChildren()) {
                stack.push(child);
            }
        }
    }
    
    public void printTree(PrintStream writer) {
        printTree(writer, false);
    }
    
    public void printStat(PrintStream writer) {
        stat.printStat(writer);
    }
    


    public void prune(double[] proportions) {
        // Let's do an breadth first tree traversal
        Queue<L2hTreeNode> queue = new LinkedList<L2hTreeNode>();
        queue.add(root);
        while (!queue.isEmpty()) {
            L2hTreeNode node = queue.remove();
            List<L2hTreeNode> nodesToRemove = new LinkedList<L2hTreeNode>();
            for (L2hTreeNode child : node.getChildren()) {
                double p = proportions[child.getTopicId()];
                if (!Double.isFinite(p) || p == 0) nodesToRemove.add(child);
                else queue.add(child); // in-order (more than two so in-order is ... fuzzy)
            }
            node.getChildren().removeAll(nodesToRemove);
        }
    }
    

//    public void setDocModelCollection(DocModelCollection docModelCollection) {
//        if (docModelCollection == null) {
//            throw new IllegalArgumentException("DocModelCollection must not be null.");
//        }
//        this.docModelCollection = docModelCollection;
//    }
    
    public List<L2hTreeNode> siblingsOnLevel(int level) {
        if (level >= siblingCollections.size()) return null;
        return siblingCollections.get(level);
    }
    
    public L2hTreeNode[] siblingsOnLevelAsArray(int level) {
        return siblingCollections.get(level).toArray(new L2hTreeNode[siblingCollections.get(level).size()]);
    }
    

    public int getHeight() {
        validateSiblingCollections();
        return siblingCollections.size();
    }

    
    private L2hTreeNode bfsClone(L2hTreeNode root) {
        // Let's do an breadth first tree traversal
        L2hTreeNode copyOfRoot = new L2hTreeNode(root); 
        
        Queue<L2hTreeNode> copyQueue = new LinkedList<L2hTreeNode>();
        copyQueue.add(copyOfRoot);
        
        Queue<L2hTreeNode> queue = new LinkedList<L2hTreeNode>();
        queue.add(root); 
        while (!queue.isEmpty()) {
            L2hTreeNode node = queue.remove();
            L2hTreeNode copyNode = copyQueue.remove();
            for (L2hTreeNode child : node.getChildren()) {
                queue.add(child); 
                
                L2hTreeNode copyChild = new L2hTreeNode(child);
                copyNode.getChildren().add(copyChild);
                copyQueue.add(copyChild);
            }
        }
        
        if (!copyQueue.isEmpty()) {
            throw new IllegalStateException("Copy Queue must be empty!");
        }
        return copyOfRoot;
    }

    private String getNodeText(L2hTreeNode node) {
        String indentation = "";
        for (int i=0; i<node.getLevel(); i++) {
            indentation += "\t";
        }
        return indentation + node.toString();
    }
    

//    private void validateDocModelCollection() {
//        if (docModelCollection == null) {
//            throw new IllegalStateException("DocModelCollection must not be null.");
//        }
//    }

    private void validateSiblingCollections() {
        if (siblingCollections == null) {
            throw new IllegalStateException("siblingCollections must not be null.");
        }
    }

}
