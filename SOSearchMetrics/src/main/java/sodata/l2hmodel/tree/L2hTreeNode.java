package sodata.l2hmodel.tree;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

// See Line 1585 in L2H.java
public class L2hTreeNode {
    private int id;
    private int labelFrequency;  // index in the label vocabulary 
    private int level;
    private long countSum;
    
    private L2hTreeNode parent;

    private String pathString;
    private int numChildren;
    private String label;
    private String[] topWords;
    
    private List<L2hTreeNode> children;
    
    private List<L2hTreeNode> pathFromRoot;

    public L2hTreeNode(int id, int index, int level, long countSum, String pathString, String label, String[] topWords, int numChildren, L2hTreeNode parent) {
        this.labelFrequency = index;
        this.level = level;
        this.countSum = countSum;
        this.parent = parent;
        this.id = id;
        this.pathString = pathString;
        this.label = label;
        this.numChildren = numChildren;
        this.topWords = null;
        
        this.children = new LinkedList<L2hTreeNode>();
        this.pathFromRoot = new LinkedList<L2hTreeNode>();
    }
    
    public L2hTreeNode() {
        this(-1, -1, -1, -1L, null, null, null, -1, null);
    }

    public L2hTreeNode(L2hTreeNode node) {
        this(node.id, node.labelFrequency, node.level, node.countSum, node.pathString, node.label, node.topWords,
                node.numChildren, node.parent);
        if (node.topWords != null) {
            topWords = Arrays.copyOf(node.topWords, node.topWords.length);
        }
    }
    
    @Override 
    public boolean equals(Object theOther) {
        if (theOther == null) {
            return false;
        }

        if (!(theOther instanceof L2hTreeNode)) {
            return false;
        }

        L2hTreeNode other = (L2hTreeNode) theOther;

        if (labelFrequency != other.labelFrequency) return false;
        
        if (level != other.level) return false;
        
        if (countSum != other.countSum) return false;
        
        if (id != other.id) return false;
        
        if (pathString == null && other.pathString != null) return false;
        
        if (pathString != null && !pathString.equals(other.pathString)) return false;
        
        if (label != other.label) return false;
        
        if (numChildren != other.numChildren) return false;

        return Arrays.equals(topWords, other.topWords);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(labelFrequency, level, countSum, parent, id, pathString, label, numChildren, topWords);
    }

    public List<L2hTreeNode> getChildren() {
        return children;
    }

    public int getLevel() {
        return level;
    }
    

    public int getNumChildren() {
        return numChildren;
    }


    public L2hTreeNode getParent() {
        return parent;
    }
    
    public List<L2hTreeNode> getPathFromRoot() {
        return pathFromRoot;
    }


    public void setLevel(int level) {
        this.level = level;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setPathString(String pathString) {
        this.pathString = pathString;
    }

    public void setNumChildren(int numChildren) {
        this.numChildren = numChildren;
    }

    public void setCountSum(long countSum) {
        this.countSum = countSum;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setLabelFrequency(int labelFrequency) {
        this.labelFrequency = labelFrequency;
    }
    

    public void setParent(L2hTreeNode parent) {
        this.parent = parent;
    }
    
    public int getId() {
        return id;
    }

    public int getTopicId() {
        return id;
    }
    
    public long getCountSum() {
        return countSum;
    }
    

    public String getLabel() {
        return label;
    }
    
    public int getLabelFrequency() {
        return this.labelFrequency;
    }
    
    public void setCountSum(int countSum) {
        this.countSum = countSum;
    }

    public void setTopWords(String[] words) {
        this.topWords = words;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(id).append(", ")
                .append(pathString)
                .append(", #c = ").append(getChildren().size())
                .append(", #o = ").append(countSum)
            .append("]")
            .append(", ")
                .append(label).append(" (").append(labelFrequency).append(")")
                .append(" ").append(countSum < 0 ? "" : countSum)
            .append(" ").append(topWords == null ? "" : Arrays.toString(topWords));
        return sb.toString();
    }
}
