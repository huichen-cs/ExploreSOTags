package com.base;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TagTree {
    private static final int DEFAULT_NUM_CHILDREN = 1000;

    private static class TagTreeNode {
        private String name;
        private TagTreeNode[] nodes;
        private TagTreeNode parent;

        private TagTreeNode(String name, int numChildren) {
            this.name = name;
            this.parent = null;
            nodes = new TagTreeNode[numChildren];
            for(int i = 0; i < nodes.length; i++) {
                nodes[i] = null;
            }
        }

        public void addNode(int index, TagTreeNode node) {
            this.nodes[index] = node;
        }

        public void printTree(Writer writer, TagTreeNode parent) throws IOException {
            writer.write("{");
            writer.write("\"name\": \""+name+"\",");
            String parentName = parent == null ? "null" : parent.name;
            writer.write("\"parent\": \""+parentName+"\"");

            // TODO: This code is assuming that if nodes has any non null elements, the first element will always be
            // non null. This may not always be true! In the future, make sure this works in all cases.
            TagTreeNode child = nodes[0];
            if(nodes[0] != null) {
                writer.write(", \"children\": [");
            }
            int childIndex = 0;
            while(child != null) {
                child.printTree(writer, this);
                childIndex++;
                if(childIndex >= nodes.length) {
                    break;
                }
                child = nodes[childIndex];
                if(child != null) {
                    writer.write(",");
                }
            }
            if(nodes[0] != null) {
                writer.write("]");
            }

            writer.write("}");
        }

        // TODO: Optimize function that optimizes the tree? Probably unnecessary for the foreseeable future, but if
        // for some reason this needs to build a huge tree, may be useful.
    }

    private void addNodeToMap(TagTreeNode node) {
        nodesMap.put(node.name, node);
        int index = 0;
        TagTreeNode child = node.nodes[index];
        while(child != null) {
            addNodeToMap(child);
            index = index + 1;
            child = node.nodes[index];
        }
    }

    private void addNodeParents(TagTreeNode node, int depth) {
        int index = 0;
        TagTreeNode child = node.nodes[index];
        while(child != null) {
            child.parent = node;
            addNodeParents(child, depth + 1);
            index = index + 1;
            child = node.nodes[index];
        }
    }

    public String getParentName(String name) {
        if(name.equals("root")) {
            return null;
        }
        TagTreeNode parent = nodesMap.get(name).parent;
        if(parent == null) {
            return null;
        }
        return parent.name;
    }

    private String[] getTreePath(String tag) {
        List<String> path1 = new ArrayList<>();
        while(tag != null) {
            path1.add(tag);
            tag = getParentName(tag);
        }
        String[] result = new String[path1.size()];
        for(int i = 0; i < result.length; i++) {
            result[i] = path1.get(result.length-1-i);
        }
        return result;
    }

    public int getTreeDistance(String tag1, String tag2) {
        String[] tag1Path = getTreePath(tag1);
        String[] tag2Path = getTreePath(tag2);
        int minLength = Math.min(tag1Path.length, tag2Path.length);
        int length = tag1Path.length+tag2Path.length;

        int i = 0;
        while(i < minLength && tag1Path[i].equals(tag2Path[i])) {
            length -= 2;
            i++;
        }

        return length;
    }

    private TagTreeNode root;
    private Map<String, TagTreeNode> nodesMap;

    public TagTree(File file) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
        try {
            String line;
            while((line = br.readLine()) != null) {
                String[] values = line.split("\\s+");
                // int tagIndex = Integer.parseInt(values[1]);
                TagTreeNode newNode = new TagTreeNode(values[2], DEFAULT_NUM_CHILDREN);

                // TODO: This is a gross hack that ignores whether or not the first node is actually the root. Maybe
                // fix this at some point.
                if(root == null) {
                    root = newNode;
                    continue;
                }

                // TODO: Like noted above, this code is assuming the tree structure is in a particular order so all
                // nodes are created correctly. In the future, it would be good if either this assumption can be
                // relaxed or the input text can be specified to always be in such an order.
                TagTreeNode nodeToAddTo = root;
                String[] nodeIndices = values[0].split(":");
                int nodeIndex = Integer.parseInt(nodeIndices[1]);
                for(int i = 2; i < nodeIndices.length; i++) {
                    nodeToAddTo = nodeToAddTo.nodes[nodeIndex];
                    nodeIndex = Integer.parseInt(nodeIndices[i]);
                }

                nodeToAddTo.addNode(nodeIndex,newNode);
            }

        } finally {
            br.close();
        }
        addNodeParents(root, 0 );
        nodesMap = new HashMap<>();
        addNodeToMap(root);
        root.parent = null;
    }

    public void generateJSON(String fileName) throws IOException {
        BufferedWriter out = new BufferedWriter(new FileWriter(fileName));
        root.printTree(out, null);
        out.close();
    }

    public static void main(String[] args) throws IOException {
        TagTree tagTree = new TagTree(new File("so2016p5000w300_iter500_tree.txt"));
        tagTree.generateJSON("TagTreeVisualization/tree.json");
    }
}
