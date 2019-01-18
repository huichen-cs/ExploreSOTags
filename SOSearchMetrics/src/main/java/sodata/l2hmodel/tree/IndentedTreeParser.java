package sodata.l2hmodel.tree;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndentedTreeParser {
    private final static Logger LOGGER = LoggerFactory.getLogger(IndentedTreeParser.class);
    private String line;

    public L2hTree parseAll(InputStream in) throws IOException {
        L2hTreeStatParser statParser = new L2hTreeStatParser();
        
        L2hTreeStat stat = new L2hTreeStat();
        L2hTreeNode root = null;
        L2hTreeNodeParser parser = new L2hTreeNodeParser();

        line = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty())
                    continue;
                
                LOGGER.debug("Line: " + line);
                if (parser.isNodeLine(line)) {
                    root = parseTree(reader);
                } 
                
                LOGGER.debug("Line: " + line);
                if (line != null && !parser.isNodeLine(line)) {
                    stat = statParser.parse(reader, stat, line);
                }
            }
        }
        
        return new L2hTree(root, stat);
    }

    public L2hTree parseAll(Path path) throws IOException {
        return parseAll(Files.newInputStream(path));
    }



    public L2hTreeNode parseTree(InputStream in) throws IOException {
        L2hTreeNode root = null;
        L2hTreeNodeParser parser = new L2hTreeNodeParser();
        int prevLevel = -1;

        // the indented text tree is essentially the result of a depth first search, use
        // a stack
        Stack<L2hTreeNode> stack = new Stack<L2hTreeNode>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty())
                    continue;
                
                LOGGER.debug("Line: " + line);
                if (!parser.isNodeLine(line)) {
                    continue;
                } else {
                    L2hTreeNode node = parser.parse(line);

                    // root
                    if (node.getLevel() == 0 && node != null) {
                        if (node.getLevel() != 0) {
                            throw new IllegalStateException("The tree isn't in a recognizable format.");
                        }
                    }
                    if (root == null) {
                        if (node.getLevel() != 0) {
                            throw new IllegalStateException("The tree isn't in a recognizable format.");
                        }
                        root = node;
                        prevLevel = node.getLevel();
                        stack.push(node);
                    } else {
                        if (node.getLevel() > prevLevel) {
                            stack.push(node);
                            prevLevel = node.getLevel();
                        } else if (node.getLevel() <= prevLevel) {
                            L2hTreeNode parentNode = stack.pop();
                            while (parentNode.getLevel() >= node.getLevel()) { // hasn't reached the parent node yet
                                L2hTreeNode childNode = parentNode;
                                parentNode = stack.pop();
                                parentNode.getChildren().add(childNode); // add first?
                                childNode.setParent(parentNode);
                                parentNode.setNumChildren(parentNode.getNumChildren() + 1);
                            }
                            stack.push(parentNode); // top of the stack is always the parent
                            stack.push(node);
                            prevLevel = node.getLevel();
                        }
                    }
                }
            }
            L2hTreeNode childNode = stack.pop();
            while (!stack.isEmpty()) {
                L2hTreeNode parentNode = stack.pop();
                parentNode.getChildren().add(childNode);
                parentNode.setNumChildren(parentNode.getNumChildren() + 1);
                childNode.setParent(parentNode);
                childNode = parentNode;
            }
        }
        return root;
    }
    
    public L2hTreeNode parseTree(Path path) throws IOException {
        return parseTree(Files.newInputStream(path));
    }
    

    private L2hTreeNode parseTree(BufferedReader reader) throws IOException {
        L2hTreeNode root = null;
        L2hTreeNodeParser parser = new L2hTreeNodeParser();
        int prevLevel = -1;

        // the indented text tree is essentially the result of a depth first search, use a stack
        Stack<L2hTreeNode> stack = new Stack<L2hTreeNode>();
        do {
            if (line.isEmpty())
                continue;
            
            if (!parser.isNodeLine(line)) {
                break;
            }
            
            LOGGER.debug("Line: " + line);
            
            L2hTreeNode node = parser.parse(line);

            // root
            if (node.getLevel() == 0 && node != null) {
                if (node.getLevel() != 0) {
                    throw new IllegalStateException("The tree isn't in a recognizable format.");
                }
            }
            if (root == null) {
                if (node.getLevel() != 0) {
                    throw new IllegalStateException("The tree isn't in a recognizable format.");
                }
                root = node;
                prevLevel = node.getLevel();
                stack.push(node);
            } else {
                if (node.getLevel() > prevLevel) {
                    stack.push(node);
                    prevLevel = node.getLevel();
                } else if (node.getLevel() <= prevLevel) {
                    L2hTreeNode parentNode = stack.pop();
                    while (parentNode.getLevel() >= node.getLevel()) { // hasn't reached the parent node yet
                        L2hTreeNode childNode = parentNode;
                        parentNode = stack.pop();
                        parentNode.getChildren().add(childNode); // add first?
                        childNode.setParent(parentNode);
                        parentNode.setNumChildren(parentNode.getNumChildren() + 1);
                    }
                    stack.push(parentNode); // top of the stack is always the parent
                    stack.push(node);
                    prevLevel = node.getLevel();
                }
            }

        } while ((line = reader.readLine()) != null);
        L2hTreeNode childNode = stack.pop();
        while (!stack.isEmpty()) {
            L2hTreeNode parentNode = stack.pop();
            parentNode.getChildren().add(childNode);
            parentNode.setNumChildren(parentNode.getNumChildren() + 1);
            childNode.setParent(parentNode);
            childNode = parentNode;
        }
        return root;
    }
}
