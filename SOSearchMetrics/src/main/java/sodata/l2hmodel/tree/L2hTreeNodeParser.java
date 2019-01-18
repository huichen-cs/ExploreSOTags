package sodata.l2hmodel.tree;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class L2hTreeNodeParser {
    private static Logger LOGGER = LoggerFactory.getLogger(L2hTreeNodeParser.class);
    
    private final static String NODE_LINE_PATTERN = "^\\s*\\[([^\\[\\]]*)\\],.*\\[([^\\[\\]]*)\\]$";
    private final static String NODE_PATTERN = "\\[([^\\[\\]]*)\\], ";
    private final static String LABEL_PATTERN = "\\],([^\\[\\]]*)\\[";
    private final static String TOP_WORD_PATTERN = "\\[([^\\[\\]]*)\\]$";
    
    private Pattern nodeLinePattern;
    private Pattern nodePattern;
    private Pattern labelPattern;
    private Pattern topWordPattern;

    public L2hTreeNodeParser() {
        nodeLinePattern = Pattern.compile(NODE_LINE_PATTERN, Pattern.MULTILINE);
        nodePattern = Pattern.compile(NODE_PATTERN, Pattern.MULTILINE);
        labelPattern = Pattern.compile(LABEL_PATTERN, Pattern.MULTILINE);
        topWordPattern = Pattern.compile(TOP_WORD_PATTERN, Pattern.MULTILINE);
    }

    public boolean isNodeLine(String text) {
        if (text == null) return false;
        Matcher matcher = nodeLinePattern.matcher(text);
        return matcher.matches();
    }

    public L2hTreeNode parse(String text) {
        /*
        str.append(node.toString()).append(", ")
            .append(getLabelString(node.id))
            .append(" ").append(node.getContent() == null ? "" : node.getContent().getCountSum())
            .append(" ").append(topWords == null ? "" : Arrays.toString(topWords))
            .append("\n\n");
        */
        L2hTreeNode node = new L2hTreeNode();

        int level = 0;
        while (level < text.length() && Character.isWhitespace(text.charAt(level))) {
            level++;
        }
        node.setLevel(level);
        LOGGER.debug("level = " + level);

        LOGGER.debug("Remain text to parse: " + text);
        Matcher matcher = nodePattern.matcher(text);
        if (!matcher.find()) {
            throw new IllegalArgumentException("The text doesn't contain a valid node text: " + text);
        }
        String nodeText = matcher.group(1);
        parseNode(nodeText, node);

        matcher = labelPattern.matcher(text);
        if (!matcher.find()) {
            throw new IllegalArgumentException("The text doesn't contain a valid label text: " + text);
        }
        String labelText = matcher.group(1);
        parseLabelText(labelText, node);

        matcher = topWordPattern.matcher(text);
        if (!matcher.find()) {
            throw new IllegalArgumentException("The text doesn't contain the list of top words: " + text);
        }
        String topWordText = matcher.group(1);

        parseTopWords(topWordText, node);

        return node;
    }

    private void parseNode(String nodeText, L2hTreeNode node) {
        
        /*
                StringBuilder str = new StringBuilder();
                str.append("[")
                        .append(id).append(", ")
                        .append(getPathString())
                        .append(", #c = ").append(getChildren().size())
                        .append(", #o = ").append(getContent().getCountSum())
                        .append("]");
                return str.toString();
        */
        LOGGER.debug("node text: " + nodeText);
        
        String[] parts = nodeText.split(",");
        if (parts.length != 4) {
            throw new IllegalArgumentException("The text isn't a valid node text: " + nodeText);
        }
        int id = Integer.parseInt(parts[0].trim());
        
        String pathString = parts[1].trim();
        
        String[] subParts = parts[2].split("=");
        if (subParts.length != 2) {
            throw new IllegalArgumentException("The text isn't a valid node text: " + nodeText);
        }
        int numChildren = Integer.parseInt(subParts[1].trim());
        
        subParts = parts[3].split("=");
        if (subParts.length != 2) {
            throw new IllegalArgumentException("The text isn't a valid node text: " + nodeText);
        }
        long countSum = Long.parseLong(subParts[1].trim());
        
        node.setId(id);
        node.setPathString(pathString);
        node.setNumChildren(numChildren);
        if (node.getCountSum() >= 0 && node.getCountSum() != countSum) {
            throw new IllegalArgumentException("The text isn't a valid node text: " + nodeText);
        }
        node.setCountSum(countSum);
    }
    
    
    
    private void parseLabelText(String labelText, L2hTreeNode node) {
        /*
        protected String getLabelString(int labelIdx) {
            return this.labelVocab.get(labelIdx) + " (" + this.labelFreqs[labelIdx] + ")";
        }
        
        ....append(" ").append(node.getContent() == null ? "" : node.getContent().getCountSum())
        */
        LOGGER.debug("label text: " + labelText);
        
        String[] parts = labelText.trim().split("\\s+");
        if (parts.length < 2) {
            throw new IllegalArgumentException("The text isn't a valid label text: " + labelText);
        }
        String label = parts[0].trim();

        int labelFrequency = Integer.parseInt(parts[1].substring(1, parts[1].length()-1));
        
        long countSum = 0;
        if (parts.length >= 3) {
            countSum = Long.parseLong(parts[2].trim());
        }
        
        node.setLabel(label);
        node.setLabelFrequency(labelFrequency);
        if (node.getCountSum() >= 0 && node.getCountSum() != countSum) {
            throw new IllegalArgumentException("The text isn't a valid label text: " + labelText);
        }
        node.setCountSum(countSum);
    }


    private void parseTopWords(String topWordText, L2hTreeNode node) {
        LOGGER.debug("top word text: " + topWordText);

        String[] words = topWordText.split(",");
        for (int i = 0; i < words.length; i++) {
            words[i] = words[i].trim();
        }
        node.setTopWords(words);
    }

}
