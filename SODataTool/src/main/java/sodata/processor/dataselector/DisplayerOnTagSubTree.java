package sodata.processor.dataselector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisplayerOnTagSubTree {
    private static Logger logger = LoggerFactory.getLogger(SelectorOnTagSubtree.class);
    
    public static void main(String[] args) {
        logger.info("Working Directory = " + System.getProperty("user.dir"));
        String tablePrefix = "wk17_";
        String tableMiddle = "BR0_D2_";
        String treeFile = "../SOTagSynonyms/tree_synonyms2.txt";
        String nodePrefix = "0:";
        String dbPropertiesFilename = "sodumpdb181.properties";
        int treeDepth = 2;
        int maxNumTags = 800;
                
        if (args.length == 7) {
            tablePrefix = args[0];
            tableMiddle = args[1];
            treeFile = args[2];
            nodePrefix = args[3];
            dbPropertiesFilename = args[4];
            treeDepth = Integer.parseInt(args[5]);
            maxNumTags = Integer.parseInt(args[6]);
        }
        logger.info("tablePrefix,tableMiddle,treeFile,nodePrefix,dbPropertiesFilename,treeDepth,maxNumTags: "
                + tablePrefix + "," 
                + tableMiddle + "," 
                + treeFile + "," 
                + nodePrefix + "," 
                + dbPropertiesFilename + "," 
                + treeDepth + ","
                + maxNumTags);
               
        SelectorOnTagSubtree selector = new SelectorOnTagSubtree(tablePrefix, tableMiddle,
                treeFile, nodePrefix, treeDepth, maxNumTags, dbPropertiesFilename);
        
        selector.dispTagQuestionCounts();
        logger.info("The work is done.");
    }
    
}
