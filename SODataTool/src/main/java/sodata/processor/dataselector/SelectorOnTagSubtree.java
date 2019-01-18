package sodata.processor.dataselector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.database.DbTagSelectorUtils;
import sodata.database.DbUtils;

public class  SelectorOnTagSubtree {
    private static Logger logger = LoggerFactory.getLogger(SelectorOnTagSubtree.class);
    
    private String inTablePrefix;
    private String outTablePrefixPart;
    private String tagSynonymTreeFilename;
    private String treeNodePrefix;
    private int treeBranchDepth;
    private int maxNumTagsToExtract;
    private String dbPropertiesFilename;
    
    private HashMap<String, TagSynonyms> tagMap;
    private boolean counted;
    private Set<String> tagSet;
    
    public SelectorOnTagSubtree(
            String inTablePrefix, 
            String outTablePrefixPart,
            String tagSynonymTreeFilename,
            String treeNodePrefix,
            int treeBranchDepth,
            int maxNumTagsToExtract,
            String dbPropertiesFilename
            ) {
        this.inTablePrefix = inTablePrefix;
        this.outTablePrefixPart = outTablePrefixPart; 
        this.tagSynonymTreeFilename = tagSynonymTreeFilename;
        this.treeNodePrefix = treeNodePrefix;
        this.treeBranchDepth = treeBranchDepth;
        this.maxNumTagsToExtract = maxNumTagsToExtract;
        this.dbPropertiesFilename = dbPropertiesFilename;
        
        DbUtils.setWorkingTablePrefix(this.inTablePrefix);
        DbTagSelectorUtils.setWorkingTablePrefix(this.inTablePrefix + this.outTablePrefixPart);        
        
        tagMap = new HashMap<String, TagSynonyms>();
        counted = false;
        tagSet = new HashSet<String>();
    }
    
    public static void main(String[] args) {
        logger.info("Working Directory = " + System.getProperty("user.dir"));
        String tablePrefix = "wk17m1_";
        String tableMiddle = "BR0_D1_";
        String treeFile = "../SOTagSynonyms/tree_synonyms2.txt";
        String nodePrefix = "0:";
        String dbPropertiesFilename = "sodumpdb54.properties";
        int treeDepth = 1;
        int maxNumTags = 1000;
        int maxPostsForTags = 500;
        boolean remakeTagTableFromPosts = true;
                
        if (args.length == 8) {
            tablePrefix = args[0];
            tableMiddle = args[1];
            treeFile = args[2];
            nodePrefix = args[3];
            dbPropertiesFilename = args[4];
            treeDepth = Integer.parseInt(args[5]);
            maxNumTags = Integer.parseInt(args[6]);
            maxPostsForTags = Integer.parseInt(args[7]);
        }
        logger.info(
                "tablePrefix,tableMiddle,treeFile,nodePrefix,dbPropertiesFilename,treeDepth,maxNumTags,maxPostsForTags: "
                + tablePrefix + "," 
                + tableMiddle + "," 
                + treeFile + "," 
                + nodePrefix + "," 
                + dbPropertiesFilename + "," 
                + treeDepth + ","
                + maxNumTags + ","
                + maxPostsForTags);
               
        SelectorOnTagSubtree selector = new SelectorOnTagSubtree(tablePrefix, tableMiddle,
                treeFile, nodePrefix, treeDepth, maxNumTags, dbPropertiesFilename);
        
        if (!selector.getTagsFromTagTree()) {
            logger.info("failed to retrieve tag list.");
            return;
        }
        logger.info("got the tag list.");
        
        Set<String> qualifiedTagSet = selector.selectTags(maxPostsForTags);
        logger.info("Selected " + qualifiedTagSet.size() + " tags.");
        
        if (!selector.makeTables(remakeTagTableFromPosts, qualifiedTagSet)) {
            logger.error("Failed to do the work.");
            return;
        }
        logger.info("Succeeded in doing the work.");
    }
    
    

    private Set<String> selectTags(int maxPostsForTags) {
        if (tagSet.isEmpty() && !getTagsFromTagTree()) {
            logger.error("Failed to load tags from tag free.");
            return null;
        }
        
        if (!counted && !getTagQuestionCounts()) {
            logger.error("Failed to laod tags from database.");
            return null;
        }
                
        // TODO: may keep the tag, but only extract max Number of posts? 
        TagSynonyms tagSynonyms;
        HashMap<String, Integer> tagPostCountMap;
        Set<String> tagSet = new HashSet<String>();
        for(Entry<String, TagSynonyms> mapEntry: tagMap.entrySet()) {
            tagSynonyms = mapEntry.getValue();
            tagPostCountMap = tagSynonyms.getTagPostCountMap();
            boolean selected = true;
            for(Entry<String, Integer> innerMapEntry: tagPostCountMap.entrySet()) {
                if (innerMapEntry.getValue() > maxPostsForTags) {
                    selected = false;
                    logger.debug("Skipped " + tagPostCountMap.entrySet().toString());
                    break;
                }
            }
            if (selected) {
                logger.debug("adding " + tagPostCountMap.entrySet().toString());
                for(Entry<String, Integer> innerMapEntry: tagPostCountMap.entrySet()) {
                    tagSet.add(innerMapEntry.getKey());
                }            
            }
        }
        return tagSet;
    }

    public boolean getTagsFromTagTree() {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(this.tagSynonymTreeFilename), StandardCharsets.UTF_8))) {
            String line;
            
            while ((line = br.readLine()) != null && tagSet != null && tagSet.size() <= this.maxNumTagsToExtract) {
                String[] branchFields = line.trim().split("\\p{javaWhitespace}");
                String treeBranch = branchFields[0];
                String tagNode = branchFields[branchFields.length-1];
                
                if (treeBranch.startsWith(this.treeNodePrefix) && countTreeHeight(treeBranch) >= this.treeBranchDepth) {
                    logger.trace("Tree Branch: " + treeBranch + " Tags: " + tagNode);
                    String[] tags = tagNode.split(":");
                    if (tags.length<2) {
                        logger.trace("No symnonym in " + tagNode + ", skip ...");
                        continue;
                    }
                    TagSynonyms tagSynonyms = new TagSynonyms(tags);
                    for(String aTag: tags) {
                        tagSet.add(aTag);
                        tagMap.put(aTag,  tagSynonyms);
                        logger.debug("Added " + aTag);
                    }
                    
                    if (tagSet.size() >= this.maxNumTagsToExtract) {
                        logger.info("Maximum tags: " + tagSet.size() + " reached.");
                    }
                }
            }
            logger.info(tagSet.size() + " tags extracted.");
             
            return true;
        } catch (FileNotFoundException e) {
            logger.error("Cannot open " + this.tagSynonymTreeFilename, e);
            return false;
        } catch (IOException e) {
            logger.error("File I/O error on " + this.tagSynonymTreeFilename, e);
            return false;
        }
    }
    
    
    public boolean getTagQuestionCounts() {
        Connection conn = null;
        TagSynonyms tagSynonyms = null;

        try {
            if ((conn = DbUtils.connect(this.dbPropertiesFilename)) == null) {
                logger.info("Failed to establish database connection.");
                return false;
            }
            logger.info("Estabished database connection");

            // Begin transaction
            conn.setAutoCommit(false);

            // may need index
            if (!completeTempTagnameTable(conn, tagSet)) {
                logger.info("Failed to build temp tagname table.");
                return false;
            }
            logger.info("Built temp tagname table.");
            
            String sql = DbTagSelectorUtils.getSqlQueryTagQuestionCounts();
            try (PreparedStatement pstmt = conn.prepareStatement(sql);
                    ResultSet rs = pstmt.executeQuery()) {
                while(rs.next()) {
                    String tagName = rs.getString(1);
                    int nPosts = rs.getInt(2);
                    logger.trace("TagId,TagName,NumPosts: " + tagName + "," + nPosts);
                    tagSynonyms = tagMap.get(tagName);
                    if (tagSynonyms == null) {
                        logger.warn("Tag " + tagName + " not found in tag synonyms.");
                    } else {
                        if (!tagSynonyms.setValue(tagName, nPosts)) {
                            logger.warn("Failed to update TagSynonyms object for " + tagName);
                        }
                    }
                }
            } catch (SQLException e) {
                logger.error("Query error: " + sql, e);
                return false;
            }

            counted = true;
            conn.commit();
        } catch (SQLException e) {
            logger.error("Encounter sql error.", e);
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    logger.error("Rollback db failure.", e);
                }
            }
        }
        
        return true;
    }


    private int countTreeHeight(String treeBranch) {
        return treeBranch.split(":").length;
    }

    private boolean makeTables(boolean remakeTagTableFromPosts, Set<String> tagSet) {
        Connection conn = null;
    
        try {
            if ((conn = DbUtils.connect(this.dbPropertiesFilename)) == null) {
                logger.info("Failed to establish database connection.");
                return false;
            }
            logger.info("Estabished database connection");
        
            // Begin transaction
            conn.setAutoCommit(false);        
            
            // may need index
            if (!completeTempTagnameTable(conn, tagSet)) {
                logger.info("Failed to build temp tagname table.");
                return false;
            }
            logger.info("Built temp tagname table.");
            
            // may need index
            if (!completeTagTable(conn)) {
                logger.info("Failed to build tag table.");
                return false;                
            }
            logger.info("Built tag table.");

            
            // may need index
            if (!completeQuestionTagTable(conn)) {
                logger.info("Failed to question tag table.");
                return false;                    
            }
            logger.info("Built question tag table.");
            

            // may need index
            if (!completeQuestionIdTable(conn)) {
                logger.info("Failed to question id table.");
                return false;
            }
            logger.info("Built question id table.");

            if (remakeTagTableFromPosts) {
                // may need index
                if (!completeTagTableFromQuestionTable(conn)) {
                    logger.info("Failed to remake tag table from selected posts.");
                    return false;
                }
                logger.info("Rebuilt tag table from selected posts.");
            } else {
                logger.info("Does not remake tag table from selected posts.");
            }
            
            
            
            // may need index
            if (!completeQuestionWordTable(conn)) {
                logger.info("Failed to question word table.");
                return false;                
            }
            logger.info("Built question word table.");
                
            // may need index
            if (!completeTempWordIdTable(conn)) {
                logger.info("Failed to complete temp word id table.");
                return false;                
            }
            logger.info("Succeeded in completing temp word id table.");
            
            if (!completeVocabularyTable(conn)) {
                logger.info("Failed to complete vocabulary table.");
                return false;                
            }
            logger.info("Succeeded in completing vocabulary table.");
            
            if (!createVocabularyTableIndicesAndTriggers(conn)) {
                logger.info("Cannot create indices and triggers for the vocabulary table.");
                return false;
            }
            logger.info("Create indices and triggers for the vocabulary table.");

            if (!createQuestionWordTableIndices(conn)) {
                logger.info("Cannot index the working question-word table.");
                return false;
            }
            logger.info("Created indices for working question-word table.");            
            
            
            // End transaction 
            conn.commit();
            
            return true;
        } catch (SQLException e) {
            logger.error("SQL Error.", e);
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.rollback();
                    conn.close();
                } catch (SQLException e) {
                    logger.error("Failed to release database connection resources.", e);
                }
            }
        }
    }
    
    
    
    
    private boolean completeTagTableFromQuestionTable(Connection conn) {
        String sql;
        if (DbUtils.tableExists(conn, DbTagSelectorUtils.getTagTable())) {
            if (!DbUtils.dropTable(conn, DbTagSelectorUtils.getTagTable())) {
                return false;
            }
        }
        
        sql = DbTagSelectorUtils.getSqlMkTagTableFromSelectOnPostTable();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("Run sql error.", e);
            return false;
        }
        
        return true;
    }

    public boolean dispTagQuestionCounts() {
        if (tagSet.isEmpty() && !getTagsFromTagTree()) {
            logger.error("Failed to load tags from tag free.");
            return false;
        }
        
        if (!counted && !getTagQuestionCounts()) {
            logger.error("Failed to laod tags from database.");
            return false;
        }
        
        TagSynonyms tagSynonyms = null;
        for(String aTag: tagSet) {
            tagSynonyms = tagMap.get(aTag);
            logger.info("Tag,PostCounts:" + aTag + "," + tagSynonyms.getValue(aTag));
        }
        return true;
    }   

    private boolean createQuestionWordTableIndices(Connection conn) {
        return DbUtils.createTable(conn, null, DbTagSelectorUtils.getSqlMkVocabTblTrigger(), DbTagSelectorUtils.getSqlMkVocabTblIndices());   
    }

    private boolean createVocabularyTableIndicesAndTriggers(Connection conn) {
        return DbUtils.createTable(conn, null, null, DbTagSelectorUtils.getSqlMkQwTblIndices());
    }

    private boolean completeTempTagnameTable(Connection conn, Set<String> tagList) {
        if (!createTempTagnameTable(conn)) return false;
        if (!populateTempTagnameTable(conn, tagList)) return false;
        return true;
    }

    
    private boolean completeTempWordIdTable(Connection conn) {
        if (!createTempWordIdTable(conn)) return false;
        if (!populateTempWordIdTable(conn)) return false;
        return true;        
    }
    
    private boolean createTempTagnameTable(Connection conn) {
        return DbUtils.createTable(conn, DbTagSelectorUtils.getSqlMkTmpTagnameTable(), null, null);
    }
    
    private boolean createTempWordIdTable(Connection conn) {
        return DbUtils.createTable(conn, DbTagSelectorUtils.getSqlMkTmpWordIdTable(), null, null);
    }
    
    private boolean populateTempTagnameTable(Connection conn, Set<String> tagList) {
        try {
            String tmpTagnameFilename = "tmpfile_tagname.csv";
            File tagnameFile = new File(tmpTagnameFilename);
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tagnameFile), StandardCharsets.UTF_8));
            
            for(String tag: tagList) {
                writer.println(tag);
            }
            writer.close();
            
            CopyManager copyManager = new CopyManager((BaseConnection)conn);
            logger.info("Copying " + tmpTagnameFilename + " " + DbTagSelectorUtils.getTmpTagnameTable());
            InputStreamReader fileStreamReader = new InputStreamReader(new FileInputStream(tmpTagnameFilename), StandardCharsets.UTF_8);
            copyManager.copyIn("COPY " + DbTagSelectorUtils.getTmpTagnameTable() + " FROM STDIN WITH CSV", fileStreamReader);
            
            tagnameFile.delete();
            
            return true;
        } catch (FileNotFoundException e) {
            logger.error("Error on tmpfile_tagname.csv", e);
        } catch (SQLException e) {
            logger.error("SQL error", e);
        } catch (IOException e) {
            logger.error("I/O error", e);
        }        
        return false;
    }
    
    private boolean populateTempWordIdTable(Connection conn) {
        try {
            String tmpWordIdFilename = "tmpfile_wordid.csv";
            File tmpWordIdFile = new File(tmpWordIdFilename);
            FileWriter fileWriter = new FileWriter(tmpWordIdFile);
            
            CopyManager copyManager = new CopyManager((BaseConnection)conn);
            logger.info("Copying distinct word id from " + DbTagSelectorUtils.getQuestionWordTable() + " to CSV file " + tmpWordIdFilename);
            copyManager.copyOut("COPY (" + DbTagSelectorUtils.getSqlQueryDistinctWordIdFromQuestionWordTable() + ") TO STDOUT WITH CSV", fileWriter);
            fileWriter.close();

            
            logger.info("Copying " + tmpWordIdFilename + " " + DbTagSelectorUtils.getTmpWordIdTable());
            InputStreamReader fileStreamReader = new InputStreamReader(new FileInputStream(tmpWordIdFilename), StandardCharsets.UTF_8);
            copyManager.copyIn("COPY " + DbTagSelectorUtils.getTmpWordIdTable() + " FROM STDIN WITH CSV", fileStreamReader);
            
            tmpWordIdFile.delete();
            
            logger.info("populated temp word id table.");
            return true;
        } catch (FileNotFoundException e) {
            logger.error("Error on tmpfile_wordid.csv", e);
        } catch (SQLException e) {
            logger.error("SQL error", e);
        } catch (IOException e) {
            logger.error("I/O error", e);
        }        
        return false;
    }    

    
    private boolean completeTagTable(Connection conn) {
        logger.debug("Building tag table ...");
        String sql = DbTagSelectorUtils.getSqlMkTagTableFromSelect();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQL error.", e);
            return false;
        }
        logger.debug("writing debug information ...");
        sql = DbTagSelectorUtils.getSqlQueryWantedTagsNotInTagTable();
        try (PreparedStatement pstmt = conn.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                logger.debug("Wanted Tag not in Tag Table " + DbTagSelectorUtils.getTagTable() + ": " + rs.getString(1).trim());
            }
            logger.debug("Done outputing wanted tags not in tag table.");
        } catch (SQLException e) {
            logger.error("SQL error.", e);
        }
        return true;
    }


    private boolean completeQuestionTagTable(Connection conn) {
        String sql = DbTagSelectorUtils.getSqlMkQuestionTagTableFromSelect();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            logger.error("SQL error.", e);
        }
        return false;
    }
    

    private boolean completeQuestionIdTable(Connection conn) {
        String sql = DbTagSelectorUtils.getSqlMkQuestionIdTableFromSelect();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            logger.error("SQL error.", e);
        }
        return false;
    }    
    
    private boolean completeQuestionWordTable(Connection conn) {
        if (!createQuestionWordTable(conn)) return false;
        if (!populateQuestionWordTable(conn)) return false;
        return true;    
    }
    

    private boolean populateQuestionWordTable(Connection conn) {
        try {
            String tmpQuestionWordFilename = "tmpfile_questionword.csv";
            File tmpQuestionWordFile = new File(tmpQuestionWordFilename);
            FileWriter fileWriter = new FileWriter(tmpQuestionWordFile);
            
            CopyManager copyManager = new CopyManager((BaseConnection)conn);
            logger.info("Copying question-word tuple from " + DbUtils.getQuestionWordTable() + " to CSV file " + tmpQuestionWordFile);
            copyManager.copyOut("COPY (" + DbTagSelectorUtils.getSqlQueryQuestionWordTable() + ") TO STDOUT WITH CSV", fileWriter);
            fileWriter.close();

            
            logger.info("Copying " + tmpQuestionWordFile + " " + DbTagSelectorUtils.getQuestionWordTable());
            InputStreamReader fileStreamReader = new InputStreamReader(new FileInputStream(tmpQuestionWordFile), StandardCharsets.UTF_8);
            copyManager.copyIn("COPY " + DbTagSelectorUtils.getQuestionWordTable() + " FROM STDIN WITH CSV", fileStreamReader);

            tmpQuestionWordFile.delete();

            return true;
        } catch (FileNotFoundException e) {
            logger.error("Error on tmpfile_wordid.csv", e);
        } catch (SQLException e) {
            logger.error("SQL error", e);
        } catch (IOException e) {
            logger.error("I/O error", e);
        }        
        return false;
    }

    private boolean createQuestionWordTable(Connection conn) {
        return DbUtils.createTable(conn, DbTagSelectorUtils.getSqlMkQuestionWordTable(), null, null);
    }

    private boolean completeVocabularyTable(Connection conn) {
        String sql = DbTagSelectorUtils.getSqlMkSimpleVocabularyTableFromSelect();
        logger.debug("SQL: " + sql);
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            logger.error("SQL error.", e);
        }
        return false;    
    }    
}
