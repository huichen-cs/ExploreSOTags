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
import java.util.Map;
import java.util.Set;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.database.DbTagSelectorUtils;
import sodata.database.DbUtils;

public class SelectorOnTagList {
    private final static Logger LOGGER = LoggerFactory.getLogger(SelectorOnTagSubtree.class);
    
    private String inTablePrefix;
    private String outTablePrefixPart;
    private String tagListFilename;
    
    public SelectorOnTagList(
            String inTablePrefix, 
            String outTablePrefixPart,
            String tagListFilename
            ) {
        this.inTablePrefix = inTablePrefix;
        this.outTablePrefixPart = outTablePrefixPart; 
        this.tagListFilename = tagListFilename;
        
        DbUtils.setWorkingTablePrefix(this.inTablePrefix);
        DbTagSelectorUtils.setWorkingTablePrefix(this.inTablePrefix + this.outTablePrefixPart);     
    }
    

    public void dispTagQuestionCounts(Map<String, Integer> tagPostCountMap) {
        tagPostCountMap.forEach((tag, count) -> 
            LOGGER.info("Tag,PostCounts:" + tag + "," + count));
    }   

    public static void main(String[] args) {
        LOGGER.info("Working Directory = " + System.getProperty("user.dir"));
        String tablePrefix = "wk17m1_";
        String tableMiddle = "android_";
        String tagListFilename = "../SOAndroid/post_tags_withandroid.csv";
        String dbPropertiesFilename = "sodumpdb181.properties";
        String anchorTagName = "android";
        int minPostsForTags = 100;
        int maxPostsToExtractForTags = 5000;
        int minCoOccurPosts = 500;
        boolean remakeTagTableFromPosts = true;
                
        if (args.length >= 7) {
            tablePrefix = args[0];
            tableMiddle = args[1];
            tagListFilename = args[2];
            dbPropertiesFilename = args[3];
            minPostsForTags = Integer.parseInt(args[4]);
            anchorTagName = args[5];
            minCoOccurPosts = Integer.parseInt(args[6]);
        }
        if (args.length >= 8) {
            maxPostsToExtractForTags = Integer.parseInt(args[7]);
        }
        
        LOGGER.info(
                "tablePrefix,tableMiddle,treeFile,dbPropertiesFilename,minPostsForTags,anchorTag,minCoOccurPosts,maxPostsToExtractForTags: "
                + tablePrefix + "," 
                + tableMiddle + "," 
                + tagListFilename + "," 
                + dbPropertiesFilename + "," 
                + minPostsForTags + "," // any tags whose post count are less than than this are removed
                + anchorTagName + ","
                + minCoOccurPosts + "," // only tags who co-occur on a post with anchorTagName at least in minCoOccurPosts are included
                + maxPostsToExtractForTags // for each tag, only extract randomly maxPostsToExtractForTags posts
                ); 
               
        SelectorOnTagList selector = new SelectorOnTagList(tablePrefix, tableMiddle, tagListFilename);
        
        Connection conn = null;
        try {
            if ((conn = DbUtils.connect(dbPropertiesFilename)) == null) {
                LOGGER.info("Failed to establish database connection.");
                return;
            }
            LOGGER.info("Estabished database connection");

            // Begin transaction
            conn.setAutoCommit(false);
            
            Set<String> tagSet = selector.getTagsFromTagListFile();
            if (tagSet == null) {
                LOGGER.info("failed to retrieve tag set.");
                return;
            }
            LOGGER.info("got " + tagSet.size() + " tags from " + tagListFilename);
            
            Map<String, Integer> tagPostCountMap = selector.getTagQuestionCounts(conn, tagSet);
            LOGGER.info("counted " + tagPostCountMap.size() + " tags.");
            
            Set<String> qualifiedTagSet = selector.selectTags(tagPostCountMap, minPostsForTags);
            if (qualifiedTagSet == null) {
                LOGGER.info("failed to select qualified tags from tag set.");
                return;
            }
            LOGGER.info("Selected " + qualifiedTagSet.size() + " tags.");
            
            selector.makeTagCoOccurPostCountTable(conn, anchorTagName);
            LOGGER.info("made the " +  DbTagSelectorUtils.getTagCoOccurPostCountTable() + " table.");
            
            Set<String> qualifiedCoOccurTagSet = selector.selectTags(conn, minCoOccurPosts);
            if (qualifiedCoOccurTagSet == null) {
                LOGGER.info("failed to select qualified tags from tag set.");
                return;
            }
            qualifiedCoOccurTagSet.add(anchorTagName);
            LOGGER.info("Selected " + qualifiedCoOccurTagSet.size() + " tags.");
            
            qualifiedTagSet.retainAll(qualifiedCoOccurTagSet);
            LOGGER.info("Retained " + qualifiedTagSet.size() + " tags.");
        
            if (!selector.makeTables(conn, remakeTagTableFromPosts, qualifiedTagSet, maxPostsToExtractForTags)) {
                LOGGER.error("Failed to do the work.");
                return;
            }
            conn.commit();
            LOGGER.info("Succeeded in doing the work.");
        } catch (FileNotFoundException e) {
            LOGGER.error("Cannot open " + tagListFilename, e);
            return;
        } catch (SQLException e) {
            LOGGER.error("SQL error ", e);
            return;
        } catch (IOException e) {
            LOGGER.error("File I/O error on " + tagListFilename, e);
            return;
        } finally {
                if (conn != null) {
                    try {
                        conn.rollback();
                        conn.close();                        
                    } catch (SQLException e) {
                        LOGGER.error("SQL error ", e);
                    }
                }
        }
    }

    private Set<String> selectTags(Map<String, Integer> tagPostCountMap, int minPostsForTags) {
        Set<String> tagSetSelected = new HashSet<String>();
        
        tagPostCountMap.forEach((tag, count)-> {
            if (count <= minPostsForTags) {
                LOGGER.debug("skipped (" + tag + ", " + count + ")");
            } else {
                LOGGER.debug("added (" + tag + ", " + count + ")");
                tagSetSelected.add(tag);            }
        });
        return tagSetSelected;
    }
    
    
    private Set<String> selectTags(Connection conn, int minCoOccurPosts) throws SQLException {
        Set<String> tagSet = new HashSet<String>();

        String sql = DbTagSelectorUtils.getSqlSelectCoOccurPostCountTable(minCoOccurPosts);
        LOGGER.debug("SQL: " + sql);

        try (PreparedStatement pstmt = conn.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery()) {
            LOGGER.debug("Issued query sql: " + sql);
            while (rs.next()) {
                String tagName = rs.getString(1);
                int postCount = rs.getInt(2);
                tagSet.add(tagName);
                LOGGER.debug("(tag, postcount): (" + tagName + ", " + postCount + ")");
            }
            LOGGER.debug("Added " + tagSet.size() + " tags.");
            return tagSet;
        }
    }


    
    private boolean makeTagCoOccurPostCountTable(Connection conn, String anchorTag) throws SQLException {
        // may need index
        if (!completeTempTagsTable(conn, this.tagListFilename)) {
            LOGGER.info("Failed to build temp tagname table.");
            return false;
        }
        LOGGER.info("Built temp tagname table.");

        String sql = DbTagSelectorUtils.getSqlMkTagCoOccurPostCountTableFromSelect(anchorTag);
        LOGGER.debug("SQL: " + sql);
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            LOGGER.debug("Issued query sql: " + sql);
            pstmt.executeUpdate();
            LOGGER.debug("Made " + DbTagSelectorUtils.getTagCoOccurPostCountTable());
        }
        return true;
    }

    private Set<String> getTagsFromTagListFile() throws FileNotFoundException, IOException {
        Set<String> tagSet = new HashSet<String>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(this.tagListFilename), StandardCharsets.UTF_8))) {
            String line;
            
            while ((line = br.readLine()) != null && tagSet != null) {
                String[] fields = line.split(",");
                String aTag = fields[1].trim();
                tagSet.add(aTag);
            }
            LOGGER.info(tagSet.size() + " tags extracted.");
             
            return tagSet;
        } 
    }
    
    
    private Map<String, Integer> getTagQuestionCounts(Connection conn, Set<String> tagSet) throws SQLException {
        Map<String, Integer> tagPostCountMap = new HashMap<String, Integer>();

        // may need index
        if (!completeTempTagnameTable(conn, tagSet)) {
            LOGGER.info("Failed to build temp tagname table.");
            return null;
        }
        LOGGER.info("Built temp tagname table.");

        String sql = DbTagSelectorUtils.getSqlQueryTagQuestionCounts();
        LOGGER.debug("SQL: " + sql);
        try (PreparedStatement pstmt = conn.prepareStatement(sql); ResultSet rs = pstmt.executeQuery()) {
            LOGGER.debug("Issued query sql: " + sql);
            while (rs.next()) {
                String tagName = rs.getString(1);
                int nPosts = rs.getInt(2);
                LOGGER.trace("TagName,NumPosts: " + tagName + "," + nPosts);
                if (tagPostCountMap.get(tagName) != null) {
                    LOGGER.warn("tagPostCountMap has already had tag " + tagName);
                }
                tagPostCountMap.put(tagName, nPosts);
                LOGGER.trace("Add to tagPostCountMap (" + tagName + ", " + nPosts + ")");
            }
        }
        return tagPostCountMap;
    }

    private boolean makeTables(Connection conn, boolean remakeTagTableFromPosts, Set<String> tagSet, int maxPostsToExtractForTags)
            throws SQLException {

        // may need index
        DbUtils.dropTable(conn, DbTagSelectorUtils.getTmpTagnameTable());
        LOGGER.info("dropped " + DbTagSelectorUtils.getTmpTagnameTable());
        if (!completeTempTagnameTable(conn, tagSet)) {
            LOGGER.info("Failed to build temp tagname table.");
            return false;
        }
        LOGGER.info("Built temp tagname table.");

        // may need index
        if (!completeTagTable(conn)) {
            LOGGER.info("Failed to build tag table.");
            return false;
        }
        LOGGER.info("Built tag table.");

        // may need index
        if (!completeQuestionTagTable(conn)) {
            LOGGER.info("Failed to question tag table.");
            return false;
        }
        LOGGER.info("Built question tag table.");

        // may need index
        if (!completeQuestionIdTable(conn, maxPostsToExtractForTags)) {
            LOGGER.info("Failed to question id table.");
            return false;
        }
        LOGGER.info("Built question id table.");
        
        if (remakeTagTableFromPosts) {
            // may need index
            if (!completeQuestionTagTableFromQuestionAndTagTables(conn)) {
                LOGGER.info("Failed to remake quesiton-tag table from selected posts.");
                return false;
            }
            LOGGER.info("Rebuilt question-tag table from selected posts.");
        } else {
            LOGGER.info("Does not remake question-tag table from selected posts.");
        }

        if (remakeTagTableFromPosts) {
            // may need index
            if (!completeTagTableFromQuestionTable(conn)) {
                LOGGER.info("Failed to remake tag table from selected posts.");
                return false;
            }
            LOGGER.info("Rebuilt tag table from selected posts.");
        } else {
            LOGGER.info("Does not remake tag table from selected posts.");
        }

        // may need index
        if (!completeQuestionWordTable(conn)) {
            LOGGER.info("Failed to question word table.");
            return false;
        }
        LOGGER.info("Built question word table.");

        // may need index
        if (!completeTempWordIdTable(conn)) {
            LOGGER.info("Failed to complete temp word id table.");
            return false;
        }
        LOGGER.info("Succeeded in completing temp word id table.");

        if (!completeVocabularyTable(conn)) {
            LOGGER.info("Failed to complete vocabulary table.");
            return false;
        }
        LOGGER.info("Succeeded in completing vocabulary table.");

        if (!createVocabularyTableIndicesAndTriggers(conn)) {
            LOGGER.info("Cannot create indices and triggers for the vocabulary table.");
            return false;
        }
        LOGGER.info("Create indices and triggers for the vocabulary table.");

        if (!createQuestionWordTableIndices(conn)) {
            LOGGER.info("Cannot index the working question-word table.");
            return false;
        }
        LOGGER.info("Created indices for working question-word table.");
        return true;
    }
    
    private boolean completeQuestionTagTableFromQuestionAndTagTables(Connection conn) {
        DbUtils.dropTable(conn, DbTagSelectorUtils.getQuestionTagTable());
        
        String sql = DbTagSelectorUtils.getSqlMkQuestionTagTableFromSelectQuestionId();
        LOGGER.info("Sql: " + sql);
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            LOGGER.error("SQL error.", e);
        }
        return false;        
    }


    private boolean completeTagTableFromQuestionTable(Connection conn) {
        String sql;
        if (DbUtils.tableExists(conn, DbTagSelectorUtils.getTagTable())) {
            if (!DbUtils.dropTable(conn, DbTagSelectorUtils.getTagTable())) {
                return false;
            }
        }
        
        sql = DbTagSelectorUtils.getSqlMkTagTableFromSelectOnPostTable();
        LOGGER.info("Sql: " + sql);
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("Run sql error.", e);
            return false;
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
    

    private boolean completeTempTagsTable(Connection conn, String tagsCsvFilename) {
        if (!createTempTagsTable(conn)) return false;
        if (!populateTempTagsTable(conn, tagsCsvFilename)) return false;
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
    

    private boolean createTempTagsTable(Connection conn) {
        return DbUtils.createTable(conn, DbTagSelectorUtils.getSqlMkTmpTagsTable(), null, null);
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
            LOGGER.info("Copying " + tmpTagnameFilename + " " + DbTagSelectorUtils.getTmpTagnameTable());
            InputStreamReader fileStreamReader = new InputStreamReader(new FileInputStream(tmpTagnameFilename), StandardCharsets.UTF_8);
            copyManager.copyIn("COPY " + DbTagSelectorUtils.getTmpTagnameTable() + " FROM STDIN WITH CSV", fileStreamReader);
            
            tagnameFile.delete();
            
            return true;
        } catch (FileNotFoundException e) {
            LOGGER.error("Error on tmpfile_tagname.csv", e);
        } catch (SQLException e) {
            LOGGER.error("SQL error", e);
        } catch (IOException e) {
            LOGGER.error("I/O error", e);
        }       
        return false;
    }
    

    
    private boolean populateTempTagsTable(Connection conn, String tagsCsvFilename) {
        try {
            CopyManager copyManager = new CopyManager((BaseConnection)conn);
            LOGGER.info("Copying " + tagsCsvFilename + " " + DbTagSelectorUtils.getTmpTagsTable());
            InputStreamReader fileStreamReader = new InputStreamReader(new FileInputStream(tagsCsvFilename), StandardCharsets.UTF_8);
            copyManager.copyIn("COPY " + DbTagSelectorUtils.getTmpTagsTable() + " FROM STDIN WITH CSV", fileStreamReader);
            return true;
        } catch (FileNotFoundException e) {
            LOGGER.error("Error on tmpfile_tagname.csv", e);
        } catch (SQLException e) {
            LOGGER.error("SQL error", e);
        } catch (IOException e) {
            LOGGER.error("I/O error", e);
        }       
        return false;
    }

    
    private boolean populateTempWordIdTable(Connection conn) {
        try {
            String tmpWordIdFilename = "tmpfile_wordid.csv";
            File tmpWordIdFile = new File(tmpWordIdFilename);
            FileWriter fileWriter = new FileWriter(tmpWordIdFile);
            
            CopyManager copyManager = new CopyManager((BaseConnection)conn);
            LOGGER.info("Copying distinct word id from " + DbTagSelectorUtils.getQuestionWordTable() + " to CSV file " + tmpWordIdFilename);
            copyManager.copyOut("COPY (" + DbTagSelectorUtils.getSqlQueryDistinctWordIdFromQuestionWordTable() + ") TO STDOUT WITH CSV", fileWriter);
            fileWriter.close();

            LOGGER.info("Copying " + tmpWordIdFilename + " " + DbTagSelectorUtils.getTmpWordIdTable());
            InputStreamReader fileStreamReader = new InputStreamReader(new FileInputStream(tmpWordIdFilename), StandardCharsets.UTF_8);
            copyManager.copyIn("COPY " + DbTagSelectorUtils.getTmpWordIdTable() + " FROM STDIN WITH CSV", fileStreamReader);

            tmpWordIdFile.delete();
            
            LOGGER.info("populated temp word id table.");
            return true;
        } catch (FileNotFoundException e) {
            LOGGER.error("Error on tmpfile_wordid.csv", e);
        } catch (SQLException e) {
            LOGGER.error("SQL error", e);
        } catch (IOException e) {
            LOGGER.error("I/O error", e);
        }       
        return false;
    }   

    
    private boolean completeTagTable(Connection conn) {
        LOGGER.debug("Building tag table ...");
        String sql = DbTagSelectorUtils.getSqlMkTagTableFromSelect();
        LOGGER.info("Sql: " + sql);
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("SQL error.", e);
            return false;
        }
        LOGGER.debug("writing debug information ...");
        sql = DbTagSelectorUtils.getSqlQueryWantedTagsNotInTagTable();
        try (PreparedStatement pstmt = conn.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                LOGGER.debug("Wanted Tag not in Tag Table " + DbTagSelectorUtils.getTagTable() + ": " + rs.getString(1).trim());
            }
            LOGGER.debug("Done outputing wanted tags not in tag table.");
        } catch (SQLException e) {
            LOGGER.error("SQL error.", e);
        }
        return true;
    }


    private boolean completeQuestionTagTable(Connection conn) {
        String sql = DbTagSelectorUtils.getSqlMkQuestionTagTableFromSelect();
        LOGGER.info("Sql: " + sql);
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            LOGGER.error("SQL error.", e);
        }
        return false;
    }
    

//    private boolean completeQuestionIdTable(Connection conn) {
//        String sql = DbTagSelectorUtils.getSqlMkQuestionIdTableFromSelect();
//        LOGGER.info("Sql: " + sql);
//        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
//            pstmt.executeUpdate();
//            return true;
//        } catch (SQLException e) {
//            LOGGER.error("SQL error.", e);
//        }
//        return false;
//    }   

    private boolean completeQuestionIdTable(Connection conn, int maxPostsToExtractForTags) {
        String sql = DbTagSelectorUtils.getSqlMkTagQuestionCountsTable();
        LOGGER.info("Sql: " + sql);
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("SQL error.", e);
            return false;
        }
        
        sql = DbTagSelectorUtils.getSqlMkQuestionIdTableFromSelect(maxPostsToExtractForTags);
        LOGGER.info("Sql: " + sql);
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            LOGGER.error("SQL error.", e);
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
            LOGGER.info("Copying question-word tuple from " 
                    + DbUtils.getQuestionWordTable() + " to CSV file " 
                    + tmpQuestionWordFile 
                    + " with query: " + DbTagSelectorUtils.getSqlQueryQuestionWordTable());
            copyManager.copyOut("COPY (" + DbTagSelectorUtils.getSqlQueryQuestionWordTable() + ") TO STDOUT WITH CSV", fileWriter);
            fileWriter.close();

            
            LOGGER.info("Copying " + tmpQuestionWordFile + " " + DbTagSelectorUtils.getQuestionWordTable());
            InputStreamReader fileStreamReader = new InputStreamReader(new FileInputStream(tmpQuestionWordFile), StandardCharsets.UTF_8);
            copyManager.copyIn("COPY " + DbTagSelectorUtils.getQuestionWordTable() + " FROM STDIN WITH CSV", fileStreamReader);
            
            tmpQuestionWordFile.delete();
            
            return true;
        } catch (FileNotFoundException e) {
            LOGGER.error("Error on tmpfile_wordid.csv", e);
        } catch (SQLException e) {
            LOGGER.error("SQL error", e);
        } catch (IOException e) {
            LOGGER.error("I/O error", e);
        }       
        return false;
    }

    private boolean createQuestionWordTable(Connection conn) {
        return DbUtils.createTable(conn, DbTagSelectorUtils.getSqlMkQuestionWordTable(), null, null);
    }

    private boolean completeVocabularyTable(Connection conn) {
        String sql = DbTagSelectorUtils.getSqlMkSimpleVocabularyTableFromSelect();
        LOGGER.debug("SQL: " + sql);
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            LOGGER.error("SQL error.", e);
        }
        return false;   
    }   
}
