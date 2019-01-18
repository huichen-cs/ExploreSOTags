package sodata.testpreparation;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.database.DbUtils;

public class ShortQuestionExtractor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShortQuestionExtractor.class);
    private static final long[] QUESTION_IDS = {2041, 66542, 17054, 17612, 23063};
    private static final String POST_TEXT_FN_PREFIX = "../SOResults/testdata/post_text_";
    private static final String POST_TAG_IDS_FN_PREFIX = "../SOResults/testdata/post_tag_ids_";
    private static final String TAGS_FN_PREFIX = "../SOResults/testdata/tags_";
    private static final String POST_TEXT_FN_SUFFIX = ".txt";
    
    public static void main( String[] args )
    {
        buildPostTextFiles();
        buildPostTagIdsFiles();
        buildTagsFiles();
    } 
    
    public static boolean buildPostTextFiles() {
        Connection conn = null; 
        ResultSet rs = null;
        
        String sql = "SELECT id,title,body FROM posts WHERE posttypeid=1 AND ( ";
        for (int i=0; i<QUESTION_IDS.length; i++) {
            sql += " id=" + QUESTION_IDS[i];
            if (i<QUESTION_IDS.length-1) {
                sql += " OR ";
            }
        }
        sql += " ) ";

        try {          
            conn = DbUtils.connect(DbUtils.SODUMPDB_PROPERTIES_FN);
            PreparedStatement pstmt = conn.prepareStatement(sql);
            LOGGER.info("Prepared query " + pstmt.toString());
            rs = pstmt.executeQuery();
            while (rs.next()) {
                long id = rs.getLong("id");
                String title = rs.getString("title");
                String body = rs.getString("body");
                String out_fn = POST_TEXT_FN_PREFIX + id + POST_TEXT_FN_SUFFIX;
                
                PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(out_fn), StandardCharsets.UTF_8));
                out.println(title);
                out.println(body);
                if (out != null) {
                    out.close();
                }
                LOGGER.info("Wrote title and body for post " + id + " to "+ out_fn);                
            }
            return true;
        } catch (IOException | SQLException  e) {
            LOGGER.error("Failed to get post titles and bodies.", e);
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    LOGGER.error("Failed to close database connection.", e);
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    LOGGER.error("Failed to close database ResultSet.", e);
                }
            }
        }
    }

    
    private static boolean buildPostTagIdsFiles() {
        try (Connection conn = DbUtils.connect(DbUtils.SODUMPDB_PROPERTIES_FN)) {
            String sqlRowNumber 
                    = "SELECT "
                    + " -1+ROW_NUMBER() OVER() AS rownumber, t.id,t.tagname "
                    + " FROM (SELECT DISTINCT tagid FROM posttags WHERE postid in ( ";
            for (int i=0; i<QUESTION_IDS.length; i++) {
                sqlRowNumber += QUESTION_IDS[i];
                if (i < QUESTION_IDS.length - 1) {
                    sqlRowNumber += ", ";
                }
            }
            sqlRowNumber += " )) AS tid, tags AS t WHERE tid.tagid=t.id ORDER BY t.id ";
            
            
            for (long questionId: QUESTION_IDS) {
                String sql = "SELECT pt.postid,pt.tagid,rn.rownumber FROM ( "
                        + sqlRowNumber + " ) AS rn, posttags AS pt " 
                        + " WHERE pt.postid=" + Long.toString(questionId) 
                        + " AND rn.id=pt.tagid";
                LOGGER.info("Sql: " + sql);    
                String out_fn = POST_TAG_IDS_FN_PREFIX + questionId + POST_TEXT_FN_SUFFIX;
                
                try (PreparedStatement pstmt = conn.prepareStatement(sql);
                        ResultSet rs = pstmt.executeQuery();
                        PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(out_fn), StandardCharsets.UTF_8))) {
                    while (rs.next()) {
                        long postId = rs.getLong(1);
                        long tagId = rs.getLong(2);
                        long rowNumber = rs.getLong(3);
                        out.println(Long.toString(postId) + "," + Long.toString(tagId) + "," + Long.toString(rowNumber));
                    }
                }
                LOGGER.info("Wrote postid and tagid for post " + questionId + " to "+ out_fn);                                
            }
            return true;
        } catch (SQLException | IOException e) {
            LOGGER.error("failed to build posttags files.", e);
            return false;
        }   
    }
    
    private static boolean buildTagsFiles() {
        String out_fn = TAGS_FN_PREFIX;
        String sql = "SELECT "
                + " -1+ROW_NUMBER() OVER() AS rownumber, t.id,t.tagname "
                + " FROM (SELECT DISTINCT tagid FROM posttags WHERE postid in ( ";
        for (int i=0; i<QUESTION_IDS.length; i++) {
            out_fn += QUESTION_IDS[i];
            sql += QUESTION_IDS[i];
            if (i < QUESTION_IDS.length - 1) {
                out_fn += "_";
                sql += ", ";
            }
        }
        sql += " )) AS tid, tags AS t WHERE tid.tagid=t.id ORDER BY t.id ";
        out_fn = out_fn + POST_TEXT_FN_SUFFIX;
        LOGGER.info("Sql: " + sql);
        LOGGER.info("out_fn: " + out_fn);
        
        
        try (Connection conn = DbUtils.connect(DbUtils.SODUMPDB_PROPERTIES_FN);
                PreparedStatement pstmt = conn.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery();
                PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(out_fn), StandardCharsets.UTF_8))) {
            while (rs.next()) {
                long rowNumber = rs.getLong(1);
                long tagId = rs.getLong(2);
                String tagName = rs.getString(3);
                out.println(Long.toString(tagId) + "," + tagName+ "," + Long.toString(rowNumber));
            }
            LOGGER.info("Wrote row number, tagid, and tagname to " + out_fn);
            return true;
        } catch (SQLException | IOException e) {
            LOGGER.error("failed to build posttags files.", e);
            return false;
        } 
    }
}
