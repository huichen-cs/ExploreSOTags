/**
 * This program queries a few tables and output tagnames and counts of questions to a CSV file. Questions
 * are posts with PostTypeId = 1. This count appears to be less or equal to the count column in the Tags 
 * table, for which, also @see SODataTool.TagPostCount. 
 */

package sodata.processor;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagQuestionCount 
{
    private static Logger logger = LoggerFactory.getLogger(TagQuestionCount.class);
    
    public static void main( String[] args )
    {
        logger.info(TagQuestionCount.class.getName() + " starts.");
        
        Properties properties = new Properties();
        InputStream in = null;
        Connection conn = null;
        PrintWriter out = null;
        String sql 
            = " SELECT " 
            +     "t.id, t.tagname, t.count, count(*) as qcount"
            + " FROM "
            +     "posts as p, posttags as ptags, tags as t, posttypes as ptypes"
            + " WHERE "
            +     "ptypes.name='Question' AND ptypes.id=p.posttypeid"
            +     " AND "
            +     "p.id=ptags.postid AND ptags.tagid=t.id"
            + " GROUP BY "
            +     "t.id"
            + " ORDER BY qcount DESC";
        String out_csv_fn = "../SOResults/tag_vs_questioncount.csv";
        
        try {
            in = new FileInputStream("sodumpdb.properties");
            properties.load(in);
            Class.forName(properties.getProperty("driver"));
            conn = DriverManager.getConnection(properties.getProperty("url"), properties);
            logger.info("Established connection to database server.");
            
            
            out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(out_csv_fn), StandardCharsets.UTF_8));
            out.println("id,tagname,count");
            PreparedStatement pstmt = conn.prepareStatement(sql);
            logger.info("Running query " + pstmt.toString());
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                long id = rs.getLong("id");
                String tagname = rs.getString("tagname");
                long count = rs.getLong("count");
                long qcount = rs.getLong("qcount");
                out.println(id + "," + tagname + "," + count + "," + qcount);
            }
            logger.info("Wrote tag versus question counts to CSV file " + out_csv_fn);
        } catch (IOException | SQLException | ClassNotFoundException ex) {
            ex.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (out != null) {
                out.close();
            }
        }
    }
}

