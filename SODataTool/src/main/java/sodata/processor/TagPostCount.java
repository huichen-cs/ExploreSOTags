/**
 * This program queries the Tabs table and output tagnames and counts to a CSV file. The count in 
 * the Tags table is the number of posts tagged, somehow it is always slightly greater than the
 * number of questions tagged, for which, also @see SODataTool.TagQuestionCount. 
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

public class TagPostCount 
{
    private static Logger logger = LoggerFactory.getLogger(TagPostCount.class);
    
    public static void main( String[] args )
    {
        logger.info(TagPostCount.class.getName() + " starts.");
        
        Properties properties = new Properties();
        InputStream in = null;
        Connection conn = null;
        PrintWriter out = null;
        String sql = "SELECT id,tagname,count FROM tags ORDER BY count DESC";
        String out_csv_fn = "../SOResults/tag_vs_postcount.csv";
        
        try {
            in = new FileInputStream("sodumpdb.properties");
            properties.load(in);
            Class.forName(properties.getProperty("driver"));
            conn = DriverManager.getConnection(properties.getProperty("url"), properties);
            logger.info("Established connection to database server.");
            
            
            out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(out_csv_fn), StandardCharsets.UTF_8));
            out.println("id,tagname,count");
            PreparedStatement pstmt = conn.prepareStatement(sql);
            logger.info("Prepared query " + pstmt.toString());
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                long id = rs.getLong("id");
                String tagname = rs.getString("tagname");
                long count = rs.getLong("count");
                out.println(id + "," + tagname + "," + count);
            }
            logger.info("Wrote tag versus post counts to CSV file " + out_csv_fn);
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
