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

public class QuestionAnswerCount {
    private static Logger logger = LoggerFactory.getLogger(QuestionAnswerCount.class);

    public static void main(String[] args) {
        logger.info(QuestionAnswerCount.class.getName() + " starts.");

        Properties properties = new Properties();
        InputStream in = null;
        Connection conn = null;
        PrintWriter out = null;
        String sql 
            = " SELECT "
            +    "p.id, p.answercount"
            + " FROM "
            +    "posts as p, posttypes as pt"
            + " WHERE "
            +    "pt.name='Question' AND p.posttypeid=pt.id"
            + " ORDER BY answercount DESC";
        String csv_fn = "../SOResults/question_vs_anscount.csv";

        try {
            in = new FileInputStream("sodumpdb.properties");
            properties.load(in);
            Class.forName(properties.getProperty("driver"));
            conn = DriverManager.getConnection(properties.getProperty("url"), properties);
            logger.info("Established connection to database server.");

            out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(csv_fn), StandardCharsets.UTF_8));
            out.println("id,tagname,count");
            PreparedStatement pstmt = conn.prepareStatement(sql);
            logger.info("Running query " + pstmt.toString());
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                long id = rs.getLong("id");
                long count = rs.getLong("answercount");
                out.println(id + "," + count);
            }
            logger.info("Wrote tag versus post counts to CSV file " + csv_fn);
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
