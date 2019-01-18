package sodata.l2hmodel.tree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

public class IndentedTreeParserTest {

    @Test
    public void testParserWithResources() {
        System.out.println(System.getProperty("user.dir"));
        try {
            IndentedTreeParser parser = new IndentedTreeParser();
            InputStream in = getClass().getClassLoader().getResourceAsStream("testdata/topwords-500.txt");
            L2hTree tree = parser.parseAll(in);
            String newTreeText = null;
            String oldTreeText = null;
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            try (PrintStream out = new PrintStream(byteOut, false, StandardCharsets.UTF_8.name())) {
                tree.printTree(out, true);
                tree.printStat(out);
                out.flush();
                newTreeText = byteOut.toString();
            }
            List<String> lines = new LinkedList<String>();
            in = getClass().getClassLoader().getResourceAsStream("testdata/topwords-500.txt");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty()) {
                        continue;
                    }
                    lines.add(line);
                }
            }
            byteOut = new ByteArrayOutputStream();
            try (PrintStream out = new PrintStream(byteOut, false, StandardCharsets.UTF_8.name())) {
                for (String line : lines) {
                    out.println(line);
                }
                out.flush();
                oldTreeText = byteOut.toString();
            }
            System.out.println(newTreeText);
            System.out.println("------------");
            System.out.println(oldTreeText);
            assertEquals(oldTreeText, newTreeText);
        } catch (IOException e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }
}
