package sodata.l2hmodel.tree;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class IndentedTreeApp {

    public static void main(String[] args) {
        IndentedTreeApp app = new IndentedTreeApp();
        Path path = null;
        try {
            IndentedTreeParser parser = new IndentedTreeParser();
            path = app.getTreeFilePathFromAny(args);
            L2hTree tree = parser.parseAll(path);
            System.out.println("Tree >>>> ");
            tree.printTree(System.out);
            tree.printStat(System.out);
        } catch (IOException e) {
            System.out.println("Usage: IndentedTreeParser <L2H_TOP_WORD_FILE>");
            e.printStackTrace();
            System.exit(-1);
        }
    }
    

    public Path getTreeFilePathFromAny(String[] args) throws IOException {
        Path path = null;
        if (args.length > 0) {
            path = Paths.get(args[0]);
        } else {
            path = getTreeFilePathFromPropertiesFile();
        }
        return path;
    }

    private Path getTreeFilePathFromPropertiesFile() throws IOException {
        Path path = null;
        try (InputStream in = getPropertiesFileStream()) {
            if (in != null) {
                path = getTreeFilePathFromStream(in);
            }
        }
        return path;
    }

    private Path getTreeFilePathFromStream(InputStream in) throws IOException {
        Properties properties = new Properties();
        properties.load(in);
        Path path = Paths.get(properties.getProperty("tree_file"));
        if (!Files.exists(path)) {
            throw new FileNotFoundException(path.toString() + " not found.");
        }
        return path;
    }

    private InputStream getPropertiesFileStream() throws IOException {
        InputStream in = null;
        Path path = getPropertiesFilePath();
        if (path != null) {
            in = Files.newInputStream(path);
        } else {
            String filename = getClass().getSimpleName() + ".properties";
            in = getClass().getClassLoader().getResourceAsStream(filename);
        }
        return in;
    }

    private Path getPropertiesFilePath() {
        String filename = getClass().getSimpleName() + ".properties";
        Path path = Paths.get(filename);
        if (Files.exists(path)) {
            return path;
        }
        return null;
    }

    /*
     * //
     * https://stackoverflow.com/questions/227486/find-where-java-class-is-loaded-
     * from private String whereFrom(Object o) { if (o == null) { return null; }
     * Class<?> c = o.getClass(); ClassLoader loader = c.getClassLoader(); if
     * (loader == null) { loader = ClassLoader.getSystemClassLoader(); while (loader
     * != null && loader.getParent() != null) { loader = loader.getParent(); } } if
     * (loader != null) { String name = c.getCanonicalName(); URL resource =
     * loader.getResource(name.replace(".", "/") + ".class"); if (resource != null)
     * { return resource.toString(); } } return "Unknown"; }
     */

}
