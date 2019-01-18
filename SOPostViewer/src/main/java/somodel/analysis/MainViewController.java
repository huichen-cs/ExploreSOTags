package somodel.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.StackPane;
import javafx.scene.web.WebView;
import javafx.stage.DirectoryChooser;
import javafx.stage.Stage;

public class MainViewController {
    private final static Logger LOGGER = LoggerFactory.getLogger(MainViewController.class);

    @FXML
    private Button browseFolderButton;

    @FXML
    private Label filenameLabel;
    
    @FXML
    private StackPane webViewHolder;
    
    @FXML
    private WebView webView;

    @FXML
    private Button nextTreeButton;

    @FXML
    private Button prevTreeButton;

    @FXML
    private StackPane imageViewHolder;
    
    @FXML
    private ImageView treeImageView;

    private Stage stage;

    private Path treeFilePath;

    private List<Path> treeFileList;
    private ListIterator<Path> treeFileIterator;
    
    private Properties settingProperties;
    
    public void initialize() {
        treeImageView.preserveRatioProperty().set(true);
        treeImageView.fitWidthProperty().bind(imageViewHolder.widthProperty());
        treeImageView.fitHeightProperty().bind(imageViewHolder.heightProperty());

        browseFolderButton.setOnAction(e -> selectFolder());
        prevTreeButton.setOnAction(e -> prevTree());
        nextTreeButton.setOnAction(e -> nextTree());
    }

    public void setStage(Stage stage) {
        this.stage = stage;
    }

    private void selectFolder() {
        DirectoryChooser chooser = new DirectoryChooser();
        chooser.setTitle("Select Tree File Folder");
        String defaultFolder = settingProperties.getProperty("defaulttreefolder");
        LOGGER.info("Default folder from application setting is " + defaultFolder);
        if (defaultFolder == null || defaultFolder.isEmpty()) {
            defaultFolder = System.getProperty("user.dir");
        }
        LOGGER.info("Default folder is at " + defaultFolder);
        File defaultDirectory = new File(defaultFolder);
        chooser.setInitialDirectory(defaultDirectory);
        File selectedDirectory = chooser.showDialog(stage);
        if (selectedDirectory == null)
            return;

        treeFilePath = selectedDirectory.toPath();
        LOGGER.info("The tree files are at " + treeFilePath);

        try {
            treeFileList = listSourceFiles(treeFilePath);
            LOGGER.info("Got " + treeFileList.size() + " tree files.");
            treeFileIterator = treeFileList.listIterator();
        } catch (IOException e) {
            LOGGER.info("Failed to iterate tree text files at " + treeFilePath);
        }
        settingProperties.setProperty("defaulttreefolder", treeFilePath.toString());
    }

    private List<Path> listSourceFiles(Path dir) throws IOException {
        List<Path> result = new LinkedList<Path>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*_*_*.txt")) {
            for (Path entry : stream) {
                result.add(entry);
            }
        }

        return result;
    }

    private void iterTree(boolean forward) {
        if (null == treeFileIterator) {
            selectFolder();
            if (null == treeFileIterator) return;
        }
        Path treeFilePath = null;
        if (forward) {
            if (!treeFileIterator.hasNext()) {
                filenameLabel.setText("No more next files.");
                return;
            }
            treeFilePath = treeFileIterator.next();
        } else {
            if (!treeFileIterator.hasPrevious()) {
                filenameLabel.setText("No more previous files.");
                return;
            }
            treeFilePath = treeFileIterator.previous();
        }
        System.out.println(treeFilePath.toString());
        
        Path pngPath = drawTreeGraph(treeFilePath.toString());
        treeImageView.setImage(new Image(pngPath.toUri().toString()));
        
        Path lastPath = treeFilePath.getName(treeFilePath.getNameCount() - 1);
        filenameLabel.setText(lastPath.toString());

        String[] parts = lastPath.toString().split("_");
        String soFileNo = parts[1];
        System.out.println(soFileNo);
        String url = "https://stackoverflow.com/questions/" + soFileNo;
        webView.getEngine().load(url);
    }

    private void nextTree() {
        iterTree(true);
    }

    private void prevTree() {
        iterTree(false);
    }
    
    private Path drawTreeGraph(String treeFilename) {
        String[] cmd = new String[4];
        cmd[0] = Paths.get(settingProperties.getProperty("python")).normalize().toString();
        cmd[1] = Paths.get(settingProperties.getProperty("pytreemaker")).normalize().toString();
        
        String pngFilename = treeFilename.replaceFirst(".txt$", ".png");
        cmd[2] = treeFilename;
        cmd[3] = pngFilename;
        for (String c: cmd) {
            System.out.println(c);
        }
        try {
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            pb.redirectOutput();
            Process p = pb.start();
            try (BufferedReader reader = 
                    new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String line;
                while((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            }
            p.waitFor();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Path pngPath = Paths.get(pngFilename);
        if (!Files.exists(pngPath)) {
            throw new RuntimeException("Cannot create png file at " + pngFilename);
        }
        System.out.println("Generated " + pngFilename);
        return pngPath;
    }

    public void setSettingProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("The applications' setting Properties object must not be null");
        }
        settingProperties = properties;
    }
}