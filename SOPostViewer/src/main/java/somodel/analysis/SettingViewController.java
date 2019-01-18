package somodel.analysis;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.stage.FileChooser;
import javafx.stage.FileChooser.ExtensionFilter;

public class SettingViewController {
    private final static Logger LOGGER = LoggerFactory.getLogger(SettingViewController.class);

    @FXML
    private TextField pythonPathField;

    @FXML
    private Button locatePythonButton;

    @FXML
    private TextField pyTreeMakerField;

    @FXML
    private Button locatePyTreeMakerButton;

    @FXML
    private Button okSettingButton;

    @FXML
    private Label settingStatusLabel;

    private Properties settingProperties;

    private EventHandler<ActionEvent> settingConfirmationHandler;

    public void initialize() {
        okSettingButton.setOnAction(e -> confirmSettings(e));
        locatePythonButton.setOnAction(e -> selectPythonPath());
        locatePyTreeMakerButton.setOnAction(e -> selectPyTreeMakerPath());
    }

    public void setSettingProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("The applications' setting Properties object must not be null");
        }
        settingProperties = properties;
        
        pythonPathField.setText(properties.getProperty("python"));
        pyTreeMakerField.setText(properties.getProperty("pytreemaker"));
    }

    public void setSettingConfirmationAction(EventHandler<ActionEvent> handler) {
        settingConfirmationHandler = handler;
    }

    private void confirmSettings(ActionEvent e) {
        if (setPythonPath() && setPyTreeMakerPath()) {
            LOGGER.info("Confirmed settings.");
            settingConfirmationHandler.handle(e);
        } else {
            settingStatusLabel.setText("Improper setting!");
        }
    }

    private void selectPythonPath() {

        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Select Python Executable");
        File selectedFile = fileChooser.showOpenDialog(locatePythonButton.getScene().getWindow());
        if (selectedFile != null) {
            pythonPathField.setText(selectedFile.getAbsolutePath());
        }
    }

    private void selectPyTreeMakerPath() {

        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Select Python Tree Maker Script");
        fileChooser.getExtensionFilters().add(new ExtensionFilter("Python Script", "*.py"));
        File selectedFile = fileChooser.showOpenDialog(locatePyTreeMakerButton.getScene().getWindow());
        if (selectedFile != null) {
            pyTreeMakerField.setText(selectedFile.getAbsolutePath());
        }
    }

    private boolean setPythonPath() {
        String py = pythonPathField.getText();
        if (py == null || py.isEmpty()) {
            LOGGER.info("Python path is null or empty.");
            return false;
        }

        Path pythonPath = Paths.get(py);
        if (!Files.isExecutable(pythonPath)) {
            LOGGER.info("Python path is not an executable.");
            return false;
        }

        settingProperties.setProperty("python", pythonPath.toString());
        return true;
    }

    private boolean setPyTreeMakerPath() {
        String maker = pyTreeMakerField.getText();
        if (maker == null || maker.isEmpty()) {
            LOGGER.info("Python tree maker script path is null or empty.");
            return false;
        }

        Path makerPath = Paths.get(maker);
        if (!Files.isReadable(makerPath) || !Files.isRegularFile(makerPath)) {
            LOGGER.info("Python tree maker script path isn't readable or a regular file.");
            return false;
        }

        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:**.py");
        if (!matcher.matches(makerPath)) {
            LOGGER.info("Python tree maker script path " + makerPath.toString() + " isn't a python script.");
            return false;
        }

        settingProperties.setProperty("pytreemaker", maker.toString());
        return true;
    }

}
