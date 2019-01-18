package somodel.analysis;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class SOPostViewerApp extends Application {
    private final static Logger LOGGER = LoggerFactory.getLogger(SOPostViewerApp.class);

    private Stage stage;
    private SettingViewController svController;
    private MainViewController mvController;
    private Parent svRoot;
    private Parent mvRoot;
    private Properties settingProperties;

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void init() {
        LOGGER.info("Launching the app ...");
    }

    @Override
    public void start(Stage primaryStage) throws Exception {
        settingProperties = loadSettingProperties();

        stage = primaryStage;

        FXMLLoader loader = new FXMLLoader();
        mvRoot = loader.load(getClass().getResourceAsStream("fxml_mainview.fxml"));
        mvController = loader.getController();
        mvController.setSettingProperties(settingProperties);

        loader = new FXMLLoader();
        svRoot = loader.load(getClass().getResourceAsStream("fxml_settingview.fxml"));
        svController = loader.getController();
        svController.setSettingProperties(settingProperties);

        primaryStage.setScene(new Scene(svRoot));
        primaryStage.show();

        svController.setSettingConfirmationAction(e -> confirmAndShowMainView(e));
    }

    @Override
    public void stop() {
        LOGGER.info("Exiting the app...");

        String location = System.getProperty("user.dir");
        Path path = Paths.get(location, "application.properties");
        try (Writer writer = Files.newBufferedWriter(path)) {
            settingProperties.store(writer, "created by " + this.getClass().getName());
            LOGGER.info("Wrote the application properties to " + path.toString());
        } catch (IOException e) {
            LOGGER.error("Failed to write the application properties to " + path.toString(), e);
        }
        LOGGER.info("Exited the app.");
    }

    private Properties loadSettingProperties() {
        Path propertiesPath = Paths.get(System.getProperty("user.dir"), "application.properties");

        Properties properties = new Properties();

        try (InputStream in = Files.newInputStream(propertiesPath)) {
            properties.load(in);
            LOGGER.info("Loaded the application properties form " + propertiesPath.toString());
        } catch (IOException e) {
            LOGGER.error("Failed to load application properties at " + propertiesPath.toString());
        }
        return properties;
    }

    private void confirmAndShowMainView(ActionEvent e) {
        stage.setScene(new Scene(mvRoot));
        stage.show();
    }

    // private String getClassesLoadingPath() {
    // String path = null;
    // try {
    // path = Paths.get(new
    // URI(whereFrom(this).replaceAll(getClass().getCanonicalName().replace(".",
    // "/") + ".class$", ""))).toString();
    // } catch (URISyntaxException e) {
    // LOGGER.error(e.getMessage(), e);
    // }
    // return path;
    // }
    //
    // private String whereFrom(Object o) {
    // if (o == null) {
    // return null;
    // }
    // Class<?> c = o.getClass();
    // ClassLoader loader = c.getClassLoader();
    // if (loader == null) {
    // LOGGER.info("class loader is null");
    // loader = ClassLoader.getSystemClassLoader();
    // while (loader != null && loader.getParent() != null) {
    // loader = loader.getParent();
    // }
    // }
    // if (loader != null) {
    // String name = c.getCanonicalName();
    // URL resource = loader.getResource(name.replace(".", "/") + ".class");
    // LOGGER.info("class is loaded from " + resource);
    // if (resource != null) {
    // return resource.toString();
    // }
    // }
    // return "Unknown";
    // }

}
