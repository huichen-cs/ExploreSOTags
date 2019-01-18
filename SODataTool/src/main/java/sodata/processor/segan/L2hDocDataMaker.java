/**
 * Extract Stack Overflow questions and format it as L2h format when the word and the label
 * vocabularies are given.
 */

package sodata.processor.segan;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.WordFrequency;
import sodata.database.DbUtils;
import sodata.database.L2hDbUtils;
import sodata.parser.SimplePostBodyWordExtractionParser;

public class L2hDocDataMaker {
    private class AppCliUsageHelper {
        private Options options;
        
        public AppCliUsageHelper(Options options) {
            this.options = options;
        }
        
        public void help(String appName) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(appName, options);
        }
    }
    
    private class L2hOptions {
        private final String dbWorkTableSuffix = "_d";
        
        private String dbPropertiesFilename;

        private String trainingDictFolder;

        private String trainingFilename;

        private String outputDataFolder;

        private String outputFormatFilename;

        private String startTimestamp;

        private String endTimestamp;
        
        private String dbWorkTablePrefix;
        
        private boolean forceDbWipe;

        public L2hOptions(String dbPropertiesFilename
                , String trainingDictFolder
                , String trainingFilename
                , String outputDataFolder
                , String outputFormatFilename
                , String startTimestamp
                , String endTimestamp
                , String dbWorkTablePrefix
                , boolean forceDbWipe) {
            this.dbPropertiesFilename = dbPropertiesFilename;
            this.trainingDictFolder = trainingDictFolder;
            this.trainingFilename = trainingFilename;
            this.outputDataFolder = outputDataFolder;
            this.outputFormatFilename = outputFormatFilename;
            this.startTimestamp = startTimestamp;
            this.endTimestamp = endTimestamp;
            this.dbWorkTablePrefix = dbWorkTablePrefix;
            this.forceDbWipe = forceDbWipe;
        }

        public String getDbPropertiesFilename() {
            return dbPropertiesFilename;
        }

        public String getDbWorkingTableSuffix() {
            return dbWorkTableSuffix;
        }

        public String getDbWorkTablePrefix() {
            return dbWorkTablePrefix;
        }

        public String getEndTimestamp() {
            return endTimestamp;
        }

        public String getOutputDataFolder() {
            return outputDataFolder;
        }

        public String getOutputFormatFilename() {
            return outputFormatFilename;
        }

        public String getStartTimestamp() {
            return startTimestamp;
        }

        public String getTrainingDictFolder() {
            return trainingDictFolder;
        }
        
        public String getTrainingFilename() {
            return trainingFilename;
        }

        public boolean isForceDbWipe() {
            return forceDbWipe;
        }
    }

    private final static Logger LOGGER = LoggerFactory.getLogger(L2hDocDataMaker.class);
    private final static boolean IGNORE_CASE = true;

    public static void main(String[] args) {
        L2hDocDataMaker docDataMaker = new L2hDocDataMaker();
        
        try {
            L2hOptions l2hOptions = docDataMaker.parseCommandLineOptions(args);
            docDataMaker.loadTrainingDictionaries(l2hOptions.getTrainingDictFolder()
                    , l2hOptions.getTrainingFilename()
                    , StandardCharsets.UTF_8);
            docDataMaker.selectQuestionsFromDb(l2hOptions.getDbPropertiesFilename()
                    , l2hOptions.isForceDbWipe()
                    , l2hOptions.getStartTimestamp(), l2hOptions.getEndTimestamp() 
                    , l2hOptions.getDbWorkTablePrefix(), l2hOptions.getDbWorkingTableSuffix()
                    , StandardCharsets.UTF_8);
            docDataMaker.saveDataSetToFiles(l2hOptions.getOutputFormatFilename()
                    , l2hOptions.getOutputDataFolder()
                    , l2hOptions.getDbWorkTablePrefix()
                    , l2hOptions.getDbWorkingTableSuffix()
                    , l2hOptions.getDbPropertiesFilename());
        } catch (ParseException e) {
           docDataMaker.help("L2hDocDataMaker");
           System.err.println("Addition error message: " + e.getMessage());
        } catch (FileNotFoundException e) {
            LOGGER.error("Cannot open the training dictionary files", e);
        } catch (IOException e) {
            LOGGER.error("Cannot read the training dictionary files", e);
        } catch (SQLException e) {
            LOGGER.error("Cannot query database", e);
        }
    }

    private void saveDataSetToFiles(String datasetName, String datasetFolder,
            String tablePrefix, String tableSuffix, String dbPropertiesFilename) {
        new File(datasetFolder).mkdirs();
        L2hDataMakerBase.makeL2HDatasetFiles(datasetName, 
                datasetFolder, tablePrefix, tableSuffix, dbPropertiesFilename);
    }

    private AppCliUsageHelper helper;

    private LabeledTextDatasetDictionaries trainingDictionaries;

    private void help(String appName) {
        if (null != helper) {
            helper.help(appName);
        }
    }

    private void loadTrainingDictionaries(String datasetFolder, String datasetName, Charset charset) throws FileNotFoundException, IOException {
        trainingDictionaries = new LabeledTextDatasetDictionaries(datasetFolder, datasetName, charset);
    }

    private Map<String, Long> makeWordRowNumberMap(ArrayList<String> wordList) {
        Map<String, Long> wordRowNumberMap = new HashMap<String, Long>();
        
        long rowNumber = 0;
        for (String word : wordList) {
            wordRowNumberMap.put(word,  rowNumber);
            rowNumber ++;
        }

        return wordRowNumberMap;
    }

    private L2hOptions parseCommandLineOptions(String[] args) throws ParseException {
        Options options = new Options();
        
        options.addOption(Option.builder().longOpt("db-properties")
                .desc("database properties")
                .hasArg(true)
                .argName("FILENAME").build());
        
        options.addOption(Option.builder().longOpt("training-dict-folder")
                    .desc("folder where training word and label dictionaries reside")
                    .hasArg(true)
                    .argName("FOLDER").build());

        options.addOption(Option.builder().longOpt("training-format-filename")
                .desc("training word and label dictionary filenames without suffix")
                .hasArg(true)
                .argName("FILENAME").build());

        options.addOption(Option.builder().longOpt("output-data-folder")
                .desc("output folder for the dataset whose tags to be inferred")
                .hasArg(true)
                .argName("FOLDER").build());

        options.addOption(Option.builder().longOpt("output-format-filename")
                .desc("output filename without suffix")
                .hasArg(true)
                .argName("FILENAME").build());

        options.addOption(Option.builder().longOpt("start-timestamp")
                .desc("start-timestamp in yyyy-MM-dd HH:mm:ss")
                .hasArg(true)
                .argName("TIMESTAMP").build());

        options.addOption(Option.builder().longOpt("end-timestamp")
                .desc("end-timestamp in yyyy-MM-dd HH:mm:ss")
                .hasArg(true)
                .argName("TIMESTAMP").build());
        
        options.addOption(Option.builder().longOpt("db-work-table-prefix")
                .desc("working database table name prefix")
                .hasArg(true)
                .argName("NAME PREFIX").build());

        options.addOption("f", "force", false, "force to wipe out existing working db tables");
        options.addOption("h", "help", false, "print this message");

        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args);

        helper = new AppCliUsageHelper(options);

        if (line.hasOption("h")) {
            throw new ParseException("User wants help.");
        }
        
        String dbPropertiesFilename = "";
        if (line.hasOption("db-properties")) {
            dbPropertiesFilename = line.getOptionValue("db-properties");
        } else {
            throw new ParseException("Option db-properties is required.");
        }

        String trainingDictFolder = "";
        if (line.hasOption("training-dict-folder")) {
            trainingDictFolder = line.getOptionValue("training-dict-folder");
        } else {
            throw new ParseException("Option training-dict-folder is required.");
        }

        String trainingFilename = "";
        if (line.hasOption("training-format-filename")) {
            trainingFilename = line.getOptionValue("training-format-filename");
        } else {
            throw new ParseException("Option training-format-filename is required.");
        }
        
        
        String outputDataFolder = "";
        if (line.hasOption("output-data-folder")) {
            outputDataFolder = line.getOptionValue("output-data-folder");
        } else {
            throw new ParseException("Option output-data-folder is required.");
        }
        
        String outputFormatFilename = ""; 
        if (line.hasOption("output-format-filename")) {
            outputFormatFilename = line.getOptionValue("output-format-filename");
        } else {
            throw new ParseException("Option output-format-filename is required.");
        }

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String startTimestamp = "";
        if (line.hasOption("start-timestamp")) {
            startTimestamp = line.getOptionValue("start-timestamp");
            try {
                format.parse(startTimestamp);
            } catch (java.text.ParseException e) {
                throw new ParseException("The start-timestamp is formatted correct.");
            }
        } else {
            throw new ParseException("Option start-timestamp is required.");
        }

        String endTimestamp = "";
        if (line.hasOption("start-timestamp")) {
            endTimestamp = line.getOptionValue("end-timestamp");
            try {
                format.parse(endTimestamp);
            } catch (java.text.ParseException e) {
                throw new ParseException("The end-timestamp is formatted correct.");
            }
        } else {
            throw new ParseException("Option end-timestamp is required.");
        }
        
        String dbWorkTablePrefix = "";
        if (line.hasOption("db-work-table-prefix")) {
            dbWorkTablePrefix = line.getOptionValue("db-work-table-prefix");
        } else {
            throw new ParseException("Option db-work-table-prefix is required");
        }
        
        boolean forceRedo = line.hasOption("f") || line.hasOption("force");

        L2hOptions l2hOptions = new L2hOptions(dbPropertiesFilename
                , trainingDictFolder
                , trainingFilename
                , outputDataFolder
                , outputFormatFilename
                , startTimestamp
                , endTimestamp
                , dbWorkTablePrefix
                , forceRedo);
        
        return l2hOptions;
    }
    
    private void saveLabelsToDb(Connection conn, String tablePrefix, String tableSuffix, Charset charset)
            throws SQLException, IOException {
        try (PreparedStatement pstmt = conn.prepareStatement(L2hDbUtils.getSqlMkL2hTmpTagTable())) {
            pstmt.executeUpdate();
        }

        String tmpTagFilename = "tmpfile_tmptagfile.csv";
        File tmpTagFile = new File(tmpTagFilename);
        try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tmpTagFile), charset))) {
            ArrayList<String> tagList = trainingDictionaries.getLabelList();
            long rowNumber = 0;
            for (String tag : tagList) {
                writer.println(tag + "," + rowNumber);
                rowNumber++;
            }
        }

        CopyManager copyManager = new CopyManager((BaseConnection) conn);
        LOGGER.info("Copying tmpfile_tmptagfile.csv to table " + DbUtils.getTmpTagTable());
        try (InputStreamReader fileStreramReader = new InputStreamReader(new FileInputStream(tmpTagFile), charset)) {
            copyManager.copyIn("COPY " + DbUtils.getTmpTagTable() + " FROM STDIN WITH CSV", fileStreramReader);
            LOGGER.info("Populated temporary tag table " + DbUtils.getTmpTagTable());
        }

        String sql = L2hDbUtils.getSqlMkL2hTagTableFromTmpTagTable(tablePrefix, tableSuffix);
        LOGGER.info("Executing sql = " + sql);
        try (PreparedStatement pstmt = conn
                .prepareStatement(sql)) {
            pstmt.executeUpdate();
        }
        DbUtils.createTableIndices(conn, L2hDbUtils.getSqlMkL2hTagTableIndices(tablePrefix, tableSuffix));

        tmpTagFile.delete();
    }

    private void saveQuestionTagsToDbOnLabels(Connection conn, String tablePrefix, String tableSuffix,
            String startTimestamp, String endTimestamp) throws SQLException {
        
        // make wk_questiontag_f_l2h
        String sql = L2hDbUtils.getSqlMkQuestionTagFromSelect(tablePrefix, tableSuffix, startTimestamp, endTimestamp);
        LOGGER.info("Executing sql = " + sql);
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
        }
    }

    private Map<String, WordFrequency> initializeWordFrequencyMap(Map<String, Long> wordRowNumberMap) {
        Map<String, WordFrequency> wordWordFrequencyMap = new HashMap<String, WordFrequency>();
        wordRowNumberMap.forEach((word, rowNumber) -> {
            wordWordFrequencyMap.put(word, new WordFrequency(rowNumber, 0L));
        });
        return wordWordFrequencyMap;
    }
    
    private void updateDocWordWordFrequencyMap(Map<String, WordFrequency> docWordWordFrequencyMap, String[] docWords, boolean ignoreCase) {
        String word;
        WordFrequency wordFrequency;
        
        for (String aWord:docWords) {
            if (ignoreCase) {
                word = aWord.trim().toLowerCase();
            } else {
                word = aWord.trim();
            }
            if (word.length() > 0) {
                if (docWordWordFrequencyMap.containsKey(word)) {
                    wordFrequency = docWordWordFrequencyMap.get(word);
                    wordFrequency.setFrequency(wordFrequency.getFrequency() + 1);
                }
            }
        }
    }

    private void saveQuestionWordsToDb(Map<String, Long> wordRowNumberMap, Connection conn, String tablePrefix,
            String tableSuffix, Charset charset) throws SQLException, IOException {
        // make "wk_questionword_f_l2h"
        String tmpQuestionWordFilename = "tmpfile_tmpquestionwordfile.csv";
        File tmpQuestionWordFile = new File(tmpQuestionWordFilename);

        Map<String, WordFrequency> wordWordFrequencyMap = initializeWordFrequencyMap(wordRowNumberMap);
        String sql = L2hDbUtils.getSqlSelectQuestionOnQuestionTagTable(tablePrefix, tableSuffix);
        try (PreparedStatement pstmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
                ResultSet rs = pstmt.executeQuery();
                PrintWriter writer = new PrintWriter(
                        new OutputStreamWriter(new FileOutputStream(tmpQuestionWordFile), charset))) {
            while (rs.next()) {
                String questionTitle = rs.getString("title");
                String questionBody = rs.getString("body");
                String questionText = questionTitle + " " + questionBody;

                long questionId = rs.getLong("id");

                String[] wordSequence = SimplePostBodyWordExtractionParser.getWordsFromPostText(questionText);

                Map<String, WordFrequency> docWordWordFrequencyMap = new HashMap<String, WordFrequency>();
                wordWordFrequencyMap.forEach((word, wordFrequency) -> {
                    wordFrequency.setFrequency(0L);
                    docWordWordFrequencyMap.put(word, wordFrequency);
                });
                docWordWordFrequencyMap.forEach((w, f) -> {
                    if (f.getFrequency() != 0L) {
                        throw new RuntimeException("Map docWordWordFrequencyMap isn't initalized.");
                    }
                });

                updateDocWordWordFrequencyMap(docWordWordFrequencyMap, wordSequence, IGNORE_CASE);
                docWordWordFrequencyMap.forEach((word, wordFrequency) -> {
                    if (wordFrequency.getFrequency() > 0L) {
                        writer.println(questionId + "," + wordFrequency.getWordId() + "," + wordFrequency.getFrequency()
                                + "," + wordFrequency.getWordId());
                    }
                });
            }
        }

        sql = L2hDbUtils.getSqlMkL2hQuestionWordTable(tablePrefix, tableSuffix);
        LOGGER.info("Executing sql = " + sql);
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
        }

        CopyManager copyManager = new CopyManager((BaseConnection) conn);
        LOGGER.info("Copying tmpfile_tmpquestionwordfile.csv to table "
                + L2hDbUtils.getL2hQuestionWordTable(tablePrefix, tableSuffix));
        try (InputStreamReader fileStreramReader = new InputStreamReader(new FileInputStream(tmpQuestionWordFile),
                charset)) {
            copyManager.copyIn(
                    "COPY " + L2hDbUtils.getL2hQuestionWordTable(tablePrefix, tableSuffix) + " FROM STDIN WITH CSV",
                    fileStreramReader);
            LOGGER.info(
                    "Populated question word table " + L2hDbUtils.getL2hQuestionWordTable(tablePrefix, tableSuffix));
        }

        String[] sqlMkIndices = L2hDbUtils.getSqlMkL2hQuestionWordTblIndices(tablePrefix, tableSuffix);
        DbUtils.createTableIndices(conn, sqlMkIndices);

        tmpQuestionWordFile.delete();
    }

    private void saveWordsToDb(Map<String, Long> wordRowNumberMap, Connection conn, String tablePrefix,
            String tableSuffix, Charset charset) throws SQLException, IOException {
        String sql = L2hDbUtils.getSqlMkL2hVocabularyTable(tablePrefix, tableSuffix);
        LOGGER.info("Executing sql = " + sql);
        DbUtils.createTable(conn, sql, null, null);

        String tmpWordFilename = "tmpfile_tmpwordfile.csv";
        File tmpWordFile = new File(tmpWordFilename);

        try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tmpWordFile), charset))) {
            wordRowNumberMap.forEach((word, rowNumber) -> {
                writer.println(word + "," + rowNumber + "," + rowNumber);
            });
        }

        CopyManager copyManager = new CopyManager((BaseConnection) conn);
        LOGGER.info("Copying tmpfile_tmpwordfile.csv to table "
                + L2hDbUtils.getL2hVocabularyTable(tablePrefix, tableSuffix));
        try (InputStreamReader fileStreramReader = new InputStreamReader(new FileInputStream(tmpWordFile), charset)) {
            copyManager.copyIn(
                    "COPY " + L2hDbUtils.getL2hVocabularyTable(tablePrefix, tableSuffix) + " FROM STDIN WITH CSV",
                    fileStreramReader);
            LOGGER.info("Populated vocabular table " + L2hDbUtils.getL2hVocabularyTable(tablePrefix, tableSuffix));
        }
        
        String[] sqlMkIndices = L2hDbUtils.getSqlMkL2hVocabularyTblIndices(tablePrefix, tableSuffix);
        LOGGER.info("Executing sql = " + String.join(",", sqlMkIndices));
        DbUtils.createTableIndices(conn, sqlMkIndices);
        
        tmpWordFile.delete();
    }

    private void selectQuestionsFromDb(String dbPropertiesFilename, boolean forceDbWipe,
            String startTimestamp, String endTimestamp,
            String tablePrefix, String tableSuffix, Charset charset) throws SQLException, IOException {
        try (Connection conn = DbUtils.connect(dbPropertiesFilename)) {
            try {
                conn.setAutoCommit(false);
                LOGGER.info("Started db transaction to create dataset tables.");
                if (forceDbWipe) {
                    wipeWorkingDbTables(conn, tablePrefix, tableSuffix);
                }
                // 1. save tags to db
                // make wk_tags_f_l2h
                saveLabelsToDb(conn, tablePrefix, tableSuffix, charset);
                // 2. select questions based tags, and timestamps
                // make wk_questiontag_f_l2h, wk_questionword_f_l2h
                saveQuestionTagsToDbOnLabels(conn, tablePrefix, tableSuffix, startTimestamp, endTimestamp);
                // 3. save the data to db (for preserving prevenance)
                Map<String, Long> wordRowNumberMap = makeWordRowNumberMap(trainingDictionaries.getWordList());
                saveWordsToDb(wordRowNumberMap, conn, tablePrefix, tableSuffix, charset);
                saveQuestionWordsToDb(wordRowNumberMap, conn, tablePrefix, tableSuffix, charset);
                conn.commit();
                LOGGER.info("Committed db transaction to create dataset tables.");
            } catch (SQLException e) {
                conn.rollback();
                LOGGER.info("Rolled back db transaction to create dataset tables.");
                throw new SQLException(e);
            } catch (IOException e) {
                conn.rollback();
                LOGGER.info("Rolled back db transaction to create dataset tables.");
                throw new IOException(e);
            }
        } 
    }

    private void wipeWorkingDbTables(Connection conn, String tablePrefix, String tableSuffix) {
        String table = L2hDbUtils.getL2hTagTable(tablePrefix, tableSuffix);
        if (DbUtils.tableExists(conn, table)) {
            DbUtils.dropTable(conn, table);
        }

        table = L2hDbUtils.getL2hQuestionTagTable(tablePrefix, tableSuffix);
        if (DbUtils.tableExists(conn, table)) {
            DbUtils.dropTable(conn, table);
        }

        table = L2hDbUtils.getL2hVocabularyTable(tablePrefix, tableSuffix);
        if (DbUtils.tableExists(conn, table)) {
            DbUtils.dropTable(conn, table);
        }

        table = L2hDbUtils.getL2hQuestionWordTable(tablePrefix, tableSuffix);
        if (DbUtils.tableExists(conn, table)) {
            DbUtils.dropTable(conn, table);
        }
    }
}
