package sodata.lldamodel.docmodel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import core.AbstractSampler;
import core.AbstractSampler.InitialState;
import data.LabelTextDataset;
import sampler.labeled.LabeledLDA;
import util.CLIUtils;
import util.IOUtils;
import util.MiscUtils;

public class LLDADocModelApp 
{   
    private final static Logger LOGGER = LoggerFactory.getLogger(LLDADocModelApp.class);
    
    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException, Exception {
        LOGGER.info("DocModelApp starts at " + System.getProperty("user.dir"));
        LLDADocModelApp docModelApp = new LLDADocModelApp();
        
        CommandLine cmdLine = docModelApp.addCommandLineOptions(args);
        if (cmdLine == null) return;
        LOGGER.info("Parsed command line arguments.");
        
        LLDAModelParameters modelParameters = null;
        try {
            modelParameters = new LLDADocModelApp().new LLDAModelParameters(cmdLine);
        } catch (FileNotFoundException e) {
            LOGGER.error("Cannot load the vocabulary file", e);
            System.exit(-1);
        }
        LOGGER.info("Determined LLDA model parameters from the command line arguments and the training dataset.");
        
        if (modelParameters.verbose) {
            docModelApp.logLLDAParameters(modelParameters);
        }
        LabeledLDA lldaSampler = docModelApp.configuredSampler(modelParameters);
        LOGGER.info("Loaded the trained LLDA sampler with samplerFolderPath at " + lldaSampler.getSamplerFolderPath());

        // newWords: D x N_d, where D is the number documents, N_d is the number of words
        TestTextDataset testTextDataSet = new TestTextDataset(modelParameters.datasetName
                , modelParameters.formatFolder);
        int[][] newWords = testTextDataSet.getWords();
        LOGGER.info("loaded new documents.");
        docModelApp.inferNewDocuments(modelParameters
                , lldaSampler
                , newWords);
        LOGGER.info("Completed.");
    }

    public CommandLine addCommandLineOptions(String[] args) throws ParseException {
        // create the command line parser
        CommandLineParser parser = new BasicParser();

        // create the Options
        Options options = new Options();

        // directories
        // LLDA has never used data-folder, a bug or feature perhaps
        addValueOption(options, "data-folder", "Processed data folder"); // data file: doc's term-frequency
        addValueOption(options, "dataset", "Dataset");                     // data file: doc's term-frequency
        addValueOption(options, "format-folder", "Folder holding formatted data"); // label file
        addValueOption(options, "format-file", "Formatted file name");             // label file
        addValueOption(options, "output", "Output folder");

        // sampling configurations
        addValueOption(options, "burnIn", "Burn-in");
        addValueOption(options, "maxIter", "Maximum number of iterations");
        addValueOption(options, "sampleLag", "Sample lag");
        addValueOption(options, "report", "Report interval");

        // model hyperparameters
        addValueOption(options, "alpha", "Hyperparameter of the symmetric Dirichlet prior "
                + "for topic distributions");
        addValueOption(options, "beta", "Hyperparameter of the symmetric Dirichlet prior "
                + "for word distributions");

        addBooleanOption(options, "paramOpt", false, "Whether hyperparameter "
                + "optimization using slice sampling is performed");
        addBooleanOption(options, "v", false, "verbose if present");
        addBooleanOption(options, "d", false, "debug if present");
        addBooleanOption(options, "help", false, "Display help message if present");

        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("help") 
                || !cmd.hasOption("dataset")
//                || !cmd.hasOption("data-folder")
                || !cmd.hasOption("format-folder")
                || !cmd.hasOption("format-file")
                || !cmd.hasOption("output")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "SOLLDADocModelApp", options ); 
            return null;
        } else {
            return cmd;
        }
    }
    
    
    public LabeledLDA configuredSampler(LLDAModelParameters lldaModelParameters) {
        LabeledLDA llda = new LabeledLDA();
        
        llda.configure(lldaModelParameters.outputFolder,
                lldaModelParameters.data.getWordVocab().size(),
                lldaModelParameters.data.getLabelVocab().size(),
                lldaModelParameters.hyperparameters.get(HyperParameterName.ALPHA),
                lldaModelParameters.hyperparameters.get(HyperParameterName.BETA),
                lldaModelParameters.initState,
                lldaModelParameters.paramOptimized,
                lldaModelParameters.burnIn,
                lldaModelParameters.maxIters,
                lldaModelParameters.sampleLag,
                lldaModelParameters.reportInterval);    
        LOGGER.debug(llda.getName());
        return llda;
    }

    public void load_test_data(LLDAModelParameters modelParameters) {
        LabelTextDataset data = new LabelTextDataset(modelParameters.datasetName);
        data.setFormatFilename(modelParameters.formatFile);
        data.loadFormattedData(modelParameters.formatFolder);
    }
    
    public void inferNewDocuments(LLDAModelParameters modelParameters
            , LabeledLDA sampler
            , int[][] newWords) {
        File samplerFolder = new File(sampler.getSamplerFolderPath());
        File iterPredFolder = new File(samplerFolder, AbstractSampler.IterPredictionFolder);
        
        
        IOUtils.createFolder(iterPredFolder);
        File testStateFolder = new File(samplerFolder
                , modelParameters.testPrefix + AbstractSampler.ReportFolder);
        IOUtils.createFolder(testStateFolder);
        
        File reportFolder = new File(sampler.getSamplerFolderPath(), modelParameters.reportFolder);
        if (!reportFolder.exists()) {
            // IOUtils.createFolder(reportFolder);
            throw new RuntimeException("Report folder not found. " + reportFolder);
        }
        String[] filenames = reportFolder.list();
        try {
            IOUtils.createFolder(iterPredFolder);
            ArrayList<Thread> threads = new ArrayList<Thread>();
            for (String filename : filenames) {
                if (!filename.contains("zip")) {
                    continue;
                }
                
                String baseFilename = IOUtils.removeExtension(filename);
                String[] baseFilenameParts = baseFilename.split("-");
                int iterNum = Integer.parseInt(baseFilenameParts[1]);
                LOGGER.debug("Iter number: " + iterNum);
                if (iterNum != modelParameters.maxIters) {
                    continue;
                }

                LOGGER.debug("report filename: " + filename);
                // folder contains multiple samples during test using a learned model
                File stateFile = new File(reportFolder, filename);
                File partialResultFile = new File(iterPredFolder, IOUtils.removeExtension(filename) + ".txt");

                LOGGER.info(String.format("newWords[%d][%d] read", newWords.length, newWords[0].length));
                LOGGER.debug("State file: " + stateFile.getAbsolutePath());
                LLDATestRunner runner = new LLDATestRunner(sampler, newWords, stateFile.getAbsolutePath(),
                        partialResultFile.getAbsolutePath());
                Thread thread = new Thread(runner);
                threads.add(thread);
            }
            LOGGER.info("Start running threads for infer new documents.");
            AbstractSampler.runThreads(threads); // run MAX_NUM_PARALLEL_THREADS threads at a time
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Exception while sampling during parallel test.");
        }
    }

    public void logLLDAParameters(LLDAModelParameters lldaModelParameters) {

        LOGGER.info("--- folder\t" + lldaModelParameters.outputFolder);
        LOGGER.info("--- label vocab:\t" + lldaModelParameters.data.getLabelVocab().size());
        LOGGER.info("--- word vocab:\t" + lldaModelParameters.data.getWordVocab().size());
        LOGGER.info("--- alpha:\t"
                + MiscUtils.formatDouble(lldaModelParameters.hyperparameters.get(HyperParameterName.ALPHA)));
        LOGGER.info("--- beta:\t"
                + MiscUtils.formatDouble(lldaModelParameters.hyperparameters.get(HyperParameterName.BETA)));
        LOGGER.info("--- burn-in:\t" + lldaModelParameters.burnIn);
        LOGGER.info("--- max iter:\t" + lldaModelParameters.maxIters);
        LOGGER.info("--- sample lag:\t" + lldaModelParameters.sampleLag);
        LOGGER.info("--- paramopt:\t" + lldaModelParameters.paramOptimized);
        LOGGER.info("--- initialize:\t" + lldaModelParameters.initState);
    }
    
    private Options addValueOption(Options options, String optName, String optDesc) {
        OptionBuilder.withLongOpt(optName);
        OptionBuilder.withDescription(optDesc);
        OptionBuilder.hasArg();
        OptionBuilder.withArgName(optName);
        return options.addOption(OptionBuilder.create());        
    }
    
    private Options addBooleanOption(Options options, String optName, boolean hasArg, String optDesc) {
        return options.addOption(optName, hasArg, optDesc);
    }



    private enum HyperParameterName {
        ALPHA, 
        BETA
    };    
    
    public class LLDAModelParameters {
        // String dataFolder;
        private String datasetName;
        private String formatFolder;
        private String outputFolder;
        private String formatFile;
        private int burnIn;
        private int maxIters;
        private int sampleLag;
        private int reportInterval;
        private HashMap<HyperParameterName, Double> hyperparameters = new HashMap<HyperParameterName, Double>();
        private boolean verbose;
        
        private LabelTextDataset data;
        
        private ArrayList<String> filteredLabelVoc;
        
        private boolean paramOptimized;
        
        private InitialState initState;
        
       
        private String reportFolder = "report/";
        
        private String testPrefix = "te_";

        public LLDAModelParameters(CommandLine cmd) throws IOException {
            // dataFolder = cmd.getOptionValue("data-folder");
            datasetName = cmd.getOptionValue("dataset");
            formatFolder = cmd.getOptionValue("format-folder");
            outputFolder = cmd.getOptionValue("output");
            formatFile = CLIUtils.getStringArgument(cmd, "format-file", datasetName);

            burnIn = CLIUtils.getIntegerArgument(cmd, "burnIn", 250);
            maxIters = CLIUtils.getIntegerArgument(cmd, "maxIter", 2);
            sampleLag = CLIUtils.getIntegerArgument(cmd, "sampleLag", 25);
            reportInterval = CLIUtils.getIntegerArgument(cmd, "report", 1);

            double alpha = CLIUtils.getDoubleArgument(cmd, "alpha", 10);
            double beta = CLIUtils.getDoubleArgument(cmd, "beta", 1000);
            hyperparameters.put(HyperParameterName.ALPHA, alpha);
            hyperparameters.put(HyperParameterName.BETA, beta);

            verbose = cmd.hasOption("v");

            data = new LabelTextDataset(datasetName);
            data.setFormatFilename(formatFile);
            data.loadFormattedData(formatFolder);
            
            filteredLabelVoc = loadFilteredLabelVoc(outputFolder);
            data.resetLabelVocab(filteredLabelVoc);
            
            //         data.filterLabelsByFrequency(minLabelFreq); --> matching filtering

            paramOptimized = cmd.hasOption("paramOpt");
            // initState = InitialState.PRESET;  
            initState = InitialState.RANDOM;  
          }
        

        private ArrayList<String> loadFilteredLabelVoc(String formatFolder) throws IOException {
            final ArrayList<String> filteredLabelVoc = new ArrayList<String>();
            Path path = Paths.get(formatFolder, "filtered", "labels.voc");
            if (!Files.exists(path)) {
                throw new IllegalStateException("Filtered label vocabular file does not exists: " + path.toString());
            }
            Files.readAllLines(path).forEach(label -> filteredLabelVoc.add(label));
            return filteredLabelVoc;
        }
    }

}

class LLDATestRunner implements Runnable {

    private LabeledLDA sampler;
    private int[][] newWords;
    private String stateFile;
    private String outputFile;


    public LLDATestRunner(LabeledLDA sampler,
            int[][] newWords,
            String stateFile,
            String outputFile) {
        this.sampler = sampler;
        this.newWords = newWords;
        this.stateFile = stateFile;
        this.outputFile = outputFile;
    }

    @Override
    public void run() {
        LabeledLDA testSampler = new LabeledLDA();
        testSampler.setVerbose(true);
        testSampler.setDebug(false);
        testSampler.setLog(false);
        testSampler.setReport(false);
        testSampler.configure(sampler);
        testSampler.setTestConfigurations(sampler.getBurnIn(), sampler.getMaxIters(), sampler.getSampleLag());

        try {
            testSampler.sampleNewDocuments(stateFile, newWords, outputFile);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }
}


