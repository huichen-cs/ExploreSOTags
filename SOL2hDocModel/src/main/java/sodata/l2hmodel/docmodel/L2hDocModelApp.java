package sodata.l2hmodel.docmodel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
import sampler.labeled.hierarchy.L2H;
import sampling.likelihood.CascadeDirMult.PathAssumption;
import taxonomy.AbstractTaxonomyBuilder;
import taxonomy.BetaTreeBuilder;
import taxonomy.MSTBuilder;
import util.CLIUtils;
import util.IOUtils;
import util.MiscUtils;

public class L2hDocModelApp 
{   
    private final static Logger LOGGER = LoggerFactory.getLogger(L2hDocModelApp.class);
    
    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException, Exception {
        LOGGER.info("DocModelApp starts at " + System.getProperty("user.dir"));
        L2hDocModelApp docModelApp = new L2hDocModelApp();
        
        CommandLine cmdLine = docModelApp.addCommandLineOptions(args);
        if (cmdLine == null) return;
        LOGGER.info("Parsed command line arguments.");
        
        L2hModelParameters modelParameters = null;
        try {
            modelParameters = new L2hDocModelApp().new L2hModelParameters(cmdLine);
        } catch (FileNotFoundException e) {
            LOGGER.error("Cannot load the vocabulary file", e);
            System.exit(-1);
        }
        LOGGER.info("Determined L2h model parameters from the command line arguments and the training dataset.");
        
        if (modelParameters.verbose) {
            docModelApp.logL2hParameters(modelParameters);
        }
        L2H l2hSampler = docModelApp.configuredSampler(modelParameters);
        LOGGER.info("Loaded the trained L2h sampler with samplerFolderPath at " + l2hSampler.getSamplerFolderPath());

        // newWords: D x N_d, where D is the number documents, N_d is the number of words
        TestTextDataset testTextDataSet = new TestTextDataset(modelParameters.datasetName
                , modelParameters.formatFolder);
        int[][] newWords = testTextDataSet.getWords();
        LOGGER.info("loaded new documents.");
        docModelApp.inferNewDocuments(modelParameters
                , l2hSampler
                , newWords);
        LOGGER.info("Completed.");
    }

    public CommandLine addCommandLineOptions(String[] args) throws ParseException {
        // create the command line parser
        CommandLineParser parser = new BasicParser();

        // create the Options
        Options options = new Options();

        // directories
        // L2H has never used data-folder, a bug or feature perhaps
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

        // model parameters
        addValueOption(options, "K", "Number of topics");
        addValueOption(options, "numTopwords", "Number of top words per topic");
        addValueOption(options, "min-label-freq", "Minimum label frequency");

        // model hyperparameters
        addValueOption(options, "alpha", "Hyperparameter of the symmetric Dirichlet prior "
                + "for topic distributions");
        addValueOption(options, "beta", "Hyperparameter of the symmetric Dirichlet prior "
                + "for word distributions");
        addValueOption(options, "a0", "a0");
        addValueOption(options, "b0", "b0");
        addValueOption(options, "path", "Path assumption");
        addValueOption(options, "tree-init", "Tree initialization type");

        addBooleanOption(options, "train", false, "Training if present");
        addBooleanOption(options, "tree", false, "Whether the tree is updated or not");
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
            formatter.printHelp( "SOL2hDocModelApp", options ); 
            return null;
        } else {
            return cmd;
        }
    }
    
    
    public L2H configuredSampler(L2hModelParameters l2hModelParameters) {
        L2H l2h = new L2H();
        
        l2h.configure(l2hModelParameters.outputFolder,
                l2hModelParameters.data.getWordVocab().size(),
                l2hModelParameters.hyperparameters.get(HyperParameterName.ALPHA),
                l2hModelParameters.hyperparameters.get(HyperParameterName.BETA),
                l2hModelParameters.hyperparameters.get(HyperParameterName.A_0),
                l2hModelParameters.hyperparameters.get(HyperParameterName.B_0),
                l2hModelParameters.treeBuilder,
                l2hModelParameters.treeUpdated,
                l2hModelParameters.sampleExact,
                l2hModelParameters.initState,
                l2hModelParameters.pathAssumption,
                l2hModelParameters.paramOptimized,
                l2hModelParameters.burnIn,
                l2hModelParameters.maxIters,
                l2hModelParameters.sampleLag,
                l2hModelParameters.reportInterval);    
        LOGGER.debug(l2h.getName());
        return l2h;
    }

    public void load_test_data(L2hModelParameters modelParameters) {
        LabelTextDataset data = new LabelTextDataset(modelParameters.datasetName);
        data.setFormatFilename(modelParameters.formatFile);
        data.loadFormattedData(modelParameters.formatFolder);
    }
    
    public void inferNewDocuments(L2hModelParameters modelParameters
            , L2H sampler
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
                double[][] initPredictions = computeInitPredictions(newWords, sampler.getL());
                LOGGER.debug("State file: " + stateFile.getAbsolutePath());
                L2HTestRunner runner = new L2HTestRunner(sampler, newWords, stateFile.getAbsolutePath(),
                        partialResultFile.getAbsolutePath(), initPredictions, modelParameters.numTopTopics);
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
    
    private double[][] computeInitPredictions(int[][] newWords, int numUniqTags) {
        LOGGER.debug("newWords.length: " + newWords.length);
        double[][] initPredictions = new double[newWords.length][];
        double p = 1./ (double) numUniqTags;
        for (int i=0; i<newWords.length; i++) {
            initPredictions[i] = new double[numUniqTags];
            LOGGER.debug(String.format("newWords[%d].length: %d", i, newWords[i].length));
            for (int j=0; j<numUniqTags; j++) {
                initPredictions[i][j] = p;
            }
        }
        return initPredictions;
    }


    public void logL2hParameters(L2hModelParameters l2hModelParameters) {

        LOGGER.info("--- folder\t" + l2hModelParameters.outputFolder);
        LOGGER.info("--- label vocab:\t" + l2hModelParameters.data.getLabelVocab().size());
        LOGGER.info("--- word vocab:\t" + l2hModelParameters.data.getWordVocab().size());
        LOGGER.info("--- alpha:\t"
                + MiscUtils.formatDouble(l2hModelParameters.hyperparameters.get(HyperParameterName.ALPHA)));
        LOGGER.info("--- beta:\t"
                + MiscUtils.formatDouble(l2hModelParameters.hyperparameters.get(HyperParameterName.BETA)));
        LOGGER.info(
                "--- a0:\t" + MiscUtils.formatDouble(l2hModelParameters.hyperparameters.get(HyperParameterName.A_0)));
        LOGGER.info(
                "--- b0:\t" + MiscUtils.formatDouble(l2hModelParameters.hyperparameters.get(HyperParameterName.B_0)));
        LOGGER.info("--- burn-in:\t" + l2hModelParameters.burnIn);
        LOGGER.info("--- max iter:\t" + l2hModelParameters.maxIters);
        LOGGER.info("--- sample lag:\t" + l2hModelParameters.sampleLag);
        LOGGER.info("--- paramopt:\t" + l2hModelParameters.paramOptimized);
        LOGGER.info("--- initialize:\t" + l2hModelParameters.initState);
        LOGGER.info("--- path assumption:\t" + l2hModelParameters.pathAssumption);
        LOGGER.info("--- tree builder:\t" + l2hModelParameters.treeBuilder.getName());
        LOGGER.info("--- updating tree?\t" + l2hModelParameters.treeUpdated);
        LOGGER.info("--- exact sampling?\t" + l2hModelParameters.sampleExact);
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
        BETA, 
        A_0, 
        B_0;
    };    
    
    public class L2hModelParameters {
        // String dataFolder;
        private String datasetName;
        private String formatFolder;
        private String outputFolder;
        private String formatFile;
        private int numTopWords;
        private int minLabelFreq;
        private int burnIn;
        private int maxIters;
        private int sampleLag;
        private int reportInterval;
        private HashMap<HyperParameterName, Double> hyperparameters = new HashMap<HyperParameterName, Double>();
        private boolean treeUpdated;
        private boolean sampleExact;
        private PathAssumption pathAssumption;
        private boolean verbose;
        boolean debug;
        
        private LabelTextDataset data;
        
        private boolean paramOptimized;
        
        private InitialState initState;
        
        private AbstractTaxonomyBuilder treeBuilder;
        
        private String reportFolder = "report/";
        
        private String testPrefix = "te_";
        
        private int numTopTopics = 20;

        public L2hModelParameters(CommandLine cmd) throws FileNotFoundException {
            // dataFolder = cmd.getOptionValue("data-folder");
            datasetName = cmd.getOptionValue("dataset");
            formatFolder = cmd.getOptionValue("format-folder");
            outputFolder = cmd.getOptionValue("output");
            formatFile = CLIUtils.getStringArgument(cmd, "format-file", datasetName);
            numTopWords = CLIUtils.getIntegerArgument(cmd, "numTopwords", 20);
            minLabelFreq = CLIUtils.getIntegerArgument(cmd, "min-label-freq", 100);

            burnIn = CLIUtils.getIntegerArgument(cmd, "burnIn", 250);
            maxIters = CLIUtils.getIntegerArgument(cmd, "maxIter", 2);
            sampleLag = CLIUtils.getIntegerArgument(cmd, "sampleLag", 25);
            reportInterval = CLIUtils.getIntegerArgument(cmd, "report", 1);

            double alpha = CLIUtils.getDoubleArgument(cmd, "alpha", 10);
            double beta = CLIUtils.getDoubleArgument(cmd, "beta", 1000);
            double a0 = CLIUtils.getDoubleArgument(cmd, "a0", 90);
            double b0 = CLIUtils.getDoubleArgument(cmd, "b0", 10);
            hyperparameters.put(HyperParameterName.ALPHA, alpha);
            hyperparameters.put(HyperParameterName.BETA, beta);
            hyperparameters.put(HyperParameterName.A_0, a0);
            hyperparameters.put(HyperParameterName.B_0, b0);
            
            
            treeUpdated = cmd.hasOption("tree");
            sampleExact = cmd.hasOption("exact");

            String path = CLIUtils.getStringArgument(cmd, "path", "max");
            switch (path) {
                case "min":
                    pathAssumption = PathAssumption.MINIMAL;
                    break;
                case "max":
                    pathAssumption = PathAssumption.MAXIMAL;
                    break;
                case "antoniak":
                    pathAssumption = PathAssumption.ANTONIAK;
                    break;
                default:
                    throw new RuntimeException(path + " path assumption is not"
                            + " supported. Use min or max.");
            }

            verbose = cmd.hasOption("v");
            debug = cmd.hasOption("d");

            data = new LabelTextDataset(datasetName);
            data.setFormatFilename(formatFile);
            data.loadFormattedData(formatFolder);
            data.filterLabelsByFrequency(minLabelFreq);
            data.prepareTopicCoherence(numTopWords);

            paramOptimized = cmd.hasOption("paramOpt");
            initState = InitialState.PRESET;

            String treeInit = CLIUtils.getStringArgument(cmd, "tree-init", "mst");
            switch (treeInit) {
                case "mst":
                    treeBuilder = new MSTBuilder(data.getLabels(), data.getLabelVocab());
                    break;
                case "beta":
                    double treeAlpha = CLIUtils.getDoubleArgument(cmd, "tree-alpha", 100);
                    double treeA = CLIUtils.getDoubleArgument(cmd, "tree-a", 0.1);
                    double treeB = CLIUtils.getDoubleArgument(cmd, "tree-b", 0.1);
                    treeBuilder = new BetaTreeBuilder(data.getLabels(), data.getLabelVocab(),
                            treeAlpha, treeA, treeB);
                    break;
                default:
                    throw new RuntimeException(treeInit + " not supported");
            }

            File builderFolder = new File(outputFolder, treeBuilder.getName());
            File treeFile = new File(builderFolder, "tree.txt");
            File labelVocFile = new File(builderFolder, "labels.voc");
            if (treeFile.exists()) {
                treeBuilder.inputTree(treeFile);
                treeBuilder.inputLabelVocab(labelVocFile);
            } else {
                throw new RuntimeException("Tree files do not exist. this is an error for inferring new documents.");
            }
            treeBuilder.outputTreeTemp(new File(treeFile + "-temp"));        
          }
    }
}

class L2HTestRunner implements Runnable {

    L2H sampler;
    int[][] newWords;
    String stateFile;
    String outputFile;
    double[][] initPredidctions;
    int topK;

    public L2HTestRunner(L2H sampler,
            int[][] newWords,
            String stateFile,
            String outputFile,
            double[][] initPreds,
            int topK) {
        this.sampler = sampler;
        this.newWords = newWords;
        this.stateFile = stateFile;
        this.outputFile = outputFile;
        this.initPredidctions = initPreds;
        this.topK = topK;
    }

    @Override
    public void run() {
        L2H testSampler = new L2H();
        testSampler.setVerbose(true);
        testSampler.setDebug(false);
        testSampler.setLog(false);
        testSampler.setReport(false);
        testSampler.configure(sampler);
        testSampler.setTestConfigurations(sampler.getBurnIn(), sampler.getMaxIters(), sampler.getSampleLag());

        try {
            testSampler.sampleNewDocuments(stateFile, newWords, outputFile, initPredidctions, topK);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }
}


