/**
 * Making 4 required input files for L2H modeling, a.k.a., Segan's data format
 * label vocabulary (.lvoc): <label-vocab-file>. all labels are indexed from 0.
 * word vocabulary  (.wvoc): <word-vocab-file>. all words are indexed from 0.
 * doc word frequency (.dat): consisting of multiple lines. each line corresponds to a document, and contains
 *                            a list of <word>:<word-frequency>
 * doc label (.docinfo): consisting of multiple lines. each line corresponds to a document, and contains a
 *                       document id followed by a list of labels that are separated by a tab
 * 
 * <dataset-name>: The name of the dataset
 * <output-folder>: Folder to output results
 * <word-vocab-file>: File contains the word vocabulary. Each word type is separated by a new line.
 *       how
 *       to
 *       scan
 *       in
 *       two
 *       arrays
 *       from
 * <doc-word-file>: File contains the formatted text of documents; one doc per line, such as, word id
 * starts from 0
 *       135:1 112:2 86:1 20:3 71:8 116:3 130:1 0:10 ...
 * <doc-info-file>: File contains the document IDs and their associated binary label.
 *     <doc-1-ID>\t<label-doc-1-1>\t<label-doc-1-2>\t ...\n
 *     <doc-2-ID>\t<label-2>\n
 *           ...
 *     <doc-D-ID>\t<label-D>\n
 * 
 * The program takes 3 command line arguments: 
 *     args[0]: dataset name
 *     args[1]: dataset folder
 *     args[2]: working table prefix
 */     

package sodata.processor.segan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.database.FilterDbUtils;
import sodata.database.DbUtils;
import sodata.filter.QuestionWordTagFilter;

public class L2hDataMaker extends L2hDataMakerBase {
    private final static Logger LOGGER = LoggerFactory.getLogger(L2hDataMaker.class);
    
    public static void main(String[] args) {

        if (args.length != 4 && args.length != 9 && args.length != 10) {
            LOGGER.info("Passed " + args.length + " arguments.");
            System.out.println(
                      "L2hDataMaker <dataset_name> <dataset_folder> <working_table_prefix> <database_property_file>\n"
                    + "Or\n"
                    + "L2hDataMaker <dataset_name> <dataset_folder> <working_table_prefix> <database_property_file> <working_table_suffix> <word_no_below> <word_no_above> <tag_no_below> <tag_no_above>\n"
                    + "Or\n"
                    + "L2hDataMaker <dataset_name> <dataset_folder> <working_table_prefix> <database_property_file> <working_table_suffix> <word_no_below> <word_no_above> <tag_no_below> <tag_no_above> --relative\n"            		);
            return;
        }
        
        String datasetName = args[0];
        String datasetFolder = args[1];
        String tablePrefix = args[2];
        String dbPropertiesFilename = args[3];
        String tableSuffix = null;
        
        LOGGER.info("Using the arguments (datasetName, datasetFolder, tablePrefix, dbPropertiesFile): " + datasetName + ", " + datasetFolder + ", " + tablePrefix + ", " + dbPropertiesFilename);
        
        if (args.length == 9) {
            tableSuffix = args[4];
            long wordNoBelow = Long.parseLong(args[5]);
            long wordNoAbove = Long.parseLong(args[6]);
            long tagNoBelow = Long.parseLong(args[7]);
            long tagNoAbove = Long.parseLong(args[8]);
            LOGGER.info("Using the arguments (wordNoBelow, wordNoAbove, tagNoBelow, tagNoAbove): " 
                    + wordNoBelow + ", " + wordNoAbove + ", " + tagNoBelow + ", " + tagNoAbove);
            QuestionWordTagFilter filter = new QuestionWordTagFilter(tableSuffix, dbPropertiesFilename);
            FilterDbUtils.purgeFilteredWorkingTables(tableSuffix, dbPropertiesFilename);
            LOGGER.info("Begin to filter extreme words and tags.");
            if (!filter.filterExtremeWords(wordNoBelow, wordNoAbove, tagNoBelow, tagNoAbove, DbUtils.class)) {
                LOGGER.error("Failed to filter data.");
                return;
            }
            LOGGER.info("Complete filtering extreme words and tags.");
        } else if (args.length == 10) {
        	if (!args[9].equals("--relative")) {
        		LOGGER.error("Unknow option. Only '--relative' is allowed.");
        		return;
        	}
            tableSuffix = args[4];
            long wordNoBelow = Long.parseLong(args[5]);
            double ratioWordNoAbove = Double.parseDouble(args[6]);
            long tagNoBelow = Long.parseLong(args[7]);
            double ratioTagNoAbove = Double.parseDouble(args[8]);
            
            long numberOfQuestions = getNumOfQuestions(tablePrefix, dbPropertiesFilename);
            if (numberOfQuestions <= 0) {
            	LOGGER.error("The number of questions in the data set is 0 or less, incorrect.");
            	return;
            }
            long wordNoAbove = Math.round(ratioWordNoAbove*numberOfQuestions);
            long tagNoAbove = Math.round(ratioTagNoAbove*numberOfQuestions);
            
            LOGGER.info("Using the arguments (wordNoBelow, wordNoAbove, tagNoBelow, tagNoAbove): " 
                    + wordNoBelow + ", " + wordNoAbove + ", " + tagNoBelow + ", " + tagNoAbove);
            QuestionWordTagFilter filter = new QuestionWordTagFilter(tableSuffix, dbPropertiesFilename);
            FilterDbUtils.purgeFilteredWorkingTables(tableSuffix, dbPropertiesFilename);
            LOGGER.info("Begin to filter extreme words and tags.");
            if (!filter.filterExtremeWords(wordNoBelow, wordNoAbove, tagNoBelow, tagNoAbove, DbUtils.class)) {
                LOGGER.error("Failed to filter data.");
                return;
            }
        }
        
        if (!prepareL2hDataset(tablePrefix, tableSuffix, dbPropertiesFilename)) {
            LOGGER.error("Failed to prepare L2H dataset tables.");
            return;
        }
        LOGGER.info("Prepared L2h dataset tables.");
        
        if (!makeDatasetFolder(datasetFolder)) {
            LOGGER.error("Failed to create folder : " + datasetFolder);
            return;
        }
        LOGGER.info("Created dataset folder: " + datasetFolder);
        
        LOGGER.info("Making L2H dataset from database described in " + dbPropertiesFilename + "...");
        if (makeL2HDatasetFiles(datasetName, datasetFolder, tablePrefix, tableSuffix, dbPropertiesFilename)) {
            LOGGER.info("successfully prepared L2H dataset.");
        } else {
            LOGGER.error("successfully prepared L2H dataset.");
        }
    }
}
