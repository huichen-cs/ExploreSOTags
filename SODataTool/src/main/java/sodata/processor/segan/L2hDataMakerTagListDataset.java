package sodata.processor.segan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.database.DbTagSelectorUtils;
import sodata.database.DbUtils;
import sodata.database.FilterDbUtils;
import sodata.filter.QuestionWordTagFilter;

public class L2hDataMakerTagListDataset extends L2hDataMakerBase {
    private final static Logger LOGGER = LoggerFactory.getLogger(L2hDataMakerTagListDataset.class);
    
    public static void main(String[] args) {

        if (args.length != 4 && args.length != 9 && args.length != 10) {
            LOGGER.info("Passed " + args.length + " arguments.");
            System.out.println(
                      "L2hDataMakerTagListDataset <dataset_name> <dataset_folder> <working_table_prefix> <database_property_file>\n"
                    + "Or\n"
                    + "L2hDataMakerTagListDataset <dataset_name> <dataset_folder> <working_table_prefix> <database_property_file> <working_table_suffix> <word_no_below> <word_no_above> <tag_no_below> <tag_no_above>\n"
                    + "Or\n"
                    + "L2hDataMakerTagListDataset <dataset_name> <dataset_folder> <working_table_prefix> <database_property_file> <working_table_suffix> <word_no_below> <word_no_above> <tag_no_below> <tag_no_above> --relative\n"                  );
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
            if (!filter.filterExtremeWords(wordNoBelow, wordNoAbove, tagNoBelow, tagNoAbove, DbTagSelectorUtils.class)) {
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
