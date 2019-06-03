package sodata.processor.segan;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sodata.WordFrequency;
import sodata.database.DbUtils;
import sodata.database.FilterDbUtils;
import sodata.database.L2hDbUtils;

public class L2hDataMakerBase {
    private final static Logger LOGGER = LoggerFactory.getLogger(L2hDataMakerBase.class);
    
    public static boolean prepareL2hDataset(String tablePrefix, String tableSuffix,
            String dbPropertiesFilename) {
        try (Connection conn = DbUtils.connect(dbPropertiesFilename)) {
            conn.setAutoCommit(false);
            
            if (!purgeL2hTables(conn, tablePrefix, tableSuffix)) {
                conn.rollback();
                return false;
            }
            LOGGER.info("Purged L2h tables.");
            
            if (!makeL2hWordVocabularyTable(conn, tablePrefix, tableSuffix)) {
                conn.rollback();
                return false;
            }
            LOGGER.info("made L2h word vocabulary table.");
            
            if (!makeL2hQuestionWordTable(conn, tablePrefix, tableSuffix)) {
                conn.rollback();
                return false;
            }
            LOGGER.info("made L2h question-word table.");
            
            if (!makeL2hTagTable(conn, tablePrefix, tableSuffix)) {
                conn.rollback();
                return false;
            }
            LOGGER.info("made L2h tag table.");
            
            if (!makeL2hQuestionTagTable(conn, tablePrefix, tableSuffix)) {
                conn.rollback();
                return false;
            }
            LOGGER.info("made l2h question-tag table.");
            
            if (!checkingOnQuestionsIds(conn, tablePrefix, tableSuffix)) {
                LOGGER.error("===============================");
                LOGGER.error("===============================");
                LOGGER.error("l2h tables failed sanity check.");
                LOGGER.error("===============================");
                LOGGER.error("===============================");
                // conn.rollback();
                // return false;
            }
            LOGGER.info("passed sanity check on questions in question-tag and question-word table.");
            
            conn.commit();
            LOGGER.info("Prepared tables for L2H dataset.");
            return true;
        } catch (SQLException e) {
            LOGGER.error("failed to process data.", e);
            return false;
        }
    }


    protected static long getNumOfQuestions(String tablePrefix, String dbPropertiesFilename) {
        DbUtils.setWorkingTablePrefix(tablePrefix);
        
        try (Connection conn = DbUtils.connect(dbPropertiesFilename);
                PreparedStatement pstmt = conn.prepareStatement(DbUtils.getSqlQuestionCount());
                ResultSet rs = pstmt.executeQuery()) {
            if (rs.next()) {
                long numberOfQuestions = rs.getLong(1);
                return numberOfQuestions;
            } else {
                return -1L;
            }
        } catch (SQLException e) {
            LOGGER.error("failed to fetch the number of questions from working tables in the database: ", e);
            return -1L;
        }
    }
    
    private static boolean makeL2hQuestionTagTable(Connection conn, String tablePrefix, String tableSuffix) {
        String sqlMkTbl = L2hDbUtils.getSqlMkL2hQuestionTagTableFromSelect(tablePrefix, tableSuffix);
        LOGGER.info("To execute sql: " + sqlMkTbl);
        try (PreparedStatement pstmt = conn.prepareStatement(sqlMkTbl)) {
            pstmt.executeUpdate();
            return DbUtils.createTable(conn, null, null,
                    L2hDbUtils.getSqlMkL2hQuestionTagTblIndices(tablePrefix, tableSuffix));
        } catch (SQLException e) {
            LOGGER.error("failed to create table and its indices: " + L2hDbUtils.getL2hQuestionTagTable(tablePrefix, tableSuffix), e);
            return false;
        }
    }

    private static boolean makeL2hTagTable(Connection conn, String tablePrefix, String tableSuffix) {
        String sqlMkTbl = L2hDbUtils.getSqlMkL2hTagTableFromSelect(tablePrefix, tableSuffix);
        LOGGER.info("To execute sql: " + sqlMkTbl);
        try (PreparedStatement pstmt = conn.prepareStatement(sqlMkTbl)) {
            pstmt.executeUpdate();
            return DbUtils.createTable(conn, null, null,
                    L2hDbUtils.getSqlMkL2hTagTblIndices(tablePrefix, tableSuffix));
        } catch (SQLException e) {
            LOGGER.error("failed to create table and its indices: " + L2hDbUtils.getL2hTagTable(tablePrefix, tableSuffix), e);
            return false;
        }
    }

    private static boolean makeL2hQuestionWordTable(Connection conn, String tablePrefix, String tableSuffix) {
        String sqlMkTbl = L2hDbUtils.getSqlMkL2hQuestionWordTableFromSelect(tablePrefix, tableSuffix);
        LOGGER.info("To execute sql: " + sqlMkTbl);
        try (PreparedStatement pstmt = conn.prepareStatement(sqlMkTbl)) {
            pstmt.executeUpdate();
            return DbUtils.createTable(conn, null, null,
                    L2hDbUtils.getSqlMkL2hQuestionWordTblIndices(tablePrefix, tableSuffix));
        } catch (SQLException e) {
            LOGGER.error("failed to create table and its indices: " + L2hDbUtils.getL2hQuestionWordTable(tablePrefix, tableSuffix), e);
            return false;
        }
    }    
        
    private static boolean makeL2hWordVocabularyTable(Connection conn, String tablePrefix, String tableSuffix) {
        String sqlMkTbl = L2hDbUtils.getSqlMkL2hVocabularyTableFromSelect(tablePrefix, tableSuffix);
        LOGGER.info("Sql: " + sqlMkTbl);
        try (PreparedStatement pstmt = conn.prepareStatement(sqlMkTbl)) {
            pstmt.executeUpdate();
            return DbUtils.createTable(conn, null, null,
                    L2hDbUtils.getSqlMkL2hVocabularyTblIndices(tablePrefix, tableSuffix));
        } catch (SQLException e) {
            LOGGER.error("failed to create table and its indices: " + L2hDbUtils.getL2hVocabularyTable(tablePrefix, tableSuffix), e);
            return false;
        }
    }
    
    public static boolean makeDatasetFolder(String datasetFolder) {
        try {
            Files.createDirectories(Paths.get(datasetFolder));
            return true;
        } catch (IOException e) {
            LOGGER.error("Cannot create driectory " + datasetFolder + ".", e);
            return false;
        }
    } 
    

    private static boolean purgeL2hTables(Connection conn, String tablePrefix, String tableSuffix) {
        String[] sqlDropTables = {
                "DROP TABLE IF EXISTS " + L2hDbUtils.getL2hVocabularyTable(tablePrefix, tableSuffix),
                "DROP TABLE IF EXISTS " + L2hDbUtils.getL2hQuestionWordTable(tablePrefix, tableSuffix),
                "DROP TABLE IF EXISTS " + L2hDbUtils.getL2hTagTable(tablePrefix, tableSuffix),
                "DROP TABLE IF EXISTS " + L2hDbUtils.getL2hQuestionTagTable(tablePrefix, tableSuffix)               
        };
        for (String sql:sqlDropTables) {
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.executeUpdate();
            } catch (SQLException e) {
                LOGGER.error("Failed to drop L2H tables.", e);
                return false;
            }
        }
        return true;
    }    
    
    public static boolean cleanDatasetFolder(String datasetFolder) {
        Path directory = Paths.get(datasetFolder);
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
               @Override
               public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                   Files.delete(file);
                   return FileVisitResult.CONTINUE;
               }

               @Override
               public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                   Files.delete(dir);
                   return FileVisitResult.CONTINUE;
               }
            });
            return true;
        } catch (IOException e) {
            LOGGER.error("Failed to clean the folder " + datasetFolder + ".", e);
            return false;
        }        
    }    
    
    public static Path getWordVocabularyFilePath(String datasetFolder, String datasetName) {
        return Paths.get(datasetFolder, datasetName + ".wvoc");
    }
    
    public static Path getTagVocabularyFilePath(String datasetFolder, String datasetName) {
        return Paths.get(datasetFolder, datasetName + ".lvoc"); 
    }
    
    public static Path getQuestionWordFreqFilePath(String datasetFolder, String datasetName) {
        return Paths.get(datasetFolder, datasetName + ".dat"); 
    }
    
    public static Path getQuestionTagFilePath(String datasetFolder, String datasetName) {
        return Paths.get(datasetFolder, datasetName + ".docinfo");
    }
    
    public static boolean makeL2HDatasetFiles(String datasetName, 
            String datasetFolder, String tablePrefix, String tableSuffix, String dbPropertiesFilename) {

        LOGGER.info("Making word-vocabulary file (.wvoc file) ...");
        if (!makeWordVocabularyFile(datasetFolder, datasetName, tablePrefix, tableSuffix, dbPropertiesFilename)) {
            LOGGER.error("Failed to make word vocabulary file.");
            return false;
        }
        LOGGER.info("Created word-vocabulary file.");
        
        LOGGER.info("Making question-word-frequency file (.dat file) ...");
        if (!makeQuestionWordFreqFile(datasetFolder, datasetName, tablePrefix, tableSuffix, dbPropertiesFilename)) {
            LOGGER.error("Failed to make doc-word-frequency file.");
            return false;
        }
        LOGGER.info("Created question-word-frequency file (.dat file).");
        
        LOGGER.info("Making tag-vocabulary file (.lvoc file) ...");
        if (!makeTagVocabularyFile(datasetFolder, datasetName, tablePrefix, tableSuffix, dbPropertiesFilename)) {
            LOGGER.error("Failed to make tag vocabulary file.");
            return false;        
        }
        LOGGER.info("Created tag-vocabulary file.");
        
        LOGGER.info("Making question-tab file (.docinfo file) ...");
        if (!makeQuestionTagFile(datasetFolder, datasetName, tablePrefix, tableSuffix, dbPropertiesFilename)) {
            LOGGER.error("Failed to make doc-tag file.");
            return false;
        }
        LOGGER.info("Created question-tab file (.docinfo file).");
        
        return true;
    }
    
    public static boolean makeFilteredL2HDatasetFiles(String datasetName, 
            String datasetFolder, String tablePrefix, String tableSuffix, String dbPropertiesFilename) {

        LOGGER.info("Making word-vocabulary file (.wvoc file) ...");
        if (!makeWordVocabularyFile(datasetFolder, datasetName, tablePrefix, tableSuffix, dbPropertiesFilename)) {
            LOGGER.error("Failed to make word vocabulary file.");
            return false;
        }
        LOGGER.info("Created word-vocabulary file.");
        
        LOGGER.info("Making question-word-frequency file (.dat file) ...");
        if (!makeFilteredQuestionWordFreqFile(datasetFolder, datasetName, tablePrefix, tableSuffix, dbPropertiesFilename)) {
            LOGGER.error("Failed to make doc-word-frequency file.");
            return false;
        }
        LOGGER.info("Created question-word-frequency file (.dat file).");
        
        LOGGER.info("Making tag-vocabulary file (.lvoc file) ...");
        if (!makeTagVocabularyFile(datasetFolder, datasetName, tablePrefix, tableSuffix, dbPropertiesFilename)) {
            LOGGER.error("Failed to make tag vocabulary file.");
            return false;        
        }
        LOGGER.info("Created tag-vocabulary file.");
        
        LOGGER.info("Making question-tab file (.docinfo file) ...");
        if (!makeFilteredQuestionTagFile(datasetFolder, datasetName, tablePrefix, tableSuffix, dbPropertiesFilename)) {
            LOGGER.error("Failed to make doc-tag file.");
            return false;
        }
        LOGGER.info("Created question-tab file (.docinfo file).");
        
        return true;
    }

    private static boolean makeWordVocabularyFile(String datasetFolder, 
            String datasetName, String tablePrefix, String tableSuffix, String dbPropertiesFilename) {
        Path vocFilePath = getWordVocabularyFilePath(datasetFolder, datasetName);
        File vocFile = new File(vocFilePath.toString());
        LOGGER.info("L2H vocabulary will be written to " + vocFile.getAbsolutePath());
        
        String sql = "SELECT word FROM " + L2hDbUtils.getL2hVocabularyTable(tablePrefix, tableSuffix) + " ORDER BY newid";
        LOGGER.info("To execute query: " + sql + ".");
        try (Connection conn = DbUtils.connect(dbPropertiesFilename);
                PreparedStatement pstmt = conn.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery();
                PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(vocFile), StandardCharsets.UTF_8))) {
            LOGGER.info("Processing query result from query: " + sql + ".");
            while (rs.next()) {
                writer.println(rs.getString(1));
            }
            LOGGER.info("Wrote vocabulary to " + vocFilePath.toString());
            return true;
        } catch (SQLException | FileNotFoundException e) {
            LOGGER.error("failed to process data.", e);
            return false;
        }        
    }
    
    private static boolean makeQuestionWordFreqFile(String datasetFolder, String datasetName, String tablePrefix,
            String tableSuffix, String dbPropertiesFilename) {
        Path datFilePath = getQuestionWordFreqFilePath(datasetFolder, datasetName);
        File datFile = new File(datFilePath.toString());
        LOGGER.info("L2H vocabulary will be written to " + datFile.getAbsolutePath());

        String sql = "SELECT postid, newwordid, wordcount FROM " 
            + L2hDbUtils.getL2hQuestionWordTable(tablePrefix, tableSuffix)
            + " ORDER BY postid, newwordid";
        LOGGER.info("To execute query: " + sql + ".");
        try (Connection conn = DbUtils.connect(dbPropertiesFilename)) {
            // query can be big, using fetch size
            int docCounter = 0;
            conn.setAutoCommit(false);
            try (PreparedStatement pstmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
                pstmt.setFetchSize(FilterDbUtils.FETCH_BATCH_SIZE);
                try (ResultSet rs = pstmt.executeQuery(); 
                        PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(datFile), StandardCharsets.UTF_8))) {
                    LOGGER.info("Processing query result from query: " + sql + ".");
                    long prevPostId = -1;;
                    long workCounter = 0l;
                    List<WordFrequency> qWordFreqList = new LinkedList<WordFrequency>();
                    while (rs.next()) {
                        long postId = rs.getLong(1);
                        long wordId = rs.getLong(2);
                        long wordCount = rs.getLong(3);

                        if (prevPostId >= 0 && prevPostId != postId) { // new
                                                                       // doc
                                                                       // begin
                            writer.print(qWordFreqList.size() + " ");
                            for(WordFrequency wordFreq:qWordFreqList) {
                                writer.print(wordFreq.wordId + ":" + wordFreq.frequency + " ");
                            }
                            writer.println();
                            docCounter ++;
                            qWordFreqList.clear();
                            qWordFreqList.add(new WordFrequency(wordId, wordCount));
                        } else {
                            qWordFreqList.add(new WordFrequency(wordId, wordCount));
                        }
                        prevPostId = postId;
                        workCounter ++;
                        if (workCounter % FilterDbUtils.LOG_WORK_COUNTER == 0) {
                            LOGGER.info("Processed " + workCounter + " post-word counts.");
                        }
                    }
                    writer.print(qWordFreqList.size() + " ");
                    for(WordFrequency wordFreq:qWordFreqList) {
                        writer.print(wordFreq.wordId + ":" + wordFreq.frequency + " ");
                    }
                    writer.println();
                    qWordFreqList.clear();                   
                    LOGGER.info("Completed processing " + workCounter + " post-word counts.");
                    LOGGER.info("Processed " + docCounter + " documents.");
                    LOGGER.info("Wrote data file to " + datFilePath.toString());
                }
            }
            return true;
        } catch (SQLException | FileNotFoundException e) {
            LOGGER.error("failed to process data.", e);
            return false;
        }
    }
    
    private static boolean makeFilteredQuestionWordFreqFile(String datasetFolder, String datasetName, String tablePrefix,
            String tableSuffix, String dbPropertiesFilename) {
        Path datFilePath = getQuestionWordFreqFilePath(datasetFolder, datasetName);
        File datFile = new File(datFilePath.toString());
        LOGGER.info("L2H vocabulary will be written to " + datFile.getAbsolutePath());

        String sql = "SELECT w.postid, w.newwordid, w.wordcount FROM " 
            + L2hDbUtils.getL2hQuestionWordTable(tablePrefix, tableSuffix) + " AS w, "
            + L2hDbUtils.getL2hQuestionTagTable(tablePrefix, tableSuffix) + " As t "
            + " WHERE w.postid=t.postid"
            + " ORDER BY w.postid, w.newwordid";
        LOGGER.info("To execute query: " + sql + ".");
        try (Connection conn = DbUtils.connect(dbPropertiesFilename)) {
            // query can be big, using fetch size
            int docCounter = 0;
            conn.setAutoCommit(false);
            try (PreparedStatement pstmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
                pstmt.setFetchSize(FilterDbUtils.FETCH_BATCH_SIZE);
                try (ResultSet rs = pstmt.executeQuery(); 
                        PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(datFile), StandardCharsets.UTF_8))) {
                    LOGGER.info("Processing query result from query: " + sql + ".");
                    long prevPostId = -1;;
                    long workCounter = 0l;
                    List<WordFrequency> qWordFreqList = new LinkedList<WordFrequency>();
                    while (rs.next()) {
                        long postId = rs.getLong(1);
                        long wordId = rs.getLong(2);
                        long wordCount = rs.getLong(3);

                        if (prevPostId >= 0 && prevPostId != postId) { // new
                                                                       // doc
                                                                       // begin
                            writer.print(qWordFreqList.size() + " ");
                            for(WordFrequency wordFreq:qWordFreqList) {
                                writer.print(wordFreq.wordId + ":" + wordFreq.frequency + " ");
                            }
                            writer.println();
                            docCounter ++;
                            qWordFreqList.clear();
                            qWordFreqList.add(new WordFrequency(wordId, wordCount));
                        } else {
                            qWordFreqList.add(new WordFrequency(wordId, wordCount));
                        }
                        prevPostId = postId;
                        workCounter ++;
                        if (workCounter % FilterDbUtils.LOG_WORK_COUNTER == 0) {
                            LOGGER.info("Processed " + workCounter + " post-word counts.");
                        }
                    }
                    writer.print(qWordFreqList.size() + " ");
                    for(WordFrequency wordFreq:qWordFreqList) {
                        writer.print(wordFreq.wordId + ":" + wordFreq.frequency + " ");
                    }
                    writer.println();
                    qWordFreqList.clear();                   
                    LOGGER.info("Completed processing " + workCounter + " post-word counts.");
                    LOGGER.info("Processed " + docCounter + " documents.");
                    LOGGER.info("Wrote data file to " + datFilePath.toString());
                }
            }
            return true;
        } catch (SQLException | FileNotFoundException e) {
            LOGGER.error("failed to process data.", e);
            return false;
        }
    }

    
    private static boolean makeTagVocabularyFile(String datasetFolder, String datasetName, 
            String tablePrefix, String tableSuffix,
            String dbPropertiesFilename) {
        Path tagFilePath = getTagVocabularyFilePath(datasetFolder, datasetName);
        File tagFile = new File(tagFilePath.toString());
        LOGGER.info("L2H Label Vocaublary will be written to " + tagFile.getAbsolutePath());
        
        String sql = "SELECT tagname FROM " + L2hDbUtils.getL2hTagTable(tablePrefix, tableSuffix) + " ORDER BY newtagid";
        LOGGER.info("To execute query: " + sql + ".");
        try (Connection conn = DbUtils.connect(dbPropertiesFilename);
                PreparedStatement pstmt = conn.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery();
                PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tagFile), StandardCharsets.UTF_8))) {
            LOGGER.info("Processing query result from query: " + sql + ".");
            while (rs.next()) {
                String tagName = rs.getString(1);
                
                writer.println(tagName);
            }
        
            LOGGER.info("Wrote label vocabulary file to " + tagFilePath.toString());
            return true;
        } catch (SQLException | FileNotFoundException e) {
            LOGGER.error("failed to process data.", e);
            return false;
        }                 
    }    
    

    private static boolean makeQuestionTagFile(String datasetFolder, String datasetName, 
            String tablePrefix, String tableSuffix,
            String dbPropertiesFilename) {
        Path docTagFilePath = getQuestionTagFilePath(datasetFolder, datasetName);
        File docTagFile = new File(docTagFilePath.toString());
        LOGGER.info("L2H doc-tag file will be written to " + docTagFile.getAbsolutePath());
        
        String sql = "SELECT postid,newtagid FROM " + L2hDbUtils.getL2hQuestionTagTable(tablePrefix, tableSuffix) + " ORDER BY postid,newtagid";
        LOGGER.info("To execute query: " + sql + ".");
        try (Connection conn = DbUtils.connect(dbPropertiesFilename);
                PreparedStatement pstmt = conn.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery();
                PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(docTagFile), StandardCharsets.UTF_8))) {
            LOGGER.info("Processing query result from query: " + sql + ".");
            long prevPostId = -1;
            while (rs.next()) {
                long postId = rs.getLong(1);
                long tagId = rs.getLong(2);
                
                if (prevPostId != postId) { // new doc begin
                    if (prevPostId >= 0) writer.println();
                    writer.print(Long.toString(postId) + "\t" + Long.toString(tagId));
                } else {
                    writer.print("\t" + Long.toString(tagId));
                }
                prevPostId = postId;
            }          
            LOGGER.info("Wrote doc-tag file to " + docTagFilePath.toString());
            return true;
        } catch (SQLException | FileNotFoundException e) {
            LOGGER.error("failed to process data.", e);
            return false;
        }  
    }
    

    private static boolean makeFilteredQuestionTagFile(String datasetFolder, String datasetName, 
            String tablePrefix, String tableSuffix,
            String dbPropertiesFilename) {
        Path docTagFilePath = getQuestionTagFilePath(datasetFolder, datasetName);
        File docTagFile = new File(docTagFilePath.toString());
        LOGGER.info("L2H doc-tag file will be written to " + docTagFile.getAbsolutePath());
        
        String sql = "SELECT t.postid,t.newtagid FROM " 
                + L2hDbUtils.getL2hQuestionTagTable(tablePrefix, tableSuffix) + " AS t, "
                + L2hDbUtils.getL2hQuestionWordTable(tablePrefix, tableSuffix) + " As w "
                + " WHERE t.postid=w.postid "
                + " ORDER BY postid,newtagid";
        LOGGER.info("To execute query: " + sql + ".");
        try (Connection conn = DbUtils.connect(dbPropertiesFilename);
                PreparedStatement pstmt = conn.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery();
                PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(docTagFile), StandardCharsets.UTF_8))) {
            LOGGER.info("Processing query result from query: " + sql + ".");
            long prevPostId = -1;
            while (rs.next()) {
                long postId = rs.getLong(1);
                long tagId = rs.getLong(2);
                
                if (prevPostId != postId) { // new doc begin
                    if (prevPostId >= 0) writer.println();
                    writer.print(Long.toString(postId) + "\t" + Long.toString(tagId));
                } else {
                    writer.print("\t" + Long.toString(tagId));
                }
                prevPostId = postId;
            }          
            LOGGER.info("Wrote doc-tag file to " + docTagFilePath.toString());
            return true;
        } catch (SQLException | FileNotFoundException e) {
            LOGGER.error("failed to process data.", e);
            return false;
        }  
    }
    
    public static boolean checkingOnQuestionsIds(Connection conn, String tablePrefix, String tableSuffix) {
        String sqlQt = "SELECT DISTINCT postid FROM " 
                + L2hDbUtils.getL2hQuestionTagTable(tablePrefix, tableSuffix)
                + " ORDER BY postid";
        LOGGER.info("SqlQT: " + sqlQt);
        String sqlQw = "SELECT DISTINCT postid FROM " 
                + L2hDbUtils.getL2hQuestionWordTable(tablePrefix, tableSuffix)
                + " ORDER BY postid";
        LOGGER.info("SqlQW: " + sqlQw);
        // sqlQt and sqlQw return the same set of results, ideally. after filtering,
        // it may not
        String sqlQtMinusQw = "(" + sqlQt + ") EXCEPT (" + sqlQw + ")";
        String sqlQwMinusQt = "(" + sqlQw + ") EXCEPT (" + sqlQt + ")";
        String sqlCompare = "(" + sqlQtMinusQw + ") UNION ALL(" + sqlQwMinusQt + ")";
        LOGGER.info("SqlCompare: " + sqlCompare);
        LOGGER.info("To execute sql: " + sqlCompare);
        try (PreparedStatement pstmt = conn.prepareStatement(sqlCompare);
                ResultSet rs = pstmt.executeQuery()) {
                return !rs.next();
        } catch (SQLException e) {
            LOGGER.error("Cannot check not question ids.", e);
            return false;
        }
    }

    public static boolean makeL2HDataset(String datasetName, String datasetFolder, String tablePrefix,
            String dbPropertiesFilename) {
        if (!prepareL2hDataset(tablePrefix, null, dbPropertiesFilename)) {
            LOGGER.error("Failed to prepare L2H dataset tables.");
            return false;
        }
        LOGGER.info("Prepared L2h dataset tables.");        
        return makeL2HDatasetFiles(datasetName, datasetFolder, tablePrefix, null, dbPropertiesFilename);
    }    
}
