package sodata.database;

public class L2hDbUtils {
    private static String l2hTableSuffix = "_l2h";
    
    public static String getL2hQuestionTagTable(String tablePrefix, String tableSuffix) {
        String tableName = getWorkingQuestionTagTable(tablePrefix, tableSuffix) + l2hTableSuffix;
        return tableName;
    }
    
    public static String getL2hQuestionWordTable(String tablePrefix, String tableSuffix) {
        String tableName = getWorkingQuestionWordTable(tablePrefix, tableSuffix) + l2hTableSuffix;
        return tableName;
    }
    
    
    public static String getL2hTagTable(String tablePrefix, String tableSuffix) {
        String tableName = getWorkingTagTable(tablePrefix, tableSuffix) + l2hTableSuffix;
        return tableName;
    }
    
    public static String getL2hVocabularyTable(String tablePrefix, String tableSuffix) {
        String tableName = getWorkingVocabularyTable(tablePrefix, tableSuffix) + l2hTableSuffix;
        return tableName;
    }
    
    public static String getSqlMkL2hQuestionTagTableFromSelect(String tablePrefix, String tableSuffix) {
        String sql = 
                " SELECT qt.postid, qt.tagid AS origtagid, t.newtagid AS newtagid "
              + " INTO " + getL2hQuestionTagTable(tablePrefix, tableSuffix) 
              + " FROM " + getWorkingQuestionTagTable(tablePrefix, tableSuffix) + " AS qt,"
                         + getL2hTagTable(tablePrefix, tableSuffix) + " AS t"
              + " WHERE qt.tagid=t.origtagid ";      
        return sql;
    }
    
    public static String[] getSqlMkL2hQuestionTagTblIndices(String tablePrefix, String tableSuffix) {
        String[] sql = 
            {
                 " CREATE INDEX " 
               +     getL2hQuestionTagTable(tablePrefix, tableSuffix) + "_postid_idx"
               + " ON " 
               +     getL2hQuestionTagTable(tablePrefix, tableSuffix) 
               + " USING btree (postid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
                 " CREATE INDEX " 
               +     getL2hQuestionTagTable(tablePrefix, tableSuffix) + "_origtagid_idx"
               + " ON " 
               +     getL2hQuestionTagTable(tablePrefix, tableSuffix) 
               + " USING btree (origtagid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
               " CREATE INDEX " 
               +     getL2hQuestionTagTable(tablePrefix, tableSuffix) + "_newtagid_idx"
               + " ON " 
               +     getL2hQuestionTagTable(tablePrefix, tableSuffix) 
               + " USING btree (newtagid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
                 " CREATE INDEX " 
               +     getL2hQuestionTagTable(tablePrefix, tableSuffix) + "_postidnewtagid_idx"
               + " ON " 
               +     getL2hQuestionTagTable(tablePrefix, tableSuffix) 
               + " USING btree (postid,newtagid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
               " CREATE INDEX " 
             +     getL2hQuestionTagTable(tablePrefix, tableSuffix) + "_postidorigtagid_idx"
             + " ON " 
             +     getL2hQuestionTagTable(tablePrefix, tableSuffix) 
             + " USING btree (postid,origtagid) WITH (FILLFACTOR = 100)" // the table is seldom change, use fillfactor 100
            };
        return sql;
    }
    
    public static String getSqlMkL2hQuestionWordTable(String tablePrefix, String tableSuffix) {
        String sql = 
                " CREATE TABLE " + getL2hQuestionWordTable(tablePrefix, tableSuffix) 
              + " ( "
              +       " postid BIGINT, "
              +       " origwordid BIGINT, "
              +       " wordcount BIGINT, "
              +       " newwordid BIGINT "
              + " ) "
              ;
        return sql;
    }
    
    public static String getSqlMkL2hQuestionWordTableFromSelect(String tablePrefix, String tableSuffix) {
        String sql = 
                " SELECT qw.postid, qw.wordid AS origwordid, qw.wordcount, v.newid AS newwordid "
              + " INTO " + getL2hQuestionWordTable(tablePrefix, tableSuffix) 
              + " FROM " + getWorkingQuestionWordTable(tablePrefix, tableSuffix) + " AS qw,"
                         + getL2hVocabularyTable(tablePrefix, tableSuffix) + " AS v"
              + " WHERE qw.wordid=v.origid ";      
        return sql;
    }  
    
    public static String[] getSqlMkL2hQuestionWordTblIndices(String tablePrefix, String tableSuffix) {
        String[] sql = 
            {
                 " CREATE INDEX " 
               +     getL2hQuestionWordTable(tablePrefix, tableSuffix) + "_postid_idx"
               + " ON " 
               +     getL2hQuestionWordTable(tablePrefix, tableSuffix) 
               + " USING btree (postid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
                 " CREATE INDEX " 
               +     getL2hQuestionWordTable(tablePrefix, tableSuffix) + "_origwordid_idx"
               + " ON " 
               +     getL2hQuestionWordTable(tablePrefix, tableSuffix) 
               + " USING btree (origwordid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
               " CREATE INDEX " 
               +     getL2hQuestionWordTable(tablePrefix, tableSuffix) + "_newwordid_idx"
               + " ON " 
               +     getL2hQuestionWordTable(tablePrefix, tableSuffix) 
               + " USING btree (newwordid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
                 " CREATE INDEX " 
               +     getL2hQuestionWordTable(tablePrefix, tableSuffix) + "_postidnewwordid_idx"
               + " ON " 
               +     getL2hQuestionWordTable(tablePrefix, tableSuffix) 
               + " USING btree (postid,newwordid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
               " CREATE INDEX " 
             +     getL2hQuestionWordTable(tablePrefix, tableSuffix) + "_postidorigwordid_idx"
             + " ON " 
             +     getL2hQuestionWordTable(tablePrefix, tableSuffix) 
             + " USING btree (postid,origwordid) WITH (FILLFACTOR = 100)" // the table is seldom change, use fillfactor 100
            };
        return sql;
    }  
    
    public static String getSqlMkL2hTagTable(String tablePrefix, String tableSuffix) {
        String sql =
                " CREATE TABLE " + getL2hTagTable(tablePrefix, tableSuffix)
                + " ( "
                +    " origtagid bigint,"
                +    " newtagid bigint,"
                +    " tagname text"
                + " ) ";
        return sql;
    }
    
    public static String getSqlMkL2hTagTableFromSelect(String tablePrefix, String tableSuffix) {
        String sql = 
                " SELECT tagid AS origtagid, -1+ROW_NUMBER() OVER() AS newtagid, tagname "
              + " INTO " + getL2hTagTable(tablePrefix, tableSuffix) 
              + " FROM " + getWorkingTagTable(tablePrefix, tableSuffix)
              + " ORDER BY origtagid";      
        return sql;
    }

    public static String[] getSqlMkL2hTagTblIndices(String tablePrefix, String tableSuffix) {
        String[] sql = 
            {
                " CREATE INDEX " 
               +     getL2hTagTable(tablePrefix, tableSuffix) + "_origtagid_idx"
               + " ON " 
               +     getL2hTagTable(tablePrefix, tableSuffix) 
               + " USING btree (origtagid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
               " CREATE INDEX " 
              +     getL2hTagTable(tablePrefix, tableSuffix) + "_newtagid_idx"
              + " ON " 
              +     getL2hTagTable(tablePrefix, tableSuffix) 
              + " USING btree (newtagid) WITH (FILLFACTOR = 100)" // the table is seldom change, use fillfactor 100
            };
        return sql;
    }    
    
    public static String getSqlMkL2hVocabularyTable(String tablePrefix, String tableSuffix) {
        String sql = 
                " CREATE TABLE " + getL2hVocabularyTable(tablePrefix, tableSuffix)
              + " ( "
              +       " word TEXT, "
              +       " origid BIGINT, "
              +       " newid BIGINT"
              + " ) ";
        return sql;
    }
    
    public static String getSqlMkL2hVocabularyTableFromSelect(String tablePrefix, String tableSuffix) {
        String sql = 
                " SELECT word, id AS origid, -1+ROW_NUMBER() OVER() AS newid "
              + " INTO " + getL2hVocabularyTable(tablePrefix, tableSuffix) 
              + " FROM " + getWorkingVocabularyTable(tablePrefix, tableSuffix)
              + " ORDER BY origid";      
        return sql;
    }
    
    public static String[] getSqlMkL2hVocabularyTblIndices(String tablePrefix, String tableSuffix) {
        String[] sql = 
            {
                " CREATE INDEX " 
               +     getL2hVocabularyTable(tablePrefix, tableSuffix) + "_origid_idx"
               + " ON " 
               +     getL2hVocabularyTable(tablePrefix, tableSuffix) 
               + " USING btree (origid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
               " CREATE INDEX " 
              +     getL2hVocabularyTable(tablePrefix, tableSuffix) + "_newid_idx"
              + " ON " 
              +     getL2hVocabularyTable(tablePrefix, tableSuffix) 
              + " USING btree (newid) WITH (FILLFACTOR = 100)" // the table is seldom change, use fillfactor 100
            };
        return sql;
    }  
        
    /*
     * 
        String sql;
        if (tableSuffix == null) {
            DbUtils.setWorkingTablePrefix(tablePrefix);
            sql = "SELECT postid,tagid FROM " + DbUtils.getQuestionTagTable() + " ORDER BY postid,tagid";
        } else {
            FilterDbUtils.setFilteredWorkingTablePrefix(tableSuffix);            
            sql = "SELECT postid,tagid FROM " + FilterDbUtils.getFilteredQuestionTagTable() + " ORDER BY postid,tagid";            
        }
        LOGGER.info("To execute query: " + sql + ".");
     */    
    public static String getWorkingQuestionTagTable(String tablePrefix, String tableSuffix) {
        String tableName;
        if (tableSuffix == null) {
            DbUtils.setWorkingTablePrefix(tablePrefix);
            tableName = DbUtils.getQuestionTagTable();
        } else {
            FilterDbUtils.setFilteredWorkingTablePrefix(tableSuffix);
            tableName = FilterDbUtils.getFilteredQuestionTagTable();
        }     
        return tableName;
    }
    
    public static String getWorkingQuestionWordTable(String tablePrefix, String tableSuffix) {
        String tableName;
        if (tableSuffix == null) {
            DbUtils.setWorkingTablePrefix(tablePrefix);
            tableName = DbUtils.getQuestionWordTable();
        } else {
            FilterDbUtils.setFilteredWorkingTablePrefix(tableSuffix);
            tableName = FilterDbUtils.getFilteredQuestionWordTable();
        }     
        return tableName;        
    }  
    
    
    public static String getWorkingTagTable(String tablePrefix, String tableSuffix) {
        String tableName;
        if (tableSuffix == null) {
            DbUtils.setWorkingTablePrefix(tablePrefix);
            tableName = DbUtils.getTagTable();
        } else {
            FilterDbUtils.setFilteredWorkingTablePrefix(tableSuffix);
            tableName = FilterDbUtils.getFilteredTagTable();
        }     
        return tableName;
    }  
    
    public static String getWorkingVocabularyTable(String tablePrefix, String tableSuffix) {
        String tableName;
        if (tableSuffix == null) {
            DbUtils.setWorkingTablePrefix(tablePrefix);
            tableName = DbUtils.getVocabularyTable();
        } else {
            FilterDbUtils.setFilteredWorkingTablePrefix(tableSuffix);
            tableName = FilterDbUtils.getFilteredVocabularyTable();
        }     
        return tableName;
    }

    public static String getSqlMkL2hTagTableFromTmpTagTable(String tablePrefix, String tableSuffix) {
        String sql = 
                " SELECT ot.id AS origtagid, tt.rownumber AS newtagid, ot.tagname AS tagname INTO " 
              + L2hDbUtils.getL2hTagTable(tablePrefix, tableSuffix)
              + " FROM Tags AS ot, " + DbUtils.getTmpTagTable() + " AS tt "
              + " WHERE ot.tagname=tt.tagname";
        return sql;
    }
    
    public static String[] getSqlMkL2hTagTableIndices(String tablePrefix, String tableSuffix) {
        String[] sql
            = {
              " CREATE INDEX " 
            +     L2hDbUtils.getL2hTagTable(tablePrefix, tableSuffix) + "_origtagid_idx"
            + " ON " 
            +     L2hDbUtils.getL2hTagTable(tablePrefix, tableSuffix) 
            + " USING btree (origtagid) WITH (FILLFACTOR = 100)" // the table is seldom change, use fillfactor 100
        };
        return sql;
    }

    public static String getSqlMkL2hTmpTagTable() {
        String sql = 
                " CREATE TEMPORARY TABLE " + DbUtils.getTmpTagTable()
                +   " ( "
                +       " tagname TEXT, " 
                +       " rownumber bigint "
                +   " ) "
                + " ON COMMIT DROP";
        return sql;
    }

    public static String getSqlMkQuestionTagFromSelect(String tablePrefix, String tableSuffix, String startTimestamp, String endTimestamp) {
        String sql = 
                " SELECT p.id AS postid, pt.tagid AS origtagid, t.newtagid AS newtagid " 
              +     " INTO " + L2hDbUtils.getL2hQuestionTagTable(tablePrefix, tableSuffix)
              + " FROM "
              +     " posts AS p, posttags AS pt, " + L2hDbUtils.getL2hTagTable(tablePrefix, tableSuffix) + " AS t " 
              + " WHERE "
              +     " p.id=pt.postid AND pt.tagid=t.origtagid "
              +     " AND p.creationdate >= to_timestamp('" + startTimestamp + "', 'yyyy-mm-dd hh24:mi:ss')"
              +     " AND p.creationdate < to_timestamp('" + endTimestamp + "', 'yyyy-mm-dd hh24:mi:ss')";  
        return sql;
    }

    public static String getSqlSelectQuestionOnQuestionTagTable(String tablePrefix, String tableSuffix) {
        String sql = 
                " SELECT DISTINCT p.id AS id, p.title AS title, p.body AS body "
              + " FROM posts AS p, " + L2hDbUtils.getL2hQuestionTagTable(tablePrefix, tableSuffix) + " AS qt "
              + " WHERE p.id=qt.postid";
        return sql;
    }

//    private static String getL2hQuestionIdTable(String tablePrefix, String tableSuffix) {
//        String tableName = getWorkingQuestionIdTable(tablePrefix, tableSuffix) + l2hTableSuffix;
//        return tableName;
//    }
//
//    private static String getWorkingQuestionIdTable(String tablePrefix, String tableSuffix) {
//        String tableName;
//        if (tableSuffix == null) {
//            DbUtils.setWorkingTablePrefix(tablePrefix);
//            tableName = DbUtils.getQuestionIdTable();
//        } else {
//            FilterDbUtils.setFilteredWorkingTablePrefix(tableSuffix);
//            tableName = FilterDbUtils.getFilteredQuestionIdTable();
//        }     
//        return tableName;
//    }  
}
