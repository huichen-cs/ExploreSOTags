package sodata.database;

public class DbTagSelectorUtils  extends DbUtilsBase {
    private static String workingTablePrefix = "Wk_BR05_D4";
    private final static String DEFAULT_TMP_TAGNAME_TABLE = "Tmp_Tagname";
    private final static String DEFAULT_TMP_WORD_ID_TABLE = "Tmp_WordId";
    private final static String DEFAULT_TMP_TAGS_TABLE = "Tmp_Tags";
    private final static String DEFAULT_TAG_TABLE = "Tags";
    private final static String DEFAULT_QUESTION_TAG_TABLE = "QuestionTag";    
    private final static String DEFAULT_QUESTION_ID_TABLE = "QuestionId";
    private final static String DEFAULT_VOCABULARY_TABLE = "QVocab";
    private final static String DEFAULT_QUESTION_WORD_TABLE = "QuestionWord";
    private final static String DEFAULT_TAG_COOCCUR_POST_COUNT_TABLE = "TagCoOccurPostCount";
    private final static String DEFAULT_TAG_QUESTION_COUNTS_TABLE = "TagQuestionCount";
    


    private static String getQuestionIdTable() {
        return workingTablePrefix + DEFAULT_QUESTION_ID_TABLE;
    }

    
    public static String getQuestionTagTable() {
        return workingTablePrefix + DEFAULT_QUESTION_TAG_TABLE;
    }
    

    
    
    public static String getQuestionWordTable() {
        return workingTablePrefix + DEFAULT_QUESTION_WORD_TABLE;
    }

    

    public static String getSqlMkQuestionIdTableFromSelect() {
        return
              " SELECT DISTINCT postid INTO " + DbTagSelectorUtils.getQuestionIdTable()
            + " FROM " + DbTagSelectorUtils.getQuestionTagTable();
    }
    
    public static String getSqlMkQuestionIdTableFromSelect(int maxPostsToExtractForTags) {
        String sql = 
                " SELECT DISTINCT st.postid as postid INTO " + DbTagSelectorUtils.getQuestionIdTable()
                + " FROM " 
                + "    ( "
                + "        SELECT qt.postid, qt.tagid, tqc.c, random() AS r " 
                + "        FROM " + DbTagSelectorUtils.getQuestionTagTable() + " AS qt, "  
                +                   DbTagSelectorUtils.getTagQuestionCountsTable() + " AS tqc " 
                + "        WHERE qt.tagid=tqc.tagid "
                + "    ) AS st " 
                + " WHERE st.r<=" + Integer.toString(maxPostsToExtractForTags) + "/st.c";
        return sql;
    }
    

    public static String getSqlMkQuestionTagTableFromSelect() {
        return
              " SELECT qt.postid, qt.tagid INTO " + DbTagSelectorUtils.getQuestionTagTable() 
            + " FROM " + DbUtils.getQuestionTagTable() + " AS qt, " + DbTagSelectorUtils.getTagTable() + " AS t " 
            + " WHERE qt.tagid=t.tagid ";
    }
    


    public static String getSqlMkQuestionWordTable() {
        return 
            " CREATE TABLE " + DbTagSelectorUtils.getQuestionWordTable()
                +   " ( "
                +       " postid BIGINT,"
                +       " wordid BIGINT,"
                +       " wordcount BIGINT"
                +   " ) ";
    }
    
    public static String getSqlMkQuestionWordTableFromSelect() {
        return
              " SELECT qw.postid,qw.wordid,qw.wordcount INTO " + DbTagSelectorUtils.getQuestionWordTable()
            + " FROM " + DbUtils.getQuestionWordTable() + " AS qw, " + DbTagSelectorUtils.getQuestionIdTable() + " AS q " 
            + " WHERE qw.postid=q.postid ";
    }
    
    public static String[] getSqlMkQwTblIndices() {
        String[] sqlMkQwTblIndices
        = {
          "ALTER TABLE "
        +      DbTagSelectorUtils.getQuestionWordTable() 
        + " ADD "
        + "    PRIMARY KEY (postid, wordid)",
          " CREATE INDEX " 
        +     DbTagSelectorUtils.getQuestionWordTable() + "_posdid_idx"
        + " ON " 
        +     DbTagSelectorUtils.getQuestionWordTable() 
        + " USING btree (postid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
          " CREATE INDEX " 
        +     DbTagSelectorUtils.getQuestionWordTable() + "_wordid_idx"
        + " ON " 
        +     DbTagSelectorUtils.getQuestionWordTable() 
        + " USING btree (wordid) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
          " CREATE INDEX "
        +     DbTagSelectorUtils.getQuestionWordTable() + "_wordcount_idx"
        + " ON " 
        +     DbTagSelectorUtils.getQuestionWordTable() 
        + " USING btree (wordcount) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
          };
        return sqlMkQwTblIndices;
    }
    
    public static String getSqlMkSimpleVocabularyTableFromSelect() {
        return
              " SELECT v.id,v.word,v.word_index,v.ndocuments,v.noccurrences INTO " + DbTagSelectorUtils.getVocabularyTable()
            + " FROM " + DbUtils.getVocabularyTable() + " AS v, " + DbTagSelectorUtils.getTmpWordIdTable() + " AS w " 
            + " WHERE v.id=w.wordid ";
    }    
    

    public static String getSqlMkTagCoOccurPostCountTableFromSelect(String anchorTagName) {
        return
                " SELECT tt.id as tagid,tt.tagname as tagname,tc.c as postcount INTO " + DbTagSelectorUtils.getTagCoOccurPostCountTable()
              + " FROM tags AS tt, " 
              + " ( "
              + "    SELECT count(pt1.postid) AS c,pt2.tagid AS tagid "
              + "    FROM posttags AS pt1, posttags AS pt2, tags AS t1, " + DbTagSelectorUtils.getTmpTagsTable() + " AS t2 "
              + "    WHERE pt1.postid=pt2.postid AND pt1.tagid=t1.id "
              + "                                AND t1.tagname='" + anchorTagName + "' "
              + "                                AND pt2.tagid=t2.tagid AND t2.tagname!='" + anchorTagName + "'"
              + "    GROUP BY pt2.tagid "
              + "    ORDER BY c "
              + " ) AS tc "
              + " WHERE tt.id=tc.tagid";
    }
    
    
    public static String getSqlMkTagQuestionCountsTable() {
        String sql = 
                " SELECT tagid, count(tagid) as c INTO " + DbTagSelectorUtils.getTagQuestionCountsTable() 
                + " FROM " + DbTagSelectorUtils.getQuestionTagTable()
                + " GROUP BY tagid";
        return sql;
    }
    


    public static String getSqlMkTagTableFromSelect() {
        return
              " SELECT t.tagid, t.tagname INTO " + DbTagSelectorUtils.getTagTable() 
            + " FROM " + DbUtils.getTagTable() + " AS t, " + DbTagSelectorUtils.getTmpTagnameTable() + " AS tt " 
            + " WHERE trim(both ' ' from t.tagname)=trim(both ' ' from tt.tagname) ";                
    }

    public static String getSqlMkTagTableFromSelectOnPostTable() {
        String sql = " SELECT DISTINCT t.tagid,t.tagname INTO " + DbTagSelectorUtils.getTagTable()
            + " FROM "
            +   DbTagSelectorUtils.getQuestionIdTable() + " AS qid, "
            +     DbUtils.getQuestionTagTable() + " AS qt, "
            +    DbUtils.getTagTable() + " AS t "
            + "    WHERE qid.postid=qt.postid AND qt.tagid=t.tagid ";
        return sql;
    }    

    public static String getSqlMkTmpTagnameTable() {
        return 
            " CREATE TEMPORARY TABLE " + DbTagSelectorUtils.getTmpTagnameTable()
                +   " ( "
                +       " tagname text " 
                +   " ) "
                + " ON COMMIT DROP";
    }
    
    
    
    public static String getSqlMkTmpTagsTable() {
        return 
                " CREATE TEMPORARY TABLE " + DbTagSelectorUtils.getTmpTagsTable()
                    +   " ( "
                    +       " tagid int,"
                    +       " tagname text " 
                    +   " ) "
                    + " ON COMMIT DROP";
    }
    
    
    
    public static String getSqlMkTmpWordIdTable() {
        return 
            " CREATE TEMPORARY TABLE " + DbTagSelectorUtils.getTmpWordIdTable()
                +   " ( "
                +       " wordid bigint " 
                 +   " ) "
                + " ON COMMIT DROP";
    }
    


    public static String[] getSqlMkVocabTblIndices() {
        String[] sqlMkVocabTblIndices
        = {
          " ALTER TABLE "
        +     DbTagSelectorUtils.getVocabularyTable()
        + " ADD "
        + "   PRIMARY KEY (id)",
          " CREATE INDEX " 
        +     DbTagSelectorUtils.getVocabularyTable() + "_noccurrences_idx"
        + " ON " 
        +     DbTagSelectorUtils.getVocabularyTable() 
        + " USING btree (noccurrences) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
          " CREATE INDEX " 
        +     DbTagSelectorUtils.getVocabularyTable() + "_ndocuments_idx"
        + " ON " 
        +     DbTagSelectorUtils.getVocabularyTable() 
        + " USING btree (ndocuments) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
          " CREATE INDEX "
        +     DbTagSelectorUtils.getVocabularyTable() + "_word_index_idx"
        + " ON " 
        +     DbTagSelectorUtils.getVocabularyTable() 
        + " USING btree (word_index) WITH (FILLFACTOR = 100)", // the table is seldom change, use fillfactor 100
          };    
     return sqlMkVocabTblIndices;
    }    
    
    
    public static String[] getSqlMkVocabTblTrigger() {
        String[] sqlMkVocabTblTrigger = {
                 " CREATE OR REPLACE FUNCTION compute_word_index_from_word_text_tagselector() "
               + " RETURNS trigger "
               + " LANGUAGE plpgsql "
               + " SECURITY DEFINER "
               + " AS $BODY$ "
               + "    BEGIN "
               + "   NEW.word_index = NEW.word::VARCHAR(" + Long.toString(DbUtils.VOCAB_TBL_WORD_INDEX_LENGTH) + "); "
               + "   RETURN NEW; "
               + " END "
               + " $BODY$ "
               ,
                 " CREATE TRIGGER compute_word_index_from_word_text_trigger_tagselector "
               + " BEFORE INSERT OR UPDATE "
               + " ON " + DbTagSelectorUtils.getVocabularyTable()
               + " FOR EACH ROW " 
               + " EXECUTE PROCEDURE compute_word_index_from_word_text_tagselector() "           
             };
            return sqlMkVocabTblTrigger;
    }    


    public static String getSqlQueryDistinctWordIdFromQuestionWordTable() {
        return
              " SELECT DISTINCT wordid FROM " + DbTagSelectorUtils.getQuestionWordTable();
    }    
    


    public static String getSqlQueryQuestionWordTable() {
        return
              " SELECT qw.postid,qw.wordid,qw.wordcount "
            + " FROM " + DbUtils.getQuestionWordTable() + " AS qw, " + DbTagSelectorUtils.getQuestionIdTable() + " AS q " 
            + " WHERE qw.postid=q.postid ";
    }

    public static String getSqlQueryTagQuestionCounts() {
        return 
             " SELECT t.tagname,COUNT(t.tagname) AS c " 
            + " FROM " 
            +    DbTagSelectorUtils.getTmpTagnameTable() + " AS tt, "
            +    DbUtils.getTagTable() + " AS t, "
            +    DbUtils.getQuestionTagTable() + " AS qt "
            + " WHERE "
            +    " tt.tagname=t.tagname AND t.tagid=qt.tagid "
            + " GROUP BY t.tagname ORDER BY c ";
    }    
    


    public static String getSqlQueryWantedTagsNotInTagTable() {
        return
              " SELECT trim(both ' ' FROM tt.tagname) FROM " 
            + DbTagSelectorUtils.getTmpTagnameTable() + " AS tt " 
            + " EXCEPT "
            + " SELECT trim(both ' ' from t.tagname) FROM "
            + DbTagSelectorUtils.getTagTable() + " AS t";
//                  " SELECT t.tagname"
//                + " FROM " + DbTagSelectorUtils.getTagTable() + " AS t RIGHT JOIN " + DbTagSelectorUtils.getTmpTagnameTable() + " AS tt " 
//                + " ON trim(both ' ' from t.tagname)=trim(both ' ' from tt.tagname) "
//                + " WHERE tt.tagname=null";            
    }
    

    public static String getSqlSelectCoOccurPostCountTable(int minCoOccurPosts) {
        return
              " SELECT t.tagname, tc.postcount"
            + " FROM "
            +    DbUtils.getTagTable() + " AS t, "
            +    DbTagSelectorUtils.getTagCoOccurPostCountTable() + " AS tc "
            + " WHERE t.tagid=tc.tagid AND tc.postcount>=" + Integer.toString(minCoOccurPosts);
    }    
    
    public static String getTagCoOccurPostCountTable() {
        return workingTablePrefix + DEFAULT_TAG_COOCCUR_POST_COUNT_TABLE;
    }        
    

    public static String getTagQuestionCountsTable() {
        return workingTablePrefix + DEFAULT_TAG_QUESTION_COUNTS_TABLE;
    }


    public static String getTagTable() {
        return workingTablePrefix + DEFAULT_TAG_TABLE;
    }


    public static String getTmpTagnameTable() {
        return workingTablePrefix + DEFAULT_TMP_TAGNAME_TABLE;
    }


    public static String getTmpTagsTable() {
        return workingTablePrefix + DEFAULT_TMP_TAGS_TABLE;
    }


    public static String getTmpWordIdTable() {
        return workingTablePrefix + DEFAULT_TMP_WORD_ID_TABLE;
    }


    public static String getVocabularyTable() {
        return workingTablePrefix + DEFAULT_VOCABULARY_TABLE;
    }


    public static void setWorkingTablePrefix(String prefix) {
        if (prefix != null)
            workingTablePrefix = prefix;
    }


    public static String getSqlMkQuestionTagTableFromSelectQuestionId() {
        return
                " SELECT qt.postid, qt.tagid INTO " + DbTagSelectorUtils.getQuestionTagTable() 
              + " FROM " + DbUtils.getQuestionTagTable() + " AS qt, " 
                         + DbTagSelectorUtils.getTagTable() + " AS t, "
                         + DbTagSelectorUtils.getQuestionIdTable() + " AS qid"
              + " WHERE qt.tagid=t.tagid AND qt.postid=qid.postid";
    }
}
