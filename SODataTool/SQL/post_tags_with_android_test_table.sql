--
-- Example SQL Statements
--
SELECT DISTINCT t2.tagname
INTO wk17m1_android_Tmp_Tagname
    FROM tags AS t2, posttags AS pt2, posttags AS pt, tags AS t 
    WHERE t.tagname='android' 
    	AND t.id=pt.tagid 
    	AND pt.postid=pt2.postid 
    	AND pt2.tagid=t2.id 
    ORDER BY t2.tagname;
    
    
CREATE TABLE wk17m1_android_tmp_tags (tagid int, tagname text);
\copy wk17m1_android_tmp_tags from ../SOAndroid/post_tags_withandroid.csv with CSV delimiter ',';


SELECT t.tagname,COUNT(t.tagname) AS c 
FROM wk17m1_android_Tmp_Tagname AS tt, wk17m1_Tags AS t, wk17m1_QuestionTag AS qt  
WHERE  tt.tagname=t.tagname AND t.tagid=qt.tagid  GROUP BY t.tagname ORDER BY c 
drop table wk17m1_android_tmp_tagname;

SELECT pt1.postid,pt1.tagid,pt2.tagid,t1.tagname,t2.tagname
FROM posttags AS pt1, posttags as pt2, tags AS t1, wk17m1_android_tmp_tags AS t2
WHERE pt1.postid=pt2.postid AND pt1.tagid=t1.id AND t1.tagname='android' AND pt2.tagid=t2.tagid AND t2.tagname!='android'
ORDER BY pt2.tagid;

SELECT pt1.postid,pt2.tagid
FROM posttags AS pt1, posttags as pt2, tags AS t1, wk17m1_android_tmp_tags AS t2
WHERE pt1.postid=pt2.postid AND pt1.tagid=t1.id AND t1.tagname='android' AND pt2.tagid=t2.tagid AND t2.tagname!='android'
GROUP BY pt2.tagid
ORDER BY pt2.tagid;

SELECT count(pt1.postid),pt2.tagid
FROM posttags AS pt1, posttags as pt2, tags AS t1, wk17m1_android_tmp_tags AS t2
WHERE pt1.postid=pt2.postid AND pt1.tagid=t1.id AND t1.tagname='android' AND pt2.tagid=t2.tagid AND t2.tagname!='android'
GROUP BY pt2.tagid
HAVING count(pt1.postid) >= 500;

SELECT count(pt1.postid) as c,pt2.tagid as tagid
FROM posttags AS pt1, posttags as pt2, tags AS t1, wk17m1_android_tmp_tags AS t2
WHERE pt1.postid=pt2.postid AND pt1.tagid=t1.id AND t1.tagname='android' AND pt2.tagid=t2.tagid AND t2.tagname!='android'
GROUP BY pt2.tagid
ORDER BY c;

SELECT tt.id,tt.tagname,tc.c
FROM tags AS tt, 
     (SELECT count(pt1.postid) as c,pt2.tagid as tagid
FROM posttags AS pt1, posttags as pt2, tags AS t1, wk17m1_android_tmp_tags AS t2
WHERE pt1.postid=pt2.postid AND pt1.tagid=t1.id AND t1.tagname='android' AND pt2.tagid=t2.tagid AND t2.tagname!='android'
GROUP BY pt2.tagid
ORDER BY c) AS tc
WHERE tt.id=tc.tagid;

SELECT tt.id,tt.tagname,tc.c
FROM tags AS tt, 
    (
        SELECT count(pt1.postid) AS c,pt2.tagid AS tagid
        FROM posttags AS pt1, posttags AS pt2, tags AS t1, wk17m1_android_tmp_tags AS t2
        WHERE pt1.postid=pt2.postid AND pt1.tagid=t1.id AND t1.tagname='android' AND pt2.tagid=t2.tagid AND t2.tagname!='android'
        GROUP BY pt2.tagid
        ORDER BY c
    ) AS tc
WHERE tt.id=tc.tagid;

SELECT t9.id,t9.tagname
FROM tags as t9, (SELECT count(pt1.postid),pt2.tagid as tagid
FROM posttags AS pt1, posttags as pt2, tags AS t1, wk17m1_android_tmp_tags AS t2
WHERE pt1.postid=pt2.postid AND pt1.tagid=t1.id AND t1.tagname='android' AND pt2.tagid=t2.tagid AND t2.tagname!='android'
GROUP BY pt2.tagid
HAVING count(pt1.postid) >= 500) as t7
WHERE t7.tagid=t9.id;
