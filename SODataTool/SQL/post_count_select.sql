--
-- Example SQL Statements
--
SELECT count(st.tagid) as c, st.tagid 
FROM 
    (
        SELECT qt.postid, qt.tagid, tqc.c, random() as r 
        FROM wk17m1_questiontag as qt, wk17m1_tagquestioncounts as tqc 
        WHERE qt.tagid=tqc.tagid
    ) as st 
WHERE st.r<=100/st.c group by st.tagid order by c desc;

SELECT st.postid, st.tagid 
FROM 
    (
        SELECT qt.postid, qt.tagid, tqc.c, random() as r 
        FROM wk17m1_questiontag as qt, wk17m1_tagquestioncounts as tqc 
        WHERE qt.tagid=tqc.tagid
    ) as st 
WHERE st.r<=100/st.c;

SELECT DISTINCT st.postid 
FROM 
    (
        SELECT qt.postid, qt.tagid, tqc.c, random() AS r 
        FROM wk17m1_questiontag AS qt, wk17m1_tagquestioncounts AS tqc 
        WHERE qt.tagid=tqc.tagid
    ) AS st 
WHERE st.r<=100/st.c;
