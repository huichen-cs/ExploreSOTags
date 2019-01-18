--
-- Example SQL Statements
--
 \copy (
    SELECT DISTINCT t2.id, t2.tagname 
    FROM tags AS t2, posttags AS pt2, posttags AS pt, tags AS t 
    WHERE t.tagname='android' 
    	AND t.id=pt.tagid 
    	AND pt.postid=pt2.postid 
    	AND pt2.tagid=t2.id 
    ORDER BY t2.tagname) 
 to './post_tags_withandroid.csv' with CSV DELIMITER ',';

