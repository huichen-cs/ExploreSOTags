SELECT COUNT(*) FROM posts AS p WHERE p.posttypeid=1 AND p.body LIKE '%<code>%';
SELECT COUNT(*) FROM posts AS p WHERE p.posttypeid=2 AND p.body LIKE '%<code>%';
SELECT COUNT(*) FROM comments AS c WHERE c.text LIKE '%<code>%';
SELECT COUNT(*) FROM tags;
SELECT COUNT(*) FROM tags WHERE count <= 10;
SELECT COUNT(*) FROM tags WHERE count > 1000;
SELECT tagname,count,count/13472796.*100. AS percent FROM tags ORDER BY count DESC;