#!/bin/bash
#
# Download and set up PostgreSQL
# clone repo
#   https://github.com/Networks-Learning/stackexchange-dump-to-postgres.git
#

# assume YourDbPassword is the password, database user is sodumpuser, 
# and database is sodumpdb
createuser -U postgres -W -D -P sodumpuser
createdb -U postgres -W -O sodumpuser sodumpdb
python load_into_pg.py -d sodump db -f ../Posts.xml -u sodumpuser -p YourDbPassword --with-post-body Posts
python load_into_pg.py -d sodumpdb -f ../Tags.xml -u sodumpuser -p YourDbPassword --with-post-body Tags
python load_into_pg.py -d sodumpdb -f ../Badges.xml -u sodumpuser -p YourDbPassword --with-post-body Badges
python load_into_pg.py -d sodumpdb -f ../Comments.xml -u sodumpuser -p YourDbPassword --with-post-body Comments
python load_into_pg.py -d sodumpdb -f ../PostHistory.xml -u sodumpuser -p YourDbPassword --with-post-body PostHistory
python load_into_pg.py -d sodumpdb -f ../PostLinks.xml -u sodumpuser -p YourDbPassword --with-post-body PostLinks
python load_into_pg.py -d sodumpdb -f ../Users.xml -u sodumpuser -p YourDbPassword --with-post-body Users
python load_into_pg.py -d sodumpdb -f ../Votes.xml -u sodumpuser -p YourDbPassword --with-post-body Votes

# the password needs to be supplied from the console
psql -U sodumpuser sodumpdb < ./sql/final_post.sql
psql -U sodumpuser sodumpdb < ./sql/optional_post.sql
