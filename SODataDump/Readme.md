
# Stack Overflow Data Dump Data Preparation

1. Download the Stack Overflow data from the Stack Exchange data dump. The
data dump can be found at https://archive.org/details/stackexchange. To
download the Stack Overflow data, run `GetSODataDump.sh` or `GetSODataDump.cmd`.
2. Import the Stack Overflow data to a PostgreSQL database. This allows us
to process the data without worrying about available main memory. The database
schema can be found at https://data.stackexchange.com/stackoverflow/query/new.
To import the data to the PostgreSQL database, we use the Python scripts
available at https://github.com/Networks-Learning/stackexchange-dump-to-postgres.git.
3. Download Stack Overflow tag synonyms from
    https://stackoverflow.com/tags/synonyms?tab=synonym&filter=all

