#!/bin/bash

sqlite_db="addresses.sqlite"
schema_file="addresses_schema.sql"
table="addresses"
mysql_db="addresses"
mysql_user="root"
mysql_pass="student"
chunk_size=50

# get mysql schema
get_schema() {
    sqlite3 "$SQLITE_DB" ".schema $TABLE_NAME" > "$SCHEMA_FILE"
}

# create mysql db
# BEFORE running: manually change "US" to 'US' on line 9 of schema (done unless you rerun get_schema)
create_mysqldb(){
    mysql -u "$mysql_user" -p"$mysql_pass" <<EOF
    CREATE DATABASE IF NOT EXISTS $mysql_db;
    USE $mysql_db;
    SOURCE $schema_file;
    ALTER TABLE $table DISABLE KEYS;
EOF
}

# convers sqlite to csv to mysql in sections
convert(){
    # rows=$(sqlite3 "$sqlite_db" "SELECT COUNT(*) FROM $table;")
    rows=50
    offset=0

    while [ $offset -lt $rows ]; do
        csv_file="chunk.csv"

        # export chunk to csv
        sqlite3 -header -csv "$sqlite_db" "SELECT * FROM $table LIMIT $chunk_size OFFSET $offset;" > "$csv_file"

        # import csv to mysql
        mysql --local-infile=1 -u "$mysql_user" -p"$mysql_pass" "$mysql_db" -e "
        LOAD DATA LOCAL INFILE '$csv_file'
        INTO TABLE $table
        FIELDS TERMINATED BY ','
        ENCLOSED BY '\"'
        IGNORE 1 ROWS;"

        # remove csv file
        # rm "$csv_file"

        offset=$((offset + chunk_size))
    done

    # reenable indexes
    mysql -u "$mysql_user" -p"$mysql_pass" "$mysql_db" -e "ALTER TABLE $table ENABLE KEYS;"
}

convert

