#!/bin/bash

sqlite_db="addresses.sqlite"
schema_file="addresses_schema.sql"
table="addresses"
mysql_db="addresses"
mysql_user="root"
mysql_pass="student"
chunk_size=50

get_schema() {
    sqlite3 "$SQLITE_DB" ".schema $TABLE_NAME" > "$SCHEMA_FILE"
}

create_mysqldb(){
    # create db
    # mysql -u "$mysql_user" -p"$mysql_pass" -e "CREATE DATABASE IF NOT EXISTS $mysql_db;"
    # load schema
    # mysql -u "$mysql_user" -p"$mysql_pass" "$mysql_db" < "$schema_file"
    # disable indexes so imports are faster
    # mysql -u "$mysql_user" -p"$mysql_pass" "$mysql_db" -e "ALTER TABLE $table DISABLE KEYS;"

    mysql -u "$mysql_user" -p"$mysql_pass" <<EOF
    CREATE DATABASE IF NOT EXISTS $mysql_db;
    USE $mysql_db;
    SOURCE $schema_file;
    ALTER TABLE $table DISABLE KEYS;
EOF
}

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

