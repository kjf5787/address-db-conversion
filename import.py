import sqlite3
import mysql.connector
import csv
import os
import tempfile
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED

# CONFIG
sqlite_db = r"C:\iste 498\address-db-conversion\addresses.sqlite"
table = "addresses"
mysql_db = "addresses"
mysql_user = "root"
mysql_pass = "4KBFT2%t^tPl"
offset_log = "offset.log"
error_log = "errors.log"

# May need to change based on hardware & performance
batch_size = 15
max_threads = 4
max_retries = 5

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)\
    
# MYSQL INITIALIZATION
def init_mysql_db(drop_existing=False):
    conn = mysql.connector.connect(user=mysql_user, password=mysql_pass, host="localhost")
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {mysql_db}")
    cursor.execute(f"USE {mysql_db}")

    if drop_existing:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        logger.info(f"Dropped existing table {table}")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table}` (
        `zipcode` VARCHAR(6) NOT NULL,
        `number` VARCHAR(30) NOT NULL,
        `street` VARCHAR(200) NOT NULL,
        `street2` VARCHAR(20),
        `city` VARCHAR(50) NOT NULL,
        `state` CHAR(2) NOT NULL,
        `plus4` CHAR(4),
        `country` CHAR(2) NOT NULL DEFAULT 'US',
        `latitude` DECIMAL(8,6) NOT NULL,
        `longitude` DECIMAL(9,6) NOT NULL,
        `source` VARCHAR(40),
        UNIQUE KEY `unique_address` (zipcode, number, street, street2, country)
    );
    """
    cursor.execute(create_table_sql)
    cursor.execute(f"ALTER TABLE {table} DISABLE KEYS")
    conn.commit()
    conn.close()
    logger.info(f"MySQL database '{mysql_db}' and table '{table}' initialized.")

# CREATE INDEX IF NOT EXISTS
def create_index_if_not_exists(index_name, columns):
    conn = mysql.connector.connect(user=mysql_user, password=mysql_pass, host="localhost", database=mysql_db)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COUNT(*) 
        FROM information_schema.statistics 
        WHERE table_schema = '{mysql_db}' 
          AND table_name = '{table}' 
          AND index_name = '{index_name}';
    """)
    exists = cursor.fetchone()[0]
    if exists == 0:
        cursor.execute(f"CREATE INDEX {index_name} ON {table} ({columns});")
        conn.commit()
        logger.info(f"Created index {index_name}")
    conn.close()

# LOGGING
def get_completed_batches():
    if not os.path.exists(offset_log):
        return set()
    with open(offset_log, "r") as f:
        completed = set(line.strip() for line in f if line.strip())
    logger.info(f"Found {len(completed)} completed batches in log")
    return completed

def log_completed_batch(batch_id):
    with open(offset_log, "a") as f:
        f.write(batch_id + "\n")

def log_error(batch_id, error):
    with open(error_log, "a") as f:
        f.write(f"{batch_id}: {str(error)}\n")
        
# GET TABLE STATISTICS
def get_table_stats():
    # SQLite stats
    conn_sqlite = sqlite3.connect(sqlite_db)
    cursor_sqlite = conn_sqlite.cursor()
    cursor_sqlite.execute(f"SELECT COUNT(*) FROM {table}")
    sqlite_total = cursor_sqlite.fetchone()[0]
    cursor_sqlite.execute(f"SELECT COUNT(DISTINCT zipcode) FROM {table}")
    sqlite_zipcodes = cursor_sqlite.fetchone()[0]
    conn_sqlite.close()
    
    # MySQL stats
    try:
        conn_mysql = mysql.connector.connect(user=mysql_user, password=mysql_pass, host="localhost", database=mysql_db)
        cursor_mysql = conn_mysql.cursor()
        cursor_mysql.execute(f"SELECT COUNT(*) FROM {table}")
        mysql_total = cursor_mysql.fetchone()[0]
        cursor_mysql.execute(f"SELECT COUNT(DISTINCT zipcode) FROM {table}")
        mysql_zipcodes = cursor_mysql.fetchone()[0]
        conn_mysql.close()
    except:
        mysql_total = 0
        mysql_zipcodes = 0
    
    logger.info(f"SQLite: {sqlite_total:,} rows, {sqlite_zipcodes:,} unique zipcodes")
    logger.info(f"MySQL: {mysql_total:,} rows, {mysql_zipcodes:,} unique zipcodes")
    return sqlite_total, sqlite_zipcodes, mysql_total, mysql_zipcodes


# PROCESS SINGLE BATCH 
def process_batch(batch):
    batch_id = f"{min(batch)}-{max(batch)}"
    
    # Get data from SQLite
    conn_sqlite = sqlite3.connect(sqlite_db)
    cursor_sqlite = conn_sqlite.cursor()
    placeholders = ",".join(["?"] * len(batch))
    cursor_sqlite.execute(f"SELECT * FROM {table} WHERE zipcode IN ({placeholders})", batch)
    rows = cursor_sqlite.fetchall()
    conn_sqlite.close()

    if not rows:
        logger.info(f"Skipping batch {batch_id} (no rows found)")
        log_completed_batch(batch_id)
        return 0

    logger.info(f"Processing batch {batch_id}: {len(rows)} rows")

    # Create temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", delete=False, newline="", encoding="utf-8", suffix=".csv") as tmpfile:
        writer = csv.writer(tmpfile, quoting=csv.QUOTE_ALL, lineterminator='\n')
        writer.writerows(rows)
        csv_file_path = tmpfile.name

    csv_file_path_safe = csv_file_path.replace("\\", "/")

    try:
        # Load into MySQL
        conn_mysql = mysql.connector.connect(
            user=mysql_user,
            password=mysql_pass,
            host="localhost",
            database=mysql_db,
            allow_local_infile=True
        )
        cursor_mysql = conn_mysql.cursor()
        
        load_sql = f"""
        LOAD DATA LOCAL INFILE '{csv_file_path_safe}'
        INTO TABLE {table}
        FIELDS TERMINATED BY ',' ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        """
        cursor_mysql.execute(load_sql)
        affected_rows = cursor_mysql.rowcount
        conn_mysql.commit()
        conn_mysql.close()
        
        log_completed_batch(batch_id)
        return len(rows)
        
    finally:
        if os.path.exists(csv_file_path):
            os.remove(csv_file_path)
            
# PROCESS SINGLE BATCH WITH DEADLOCK RETRY
def process_batch_with_retry(batch):
    batch_id = f"{min(batch)}-{max(batch)}"
    for attempt in range(1, max_retries + 1):
        try:
            rows_processed = process_batch(batch)
            logger.info(f"Batch {batch_id}: {rows_processed} rows processed")
            return rows_processed
        except mysql.connector.errors.DatabaseError as e:
            if getattr(e, 'errno', None) == 1213:  # Deadlock
                wait_time = 2 ** attempt
                logger.warning(f"Deadlock on batch {batch_id}. Retrying in {wait_time}s (attempt {attempt}/{max_retries})")
                time.sleep(wait_time)
            else:
                logger.error(f"Database error on batch {batch_id}: {e}")
                log_error(batch_id, e)
                raise
        except Exception as e:
            logger.error(f"Unexpected error on batch {batch_id}: {e}")
            log_error(batch_id, e)
            raise
    
    logger.error(f"Failed batch {batch_id} after {max_retries} retries.")
    log_error(batch_id, f"Failed after {max_retries} retries")
    return 0

def worker_thread(zipcodes, current_batch_size, completed):
    """Process a slice of ZIP codes sequentially (one per thread)."""
    i = 0
    total_rows = 0
    max_batch = 50

    while i < len(zipcodes):
        batch = zipcodes[i:i+current_batch_size]
        batch_id = f"{min(batch)}-{max(batch)}"

        if batch_id in completed:
            logger.info(f"Skipping completed batch: {batch_id}")
            i += len(batch)
            continue

        start_time = time.time()
        try:
            rows = process_batch_with_retry(batch)
            total_rows += rows
            duration = time.time() - start_time
            logger.info(f"[Thread {batch_id}] {rows} rows in {duration:.2f}s")

            # adaptive batch sizing per thread
            if duration < 2 and current_batch_size < max_batch:
                current_batch_size = min(current_batch_size * 2, max_batch)
                logger.info(f"[Thread {batch_id}] Increasing batch size to {current_batch_size}")
            elif duration > 10 and current_batch_size > batch_size:
                current_batch_size = max(current_batch_size // 2, batch_size)
                logger.info(f"[Thread {batch_id}] Decreasing batch size to {current_batch_size}")

        except Exception as e:
            logger.error(f"Batch {batch_id} failed: {e}")
        i += len(batch)

    return total_rows


def convert_batches_parallel(resume_from=None, fresh_start=False):
    logger.info("Starting migration process with partitioned ZIP ranges (multi-threaded, deadlock-safe)...")
    logger.info(f"Configuration: initial_batch_size={batch_size}, max_threads={max_threads}")

    # Show initial statistics
    get_table_stats()

    # Initialize MySQL
    init_mysql_db(drop_existing=fresh_start)

    # Create indexes safely
    create_index_if_not_exists("latitude_longitude", "latitude, longitude")
    create_index_if_not_exists("number_street", "number, street")
    create_index_if_not_exists("state_city", "state, city")
    create_index_if_not_exists("zipcode_number", "zipcode, number")
    create_index_if_not_exists("country", "country")

    # Get all ZIP codes from SQLite
    conn_sqlite = sqlite3.connect(sqlite_db)
    cursor_sqlite = conn_sqlite.cursor()
    cursor_sqlite.execute(f"SELECT DISTINCT zipcode FROM {table} ORDER BY zipcode")
    all_zipcodes = [str(row[0]) for row in cursor_sqlite.fetchall()]
    conn_sqlite.close()

    logger.info(f"Found {len(all_zipcodes)} unique ZIP codes in SQLite")
    logger.info(f"ZIP code range: {all_zipcodes[0]} to {all_zipcodes[-1]}")

    # Filter ZIP codes if resuming
    if resume_from:
        resume_from_str = str(resume_from)
        original_count = len(all_zipcodes)
        all_zipcodes = [z for z in all_zipcodes if z >= resume_from_str]
        logger.info(f"Resuming from {resume_from_str}: processing {len(all_zipcodes)}/{original_count} ZIP codes")

    # Load completed batches
    completed = get_completed_batches()

    # Partition zipcodes across threads (disjoint slices)
    partitions = [all_zipcodes[i::max_threads] for i in range(max_threads)]

    total_rows_processed = 0
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(worker_thread, part, batch_size, completed) for part in partitions]

        for future in as_completed(futures):
            total_rows_processed += future.result()

    # Re-enable keys
    try:
        conn_mysql = mysql.connector.connect(user=mysql_user, password=mysql_pass, host="localhost", database=mysql_db)
        cursor_mysql = conn_mysql.cursor()
        cursor_mysql.execute(f"ALTER TABLE {table} ENABLE KEYS")
        conn_mysql.commit()
        conn_mysql.close()
        logger.info("Re-enabled MySQL keys")
    except Exception as e:
        logger.error(f"Error re-enabling keys: {e}")

    logger.info(f"Migration completed! Processed {total_rows_processed} rows")
    get_table_stats()

# VALIDATION FUNCTION
def validate_migration():
    """Check if migration completed successfully"""
    sqlite_total, sqlite_zipcodes, mysql_total, mysql_zipcodes = get_table_stats()
    
    logger.info("=== VALIDATION RESULTS ===")
    if mysql_total == sqlite_total and mysql_zipcodes == sqlite_zipcodes:
        logger.info("[SUCCESS] Migration appears successful - row counts match!")
    else:
        logger.warning("[INCOMPLETE] Migration may be incomplete:")
        logger.warning(f"  Rows: SQLite={sqlite_total:,}, MySQL={mysql_total:,}, Diff={sqlite_total-mysql_total:,}")
        logger.warning(f"  ZIP codes: SQLite={sqlite_zipcodes:,}, MySQL={mysql_zipcodes:,}, Diff={sqlite_zipcodes-mysql_zipcodes:,}")
        
# RUN
if __name__ == "__main__":
    
    # NORMAL MODE - optimized processing
    # convert_batches_parallel()
    
    # For a fresh start (drops existing table):
    convert_batches_parallel(fresh_start=True)
    
    # To resume from a specific ZIP code:
    # convert_batches_parallel(resume_from="12345")
    
    # Validate the results

    validate_migration()
