# address-db-conversion
Bash project to convert addresses database from sqlite to mysql.

# set up
1. Request ISTE-Ubuntu 22.04 w/Apache, MySQL, PHP deployment
2. Download sqlite
   ```
   sudo apt install sqlite3
   ```
   This may be held up by an unattended-upgr if you just created the vm, let that finish first.
3. Clone this repo
4. Update the permissions for the script
   ```
   chmod 755 import.py
   ```
5. Download the sqlite file and put it in the project folder<br>
   This file is too large to store in the repo, but it can be found in the Jira.<br>
   Note: when you make any commits make sure not to include this file
6. Enable local_infile by adding to /etc/mysql/my.cnf<br>
   You need to use sudo to edit this file. I opened it with nano:
   ```
   sudo nano /etc/mysql/my.cnf
   ```
   Add these lines to the bottom:
   ```
   [mysqld]
   local_infile = 1

   [client]
   local_infile = 1
   ```
7. Restart MySQL after step 6
   ```
   sudo systemctl restart mysql
   ```
8. Set config settings
   ```
   sqlite_db =
   mysql_pass =
   ```
9. Choose run parameters  (Comment out what isn't being used. USE FRESH START ON FIRST RUN)
   ```
   NORMAL MODE - optimized processing
   convert_batches_parallel()
    
   For a fresh start (drops existing table):
   convert_batches_parallel(fresh_start=True)
    
   To resume from a specific ZIP code:
   convert_batches_parallel(resume_from="12345")
    
   Validate the results
    validate_migration()
    ```
10. Sit and wait 
