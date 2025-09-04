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
   chmod 755 convert.sh
   ```
6. Download the sqlite file and put it in the project folder
   This file is too large to store in the repo, but it can be found in the Jira.
   Note: when you make any commits make sure not to include this file
7. Enable local_infile by adding the following lines to /etc/mysql/my.cnf
   ```
   [mysqld]
   local_infile = 1
   ```
8. Restart MySQL after step 7
   ```
   sudo systemctl restart mysql
   ```
