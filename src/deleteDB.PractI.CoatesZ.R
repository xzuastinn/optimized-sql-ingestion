#------------------------------------------------------------
# Program: deleteDB.PractI.CoatesZ.R
# Author: Zachary Coates
# Semester: Spring 2025
# Description: This script connects to the Aiven MySQL database
# and deletes (DROPs) all tables if they exist.
#------------------------------------------------------------

# Load required libraries
if (!requireNamespace("DBI", quietly = TRUE)) install.packages("DBI", dependencies = TRUE)
if (!requireNamespace("RMySQL", quietly = TRUE)) install.packages("RMySQL", dependencies = TRUE)

library(DBI)
library(RMySQL)

# Get all active connections
all_conns <- dbListConnections(RMySQL::MySQL())

# Disconnect all connections
for (conn in all_conns) {
  dbDisconnect(conn)
}
# Function to establish database connection
connect_db <- function() {
  db_host <- "mysql-3cfa0d7f-northeastern-861d.g.aivencloud.com"
  db_port <- 15087
  db_name <- "defaultdb"
  db_user <- "avnadmin"
  db_password <- Sys.getenv("DB_PASSWORD")
  ssl_mode <- "REQUIRED"
  
  con <- dbConnect(RMySQL::MySQL(),
                   dbname = db_name,
                   host = db_host,
                   port = db_port,
                   user = db_user,
                   password = db_password,
                   sslmode = ssl_mode)
  
  return(con)
}

# Function to drop all tables
drop_all_tables <- function(con) {
  # Get all table names
  tables <- dbListTables(con)
  
  if (length(tables) == 0) {
    print("No tables found. Database is already empty.")
    return()
  }
  
  # Disable foreign key checks to avoid dependency issues
  dbExecute(con, "SET FOREIGN_KEY_CHECKS = 0;")
  
  for (table in tables) {
    drop_query <- sprintf("DROP TABLE IF EXISTS `%s`;", table)
    dbExecute(con, drop_query)
    cat(sprintf("Dropped table: %s\n", table))
  }
  
  # Re-enable foreign key checks
  dbExecute(con, "SET FOREIGN_KEY_CHECKS = 1;")
  
  print("All tables dropped successfully.")
}

# Main function to orchestrate the cleanup
main <- function() {
  con <- connect_db()
  
  drop_all_tables(con)
  
  dbDisconnect(con)
  print("Database cleanup complete.")
}

main()
