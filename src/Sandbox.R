#------------------------------------------------------------
# Load Required Libraries
#------------------------------------------------------------

if (!requireNamespace("DBI", quietly = TRUE)) install.packages("DBI", dependencies = TRUE)
if (!requireNamespace("RMySQL", quietly = TRUE)) install.packages("RMySQL", dependencies = TRUE)
if (!requireNamespace("data.table", quietly = TRUE)) install.packages("data.table", dependencies = TRUE)

library(DBI)
library(RMySQL)
library(data.table)

#------------------------------------------------------------
# Connect to Database
#------------------------------------------------------------

connect_db <- function() {
  con <- dbConnect(RMySQL::MySQL(),
                   dbname = "defaultdb",
                   host = "mysql-3cfa0d7f-northeastern-861d.g.aivencloud.com",
                   port = 15087,
                   user = "avnadmin",
                   password = Sys.getenv("DB_PASSWORD"))
  return(con)
}

#------------------------------------------------------------
# Check for Missing Bill IDs in Visit Table
#------------------------------------------------------------

check_missing_bill_ids <- function(con) {
  query <- "SELECT COUNT(*) AS MissingBillIDs FROM Visit WHERE `Bill ID` IS NULL;"
  result <- dbGetQuery(con, query)
  print(sprintf("🔍 Missing Bill IDs in Visit Table: %d", result$MissingBillIDs))
}

#------------------------------------------------------------
# Check for Duplicate Food Bills Per Visit
#------------------------------------------------------------

check_duplicate_food_bills <- function(con) {
  query <- "
    SELECT `Visit ID`, COUNT(*) AS DuplicateFoodBills
    FROM Bill
    WHERE `Food Bill` > 0
    GROUP BY `Visit ID`
    HAVING COUNT(*) > 1;
  "
  
  result <- dbGetQuery(con, query)
  print(sprintf("🔍 Visits with Duplicate Food Bills: %d", nrow(result)))
  
  if (nrow(result) > 0) {
    print("🔎 Sample of Duplicate Food Bills:")
    print(head(result, 10))
  }
}

#------------------------------------------------------------
# Check for Duplicate Alcohol Bills Per Visit
#------------------------------------------------------------

check_duplicate_alcohol_bills <- function(con) {
  query <- "
    SELECT `Visit ID`, COUNT(*) AS DuplicateAlcoholBills
    FROM Bill
    WHERE `Alcohol Bill` > 0
    GROUP BY `Visit ID`
    HAVING COUNT(*) > 1;
  "
  
  result <- dbGetQuery(con, query)
  print(sprintf("🔍 Visits with Duplicate Alcohol Bills: %d", nrow(result)))
  
  if (nrow(result) > 0) {
    print("🔎 Sample of Duplicate Alcohol Bills:")
    print(head(result, 10))
  }
}

#------------------------------------------------------------
# Run the Script
#------------------------------------------------------------

main <- function() {
  con <- connect_db()
  
  check_missing_bill_ids(con)
  check_duplicate_food_bills(con)
  check_duplicate_alcohol_bills(con)
  
  dbDisconnect(con)
  print(" Data validation complete.")
}

# Run the script
main()
