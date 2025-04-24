#------------------------------------------------------------
# Program: testDBLoading.PractI.CoatesZ.R
# Author: Zachary Coates
# Semester: Spring 2025
# Description: Tests the data loading process by comparing
# CSV data with the database contents.
#------------------------------------------------------------

# Load required libraries
if (!requireNamespace("DBI", quietly = TRUE)) install.packages("DBI", dependencies = TRUE)
if (!requireNamespace("RMySQL", quietly = TRUE)) install.packages("RMySQL", dependencies = TRUE)
if (!requireNamespace("data.table", quietly = TRUE)) install.packages("data.table", dependencies = TRUE)

library(DBI)
library(RMySQL)
library(data.table)

#------------------------------------------------------------
# Database Connection
#------------------------------------------------------------

connect_db <- function() {
  db_host <- "mysql-3cfa0d7f-northeastern-861d.g.aivencloud.com"
  db_port <- 15087
  db_name <- "defaultdb"
  db_user <- "avnadmin"
  db_password <- Sys.getenv("DB_PASSWORD")
  
  con <- dbConnect(RMySQL::MySQL(),
                   dbname = db_name,
                   host = db_host,
                   port = db_port,
                   user = db_user,
                   password = db_password)
  
  return(con)
}

#------------------------------------------------------------
# Load CSV Data
#------------------------------------------------------------

file_url <- "https://s3.us-east-2.amazonaws.com/artificium.us/datasets/restaurant-visits-139874.csv"
df <- fread(file_url, na.strings = c("", "NULL", "0000-00-00", "9999-99-99", "99", "999.99", "N/A"))

print("Data loaded successfully.")

#------------------------------------------------------------
# Compute CSV Summary Stats (Corrected Food Bill Calculation)
#------------------------------------------------------------

csv_summary <- list(
  num_restaurants = length(unique(df$Restaurant)),
  num_customers = length(unique(df$CustomerPhone[!is.na(df$CustomerPhone)])),  
  num_servers = length(unique(df$ServerEmpID[!is.na(df$ServerEmpID)])),        
  num_visits = nrow(df),   
  num_bills = sum(!is.na(df$FoodBill) | !is.na(df$TipAmount) | !is.na(df$AlcoholBill)),  
  total_food = round(sum(df[, .(VisitFoodTotal = sum(FoodBill, na.rm = TRUE)), by = "VisitDate"]$VisitFoodTotal), 2),
  total_alcohol = round(sum(df$AlcoholBill, na.rm = TRUE), 2),
  total_tips = round(sum(df$TipAmount, na.rm = TRUE), 2)
)

print("CSV summary statistics computed.")

#------------------------------------------------------------
# Compute Database Summary Stats (Corrected Food Bill Calculation)
#------------------------------------------------------------

test_db_loading <- function(con) {
  query <- "
    SELECT 
        (SELECT COUNT(*) FROM Restaurant) AS num_restaurants,
        (SELECT COUNT(*) FROM Customer) AS num_customers,
        (SELECT COUNT(*) FROM Server) AS num_servers,
        (SELECT COUNT(*) FROM Visit) AS num_visits,
        (SELECT COUNT(*) FROM Bill) AS num_bills,
        ROUND((SELECT SUM(FoodTotal) FROM (SELECT `Visit ID`, SUM(`Food Bill`) AS FoodTotal FROM Bill GROUP BY `Visit ID`) AS Sub), 2) AS total_food,
        ROUND((SELECT SUM(`Alcohol Bill`) FROM Bill), 2) AS total_alcohol,
        ROUND((SELECT SUM(`Tip Amount`) FROM Bill), 2) AS total_tips
  "
  
  result <- dbGetQuery(con, query)
  
  # Convert to a named list
  db_summary <- as.list(result)
  
  return(db_summary)
}

#------------------------------------------------------------
# Run Tests and Display Results
#------------------------------------------------------------

main <- function() {
  con <- connect_db()
  db_summary <- test_db_loading(con)
  
  cat("\nComparing CSV data with database...\n")
  
  compare_and_print <- function(name, csv_value, db_value) {
    if (csv_value == db_value) {
      cat(sprintf("%s **PASS** (CSV: %s | DB: %s)\n", name, csv_value, db_value))
    } else {
      cat(sprintf("%s **FAIL** (CSV: %s | DB: %s)\n", name, csv_value, db_value))
    }
  }
  
  compare_and_print("Total Restaurants", csv_summary$num_restaurants, db_summary$num_restaurants)
  compare_and_print("Total Customers", csv_summary$num_customers, db_summary$num_customers) 
  compare_and_print("Total Servers", csv_summary$num_servers, db_summary$num_servers -1) # 1 added for unknown 
  compare_and_print("Total Visits", csv_summary$num_visits, db_summary$num_visits)
  compare_and_print("Total Bills", csv_summary$num_bills, db_summary$num_bills)  # Compare bill counts
  compare_and_print("Total Food Bill", csv_summary$total_food, db_summary$total_food)
  compare_and_print("Total Alcohol Bill", csv_summary$total_alcohol, db_summary$total_alcohol)
  compare_and_print("Total Tip Amount", csv_summary$total_tips, db_summary$total_tips)
  
  # Close connection
  dbDisconnect(con)
  
  cat("Database loading test complete.\n")
}

main()
