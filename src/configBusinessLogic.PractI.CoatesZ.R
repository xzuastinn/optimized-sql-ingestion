# ============================================================
# Program: configBusinessLogic.PractI.CoatesZ.R
# Author: Zachary Coates
# Semester: Spring 2025
# Description: Defines and tests the `storeVisit` stored procedure.
# ============================================================

# Load required libraries
if (!requireNamespace("DBI", quietly = TRUE)) install.packages("DBI", dependencies = TRUE)
if (!requireNamespace("RMySQL", quietly = TRUE)) install.packages("RMySQL", dependencies = TRUE)

library(DBI)
library(RMySQL)

# ============================================================
# Database Connection Function
# ============================================================

connect_db <- function() {
  con <- dbConnect(RMySQL::MySQL(),
                   dbname = "defaultdb",
                   host = "mysql-3cfa0d7f-northeastern-861d.g.aivencloud.com",
                   port = 15087,
                   user = "avnadmin",
                   password = Sys.getenv("DB_PASSWORD"))
  return(con)
}

# ============================================================
# Drop Existing Stored Procedure (if any) & Create New One
# ============================================================

create_stored_procedures <- function(con) {
  dbExecute(con, "DROP PROCEDURE IF EXISTS storeVisit")
  dbExecute(con, "DROP PROCEDURE IF EXISTS storeNewVisit")
  
  storeVisitSQL <- "
  CREATE PROCEDURE storeVisit(
      IN p_restaurant_id INT,
      IN p_customer_id INT,
      IN p_visit_date DATE,
      IN p_visit_time TIME,
      IN p_meal_id INT,
      IN p_party_size INT,
      IN p_food_bill DECIMAL(10,2),
      IN p_alcohol_bill DECIMAL(10,2),
      IN p_payment_method VARCHAR(50),
      IN p_server_id INT
  )
  BEGIN
      DECLARE v_party_id INT;
      DECLARE v_bill_id INT;
      DECLARE v_visit_id INT;

      -- Insert Party
      INSERT INTO Party (`Party Size`) VALUES (p_party_size);
      SET v_party_id = LAST_INSERT_ID();

      -- Insert Visit
      INSERT INTO Visit (`Visit Date`, `Visit Time`, `Meal ID`, `Restaurant ID`, `Customer ID`, `Server ID`, `Party ID`)
      VALUES (p_visit_date, p_visit_time, p_meal_id, p_restaurant_id, p_customer_id, p_server_id, v_party_id);
      SET v_visit_id = LAST_INSERT_ID();

      -- Insert Bill (Ensure Payment Method exists)
      INSERT INTO Bill (`Food Bill`, `Alcohol Bill`, `Payment ID`, `Visit ID`)
      VALUES (
          p_food_bill, 
          p_alcohol_bill, 
          (SELECT `Payment ID` FROM `Payment Method` WHERE `Type` = p_payment_method LIMIT 1), 
          v_visit_id
      );
      SET v_bill_id = LAST_INSERT_ID();

      -- Update Visit with Bill ID
      UPDATE Visit SET `Bill ID` = v_bill_id WHERE `Visit ID` = v_visit_id;

      -- Return Visit ID
      SELECT v_visit_id AS New_Visit_ID;
  END;"
  
  storeNewVisitSQL <- "
  CREATE PROCEDURE storeNewVisit(
      IN p_restaurant_name VARCHAR(200),
      IN p_customer_phone VARCHAR(20),
      IN p_customer_name VARCHAR(255),
      IN p_customer_email VARCHAR(255),
      IN p_visit_date DATE,
      IN p_visit_time TIME,
      IN p_meal_id INT,
      IN p_party_size INT,
      IN p_food_bill DECIMAL(10,2),
      IN p_alcohol_bill DECIMAL(10,2),
      IN p_payment_method VARCHAR(50),
      IN p_server_name VARCHAR(200),
      IN p_hourly_rate DECIMAL(10,2)
  )
  BEGIN
      DECLARE v_restaurant_id INT;
      DECLARE v_customer_id INT;
      DECLARE v_server_id INT;
      DECLARE v_party_id INT;
      DECLARE v_bill_id INT;
      DECLARE v_visit_id INT;

      SELECT `Restaurant ID` INTO v_restaurant_id FROM Restaurant WHERE `Restaurant Name` = p_restaurant_name LIMIT 1;
      IF v_restaurant_id IS NULL THEN
          INSERT INTO Restaurant (`Restaurant Name`) VALUES (p_restaurant_name);
          SET v_restaurant_id = LAST_INSERT_ID();
      END IF;

      SELECT `Customer ID` INTO v_customer_id FROM Customer 
      WHERE `Customer Phone` = p_customer_phone OR `Customer Email` = p_customer_email LIMIT 1;
      IF v_customer_id IS NULL THEN
          INSERT INTO Customer (`Customer Phone`, `Customer Name`, `Customer Email`, `Loyalty Member`)
          VALUES (p_customer_phone, p_customer_name, p_customer_email, FALSE);
          SET v_customer_id = LAST_INSERT_ID();
      END IF;

      SELECT `Server ID` INTO v_server_id FROM Server WHERE `Server Name` = p_server_name LIMIT 1;
      IF v_server_id IS NULL THEN
          INSERT INTO Server (`Server Name`, `Hourly Rate`) VALUES (p_server_name, p_hourly_rate);
          SET v_server_id = LAST_INSERT_ID();
      END IF;

      INSERT INTO Party (`Party Size`) VALUES (p_party_size);
      SET v_party_id = LAST_INSERT_ID();

      INSERT INTO Visit (`Visit Date`, `Visit Time`, `Meal ID`, `Restaurant ID`, `Customer ID`, `Server ID`, `Party ID`)
      VALUES (p_visit_date, p_visit_time, p_meal_id, v_restaurant_id, v_customer_id, v_server_id, v_party_id);
      SET v_visit_id = LAST_INSERT_ID();

      INSERT INTO Bill (`Food Bill`, `Alcohol Bill`, `Payment ID`, `Visit ID`)
      VALUES (
          p_food_bill, 
          p_alcohol_bill, 
          (SELECT `Payment ID` FROM `Payment Method` WHERE `Type` = p_payment_method LIMIT 1), 
          v_visit_id
      );
      SET v_bill_id = LAST_INSERT_ID();

      UPDATE Visit SET `Bill ID` = v_bill_id WHERE `Visit ID` = v_visit_id;

      SELECT v_visit_id AS New_Visit_ID;
  END;"
  
  dbExecute(con, storeVisitSQL)
  dbExecute(con, storeNewVisitSQL)
  print("Stored procedure `storeVisit` and `storeNewVisit` created successfully.")
}

# ============================================================
# Test the `storeVisit` and `storeNewVisit` Stored Procedures
# ============================================================

test_storeVisit <- function(con) {
  print("Running test for `storeVisit` procedure...")
  
  # Call stored procedure correctly
  query <- "CALL storeVisit(1, 3, '2025-03-11', '18:30:00', 2, 4, 120.50, 30.00, 'Credit Card', 2)"
  res <- dbSendQuery(con, query)  # Send the stored procedure call
  result <- dbFetch(res)  # Fetch the result properly
  dbClearResult(res)  # Ensure no lingering results
  
  # Ensure the stored procedure returned a Visit ID
  if (nrow(result) == 0 || is.na(result$New_Visit_ID[1])) {
    stop("Error: `storeVisit` did not return a valid Visit ID.")
  }
  
  visit_id <- result$New_Visit_ID[1]
  print(paste("Test Visit ID:", visit_id))
  
  # Reconnect to avoid "out of sync" issues
  dbDisconnect(con)  # Close the existing connection
  con <- connect_db()  # Reconnect
  
  # Query inserted records safely
  visit_check <- dbGetQuery(con, sprintf("SELECT * FROM Visit WHERE `Visit ID` = %s", visit_id))
  bill_check <- dbGetQuery(con, sprintf("SELECT * FROM Bill WHERE `Visit ID` = %s", visit_id))
  party_check <- dbGetQuery(con, sprintf("SELECT * FROM Party WHERE `Party ID` = (SELECT `Party ID` FROM Visit WHERE `Visit ID` = %s)", visit_id))
  
  print("Inserted Visit Record:")
  print(visit_check)
  
  print("Inserted Bill Record:")
  print(bill_check)
  
  print("Inserted Party Record:")
  print(party_check)
  
  return(list(visit_id = visit_id, con = con))  # Return Visit ID and connection
}

test_storeNewVisit <- function() {
  print("Running test for `storeNewVisit` procedure...")
  
  con <- connect_db()
  #Ensure the connection is valid before execution
  if (!dbIsValid(con)) {
    print("Reconnecting to database before executing storeNewVisit...")
    con <- connect_db()
  }
  
  #Call stored procedure correctly
  query <- "CALL storeNewVisit('New Restaurant', '555-9999', 'Alice Example', 'alice@example.com', '2025-03-11', '19:00:00', 3, 4, 110.00, 25.00, 'Cash', 'Bob Waiter', 18.00)"
  
  res <- dbSendQuery(con, query)  # Execute the stored procedure
  result <- dbFetch(res)  # Fetch the Visit ID result
  dbClearResult(res)  # Clear query result to avoid sync issues
  
  #Ensure the stored procedure returned a Visit ID
  if (nrow(result) == 0 || is.na(result$New_Visit_ID[1])) {
    stop("Error: `storeNewVisit` did not return a valid Visit ID.")
  }
  
  visit_id <- result$New_Visit_ID[1]
  print(paste("Test New Visit ID:", visit_id))
  
  #Reconnect to avoid "out of sync" issues
  dbDisconnect(con)  # Close the existing connection
  con <- connect_db()  # Reconnect
  
  #Query inserted records to verify presence
  visit_check <- dbGetQuery(con, sprintf("SELECT * FROM Visit WHERE `Visit ID` = %s", visit_id))
  bill_check <- dbGetQuery(con, sprintf("SELECT * FROM Bill WHERE `Visit ID` = %s", visit_id))
  party_check <- dbGetQuery(con, sprintf("SELECT * FROM Party WHERE `Party ID` = (SELECT `Party ID` FROM Visit WHERE `Visit ID` = %s)", visit_id))
  
  print("Inserted Visit Record:")
  print(visit_check)
  
  print("Inserted Bill Record:")
  print(bill_check)
  
  print("Inserted Party Record:")
  print(party_check)
  
  return(list(visit_id = visit_id, con = con))  # Return Visit ID and connection
}

# ============================================================
# Cleanup Function: Delete Test Data
# ============================================================

cleanup_test_data <- function(con, visit_id) {
  print("Cleaning up test data...")
  
  # Ensure visit_id is a single value
  if (length(visit_id) > 1) {
    print("Warning: Multiple Visit IDs detected. Using only the first one.")
    visit_id <- visit_id[1]
  }
  
  # Ensure visit_id is valid
  if (is.na(visit_id) || visit_id == "") {
    print("Error: Visit ID is invalid, skipping cleanup.")
    return()
  }
  
  # Ensure the connection is valid before proceeding
  if (!dbIsValid(con)) {
    print("Reconnecting to database...")
    con <- connect_db()
  }
  
  # Debugging logs
  print(paste("Cleaning up Visit ID:", visit_id))
  
  # Use transactions to ensure referential integrity
  dbExecute(con, "START TRANSACTION")
  
  tryCatch({
    # Get associated Bill ID & Party ID safely for the test Visit ID only
    bill_query <- dbGetQuery(con, sprintf("SELECT `Bill ID` FROM Bill WHERE `Visit ID` = %s", visit_id))
    party_query <- dbGetQuery(con, sprintf("SELECT `Party ID` FROM Visit WHERE `Visit ID` = %s", visit_id))
    
    bill_id <- ifelse(nrow(bill_query) > 0, bill_query$`Bill ID`[1], NA)
    party_id <- ifelse(nrow(party_query) > 0, party_query$`Party ID`[1], NA)
    
    print(paste("Bill ID:", bill_id, "| Party ID:", party_id))
    
    # Delete records if they exist (only for the test visit)
    if (!is.na(bill_id)) {
      dbExecute(con, sprintf("DELETE FROM Bill WHERE `Bill ID` = %s", bill_id))
      print(paste("Deleted Bill ID:", bill_id))
    }
    
    if (!is.na(visit_id)) {
      dbExecute(con, sprintf("DELETE FROM Visit WHERE `Visit ID` = %s", visit_id))
      print(paste("Deleted Visit ID:", visit_id))
    }
    
    if (!is.na(party_id)) {
      dbExecute(con, sprintf("DELETE FROM Party WHERE `Party ID` = %s", party_id))
      print(paste("Deleted Party ID:", party_id))
    }
    
    dbExecute(con, "COMMIT")  # Commit the transaction
    print("Test data successfully deleted.")
    
  }, error = function(e) {
    dbExecute(con, "ROLLBACK")  # Rollback if there's an error
    print("Error during cleanup. Transaction rolled back.")
    print(e)
  })
}

# ============================================================
# Main Execution
# ============================================================

main <- function() {
  
  # Close all existing connections before proceeding
  all_cons <- dbListConnections(RMySQL::MySQL())
  for (con in all_cons) {
    dbDisconnect(con)
  }
  
  con <- connect_db()
  
  # Create the stored procedure
  create_stored_procedures(con)
  
  # Run the test
  test_result <- test_storeVisit(con)
  
  # Extract Visit ID
  test_visit_id <- test_result$visit_id
  test_new_visit_id <- test_storeNewVisit()
  
  # Ensure connection is valid before cleanup
  if (!dbIsValid(test_result$con)) {
    print("Reconnecting before cleanup...")
    con <- connect_db()
  } else {
    con <- test_result$con  # Use valid connection
  }
  
  cleanup_test_data(con, test_visit_id)
  cleanup_test_data(con, test_new_visit_id)
  
  
  dbDisconnect(con)
  print("Database connection closed successfully.")
  
}

# Run the script
main()
