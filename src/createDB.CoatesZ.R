#------------------------------------------------------------
# Program: createDB.PractI.CoatesZ.R
# Author: Zachary Coates
# Semester: Spring 2025
# Description: This script connects to the Aiven MySQL database
# and creates the 3NF normalized schema with constraints.
#------------------------------------------------------------

# Load required libraries
if (!requireNamespace("DBI", quietly = TRUE)) install.packages("DBI", dependencies = TRUE)
if (!requireNamespace("RMySQL", quietly = TRUE)) install.packages("RMySQL", dependencies = TRUE)

library(DBI)
library(RMySQL)

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

# Function to create tables (without foreign keys)
create_core_tables <- function(con) {
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS `Restaurant` (
        `Restaurant ID` INT PRIMARY KEY AUTO_INCREMENT,
        `Restaurant Name` VARCHAR(200) NOT NULL UNIQUE
    );
  ")
  
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS `Server` (
        `Server ID` INT PRIMARY KEY AUTO_INCREMENT,
        `Server Emp ID` INT DEFAULT NULL,
        `Server Name` VARCHAR(200) DEFAULT NULL,
        `Start Date Hired` DATE DEFAULT NULL,
        `End Date Hired` DATE DEFAULT NULL,
        `Hourly Rate` DOUBLE DEFAULT 0.00 CHECK (`Hourly Rate` >= 0),
        `Server Birth Date` DATE DEFAULT NULL,
        `Server TIN` VARCHAR(20) DEFAULT NULL
    );
  ")
  
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS `Customer` (
        `Customer ID` INT PRIMARY KEY AUTO_INCREMENT,
        `Customer Phone` VARCHAR(20) UNIQUE DEFAULT NULL,
        `Customer Name` VARCHAR(255) DEFAULT NULL,
        `Customer Email` VARCHAR(255) UNIQUE DEFAULT NULL,
        `Loyalty Member` BOOLEAN DEFAULT FALSE
    );
  ")
  
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS `Meal Type` (
        `Meal ID` INT PRIMARY KEY AUTO_INCREMENT,
        `Meal Type` VARCHAR(50) NOT NULL UNIQUE
    );
  ")
  
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS `Payment Method` (
        `Payment ID` INT PRIMARY KEY AUTO_INCREMENT,
        `Type` VARCHAR(50) NOT NULL UNIQUE
    );
  ")
  
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS `Bill` (
        `Bill ID` INT PRIMARY KEY AUTO_INCREMENT,
        `Food Bill` DOUBLE DEFAULT 0.00 CHECK (`Food Bill` >= 0),
        `Tip Amount` DOUBLE DEFAULT 0.00 CHECK (`Tip Amount` >= 0),
        `Discount Applied` DOUBLE DEFAULT 0.00 CHECK (`Discount Applied` >= 0),
        `Alcohol Bill` DOUBLE DEFAULT 0.00 CHECK (`Alcohol Bill` >= 0)
    );
  ")
  
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS `Party` (
    `Party ID` INT PRIMARY KEY AUTO_INCREMENT,
    `Party Size` INT DEFAULT NULL,
    `Genders` VARCHAR(10) DEFAULT NULL,  -- Concatenated gender string (e.g., 'mf', 'u', 'mm')
    `Visit ID` INT DEFAULT NULL
    );
  ")

  
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS `Visit` (
        `Visit ID` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
        `Visit Date` DATE NOT NULL,
        `Visit Time` TIME DEFAULT NULL,
        `Wait Time` DOUBLE DEFAULT 0 CHECK (`Wait Time` >= 0)
    );
  ")
  
  # Ensure default "Unknown" server exists
  dbExecute(con, "
    INSERT IGNORE INTO `Server` (`Server ID`, `Server Emp ID`, `Server Name`, `Start Date Hired`, `End Date Hired`, `Hourly Rate`, `Server Birth Date`, `Server TIN`)
    VALUES (1, NULL, NULL, NULL, NULL, 0.00, NULL, NULL);
  ")
  
  # Ensure default "Unknown" customer exists
  dbExecute(con, "
    INSERT IGNORE INTO `Customer` (`Customer ID`, `Customer Phone`, `Customer Name`, `Customer Email`, `Loyalty Member`)
    VALUES (1, NULL, NULL, NULL, FALSE);
  ")
  
  dbExecute(con, "ALTER TABLE `Server` AUTO_INCREMENT = 2;")
  dbExecute(con, "ALTER TABLE `Customer` AUTO_INCREMENT = 2;")
  
  print("Core tables created successfully.")
}

add_foreign_keys <- function(con) {
  check_and_add_column <- function(table_name, column_name, column_def) {
    query <- sprintf("SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='%s' AND COLUMN_NAME='%s'", table_name, column_name)
    result <- dbGetQuery(con, query)
    
    if (result$cnt == 0) {
      alter_query <- sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s", table_name, column_name, column_def)
      dbExecute(con, alter_query)
    }
  }
  
  # Add necessary columns before foreign keys
  check_and_add_column("Server", "Restaurant ID", "INT")
  check_and_add_column("Visit", "Meal ID", "INT NOT NULL")
  check_and_add_column("Visit", "Restaurant ID", "INT NOT NULL")
  check_and_add_column("Visit", "Customer ID", "INT DEFAULT NULL")
  check_and_add_column("Visit", "Server ID", "INT DEFAULT NULL")
  check_and_add_column("Visit", "Bill ID", "INT DEFAULT NULL")
  check_and_add_column("Visit", "Party ID", "INT DEFAULT NULL")
  check_and_add_column("Bill", "Payment ID", "INT NOT NULL")
  check_and_add_column("Bill", "Visit ID", "INT NOT NULL")
  check_and_add_column("Party", "Visit ID", "INT NOT NULL")
  
  dbExecute(con, "ALTER TABLE `Server` ADD FOREIGN KEY (`Restaurant ID`) REFERENCES `Restaurant`(`Restaurant ID`) ON DELETE SET NULL")
  dbExecute(con, "ALTER TABLE `Visit` ADD FOREIGN KEY (`Meal ID`) REFERENCES `Meal Type`(`Meal ID`) ON DELETE RESTRICT")
  dbExecute(con, "ALTER TABLE `Visit` ADD FOREIGN KEY (`Restaurant ID`) REFERENCES `Restaurant`(`Restaurant ID`) ON DELETE CASCADE")
  dbExecute(con, "ALTER TABLE `Visit` ADD FOREIGN KEY (`Customer ID`) REFERENCES `Customer`(`Customer ID`) ON DELETE SET NULL")
  dbExecute(con, "ALTER TABLE `Visit` ADD FOREIGN KEY (`Server ID`) REFERENCES `Server`(`Server ID`) ON DELETE SET NULL")
  dbExecute(con, "ALTER TABLE `Visit` ADD FOREIGN KEY (`Bill ID`) REFERENCES `Bill`(`Bill ID`) ON DELETE SET NULL")
  dbExecute(con, "ALTER TABLE `Visit` ADD FOREIGN KEY (`Party ID`) REFERENCES `Party`(`Party ID`) ON DELETE SET NULL")
  dbExecute(con, "ALTER TABLE `Bill` ADD FOREIGN KEY (`Payment ID`) REFERENCES `Payment Method`(`Payment ID`) ON DELETE RESTRICT")
  dbExecute(con, "ALTER TABLE `Bill` ADD FOREIGN KEY (`Visit ID`) REFERENCES `Visit`(`Visit ID`) ON DELETE CASCADE")
  dbExecute(con, "ALTER TABLE `Party` ADD FOREIGN KEY (`Visit ID`) REFERENCES `Visit`(`Visit ID`) ON DELETE CASCADE")
  
  print("Foreign keys added successfully.")
}

# Main function to orchestrate the setup
main <- function() {
  con <- connect_db()
  
  create_core_tables(con)
  add_foreign_keys(con)
  
  dbDisconnect(con)
  print("Database setup complete.")
}

main()
