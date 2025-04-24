#------------------------------------------------------------
# Program: loadDB.PractI.CoatesZ.R
# Author: Zachary Coates
# Semester: Spring 2025
# Description: Loads data from a CSV file into MySQL efficiently.
#------------------------------------------------------------

#------------------------------------------------------------
# Load required libraries
#------------------------------------------------------------
if (!requireNamespace("DBI", quietly = TRUE)) install.packages("DBI", dependencies = TRUE)
if (!requireNamespace("RMySQL", quietly = TRUE)) install.packages("RMySQL", dependencies = TRUE)
if (!requireNamespace("readr", quietly = TRUE)) install.packages("readr", dependencies = TRUE)
if (!requireNamespace("data.table", quietly = TRUE)) install.packages("data.table", dependencies = TRUE)

library(DBI)
library(RMySQL)
library(readr)
library(data.table)

#------------------------------------------------------------
# Database Connection
#------------------------------------------------------------

connect_db <- function() {
  con <- dbConnect(RMySQL::MySQL(),
                   dbname = "defaultdb",
                   host = "mysql-3cfa0d7f-northeastern-861d.g.aivencloud.com",
                   port = 15087,
                   user = "avnadmin",
                   password = Sys.getenv("DB_PASSWORD")
  return(con)
}

#------------------------------------------------------------
# Load Data
#------------------------------------------------------------

file_url <- "https://s3.us-east-2.amazonaws.com/artificium.us/datasets/restaurant-visits-139874.csv"
df <- fread(file_url, na.strings = c("", "NULL", "0000-00-00", "9999-99-99", "999.99", "N/A"))
# Ensure VisitDate is properly formatted as Date
df[, VisitDate := as.Date(VisitDate, format="%Y-%m-%d")]

# Convert negative or unrealistic values to NA
df$WaitTime[df$WaitTime < 0] <- NA
df$PartySize[df$PartySize > 75] <- NA

#------------------------------------------------------------
# Batch Insert Function
#------------------------------------------------------------

batch_insert <- function(con, table_name, df, columns, batch_size = 5000) {
  total_rows <- nrow(df)
  if (total_rows == 0) return()
  
  for (start in seq(1, total_rows, by = batch_size)) {
    end <- min(start + batch_size - 1, total_rows)
    batch <- df[start:end, ]
    
    values <- apply(batch, 1, function(row) {
      formatted_row <- sapply(names(row), function(col) {
        value <- row[[col]]
        
        if (is.na(value) || value == "NA" || value == "" || value == "NULL") {
          return("NULL")
        } else if (col == "LoyaltyMember") {
          return(ifelse(as.character(value) == "True", "1", "0"))  # Ensure explicit conversion
        } else if (col %in% c("StartDateHired", "EndDateHired", "ServerBirthDate")) {
          date_val <- convert_date(value)
          return(ifelse(is.na(date_val), "NULL", sprintf("'%s'", date_val)))
        } else {
          return(sprintf("'%s'", gsub("'", "''", value)))
        }
      })
      sprintf("(%s)", paste(formatted_row, collapse = ", "))
    })
    
    query <- sprintf("INSERT INTO `%s` (%s) VALUES %s;",
                     table_name, paste(sprintf("`%s`", columns), collapse = ", "), paste(values, collapse = ", "))
    
    dbExecute(con, query)
  }
}

#------------------------------------------------------------
#Date Conversion Function
#------------------------------------------------------------

convert_date <- function(date_value) {
  if (is.na(date_value) || date_value == "" || date_value %in% c("NA", "NULL")) {
    return(NA)  #Return actual NA (R equivalent of SQL NULL)
  } else {
    tryCatch({
      format(as.Date(date_value, tryFormats = c("%m/%d/%Y", "%Y-%m-%d")), "%Y-%m-%d")
    }, error = function(e) {
      return(NA)  #Return NA if parsing fails
    })
  }
}

#------------------------------------------------------------
# Insert Lookup Data (static values)
#------------------------------------------------------------

insert_lookup_data <- function(con) {
  dbExecute(con, "INSERT IGNORE INTO `Meal Type` (`Meal ID`, `Meal Type`) VALUES (1, 'Breakfast'), (2, 'Lunch'), (3, 'Dinner'), (4, 'Take-Out');")
  dbExecute(con, "INSERT IGNORE INTO `Payment Method` (`Payment ID`, `Type`) VALUES (1, 'Cash'), (2, 'Credit Card'), (3, 'Mobile Payment');")
}

#------------------------------------------------------------
# Insert Base Data
#------------------------------------------------------------

insert_restaurants <- function(con, df, batch_size = 5000) {
  if ("Restaurant" %in% names(df)) {
    #Extract unique restaurant names from "Restaurant" (CSV column)
    restaurants <- unique(df[!is.na(Restaurant), .(Restaurant)])
    
    total_rows <- nrow(restaurants)
    if (total_rows == 0) return()  # Exit if no data
    
    for (start in seq(1, total_rows, by = batch_size)) {
      end <- min(start + batch_size - 1, total_rows)
      batch <- restaurants[start:end, ]
      
      # Batch insert into MySQL, mapping "Restaurant" (CSV) -> "Restaurant Name" (MySQL)
      values <- paste(sprintf("('%s')", batch$Restaurant), collapse = ", ")
      
      query <- sprintf("INSERT INTO `Restaurant` (`Restaurant Name`) VALUES %s ON DUPLICATE KEY UPDATE `Restaurant Name` = VALUES(`Restaurant Name`);", values)
      
      dbExecute(con, query)
    }
  }
}

insert_servers <- function(con, df) {
  if ("ServerEmpID" %in% names(df)) {
    servers <- unique(df[, .(ServerEmpID, ServerName, StartDateHired, EndDateHired, HourlyRate, ServerBirthDate, ServerTIN)])
    
    for (i in 1:nrow(servers)) {
      # Function to clean date values and handle missing/invalid dates
      clean_date <- function(date_value) {
        if (is.na(date_value) || date_value == "" || date_value %in% c("NA", "N/A", "NULL")) {
          return("NULL")
        } else {
          tryCatch({
            return(sprintf("'%s'", format(as.Date(date_value, tryFormats = c("%m/%d/%Y", "%Y-%m-%d")), "%Y-%m-%d")))
          }, error = function(e) {
            return("NULL")  # If date conversion fails, default to NULL
          })
        }
      }
      
      start_date <- clean_date(servers$StartDateHired[i])
      end_date <- clean_date(servers$EndDateHired[i])
      birth_date <- clean_date(servers$ServerBirthDate[i])
      
      #Handle empty or missing ServerTIN values
      server_tin <- ifelse(is.na(servers$ServerTIN[i]) || servers$ServerTIN[i] == "", "NULL", sprintf("'%s'", servers$ServerTIN[i]))
      
      #Check if the server already exists
      if (is.na(servers$ServerEmpID[i])) {
        # Special check for servers with NULL `ServerEmpID`
        check_query <- "SELECT COUNT(*) AS count FROM Server WHERE `Server Emp ID` IS NULL"
      } else {
        check_query <- sprintf("SELECT COUNT(*) AS count FROM Server WHERE `Server Emp ID` = %s", servers$ServerEmpID[i])
      }
      
      existing_count <- dbGetQuery(con, check_query)$count
      
      if (existing_count == 0) {  #Only insert if the server does not exist
        query <- sprintf("INSERT INTO Server (`Server Emp ID`, `Server Name`, `Start Date Hired`, `End Date Hired`, `Hourly Rate`, `Server Birth Date`, `Server TIN`) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s)",
                         ifelse(is.na(servers$ServerEmpID[i]), "NULL", servers$ServerEmpID[i]),
                         ifelse(is.na(servers$ServerName[i]), "NULL", sprintf("'%s'", servers$ServerName[i])),
                         start_date, 
                         end_date, 
                         ifelse(is.na(servers$HourlyRate[i]), "NULL", as.character(servers$HourlyRate[i])),
                         birth_date, 
                         server_tin)  #Ensure empty TIN is NULL
        dbExecute(con, query)
      }
    }
  }
}

insert_customers <- function(con, df) {
  if (any(c("CustomerPhone", "CustomerEmail") %in% names(df))) {
    customers <- unique(df[, .(CustomerPhone, CustomerName, CustomerEmail, LoyaltyMember)])
    
    for (i in 1:nrow(customers)) {
      # Step 1: Check if the customer already exists (matching by phone OR email)
      customer_id_query <- sprintf("
        SELECT `Customer ID` FROM Customer 
        WHERE (COALESCE(`Customer Phone`, '') = COALESCE(%s, '')) 
           OR (COALESCE(`Customer Email`, '') = COALESCE(%s, ''))
        LIMIT 1;",
                                   ifelse(is.na(customers$CustomerPhone[i]), "NULL", sprintf("'%s'", customers$CustomerPhone[i])),
                                   ifelse(is.na(customers$CustomerEmail[i]), "NULL", sprintf("'%s'", customers$CustomerEmail[i]))
      )
      
      result <- dbGetQuery(con, customer_id_query)
      
      if (nrow(result) == 0) {  
        # Step 2: Insert new customer if no match found
        insert_customer_query <- sprintf("
          INSERT INTO Customer (`Customer Phone`, `Customer Name`, `Customer Email`, `Loyalty Member`) 
          VALUES (%s, %s, %s, %s);",
                                         ifelse(is.na(customers$CustomerPhone[i]), "NULL", sprintf("'%s'", customers$CustomerPhone[i])),
                                         ifelse(is.na(customers$CustomerName[i]), "NULL", sprintf("'%s'", customers$CustomerName[i])),
                                         ifelse(is.na(customers$CustomerEmail[i]), "NULL", sprintf("'%s'", customers$CustomerEmail[i])),
                                         ifelse(is.na(customers$LoyaltyMember[i]), "0", as.integer(customers$LoyaltyMember[i])) 
        )
        dbExecute(con, insert_customer_query)
      }
    }
    
    #Step 3: Fetch all customers to create a mapping of phone numbers and emails to IDs
    customer_map <- dbGetQuery(con, "SELECT `Customer ID`, `Customer Phone`, `Customer Email` FROM Customer")
    
    return(customer_map)  #Retun mapping for use in `insert_visits`
  }
}

#------------------------------------------------------------
# Insert Visits (Optimized with Pre-mapped Foreign Keys)
#------------------------------------------------------------

insert_visits <- function(con, df, batch_size = 5000) {
  required_columns <- c("VisitDate", "VisitTime", "WaitTime", "MealType", "Restaurant", "CustomerPhone", "CustomerEmail", "ServerEmpID")
  missing_columns <- setdiff(required_columns, names(df))
  
  visits <- df[, .(VisitDate, VisitTime, WaitTime, MealType, Restaurant, CustomerPhone, CustomerEmail, ServerEmpID)]
  
  #Load Foreign Key Mappings
  customer_map <- as.data.table(dbGetQuery(con, "SELECT `Customer ID`, `Customer Phone`, `Customer Email` FROM Customer"))
  restaurant_map <- as.data.table(dbGetQuery(con, "SELECT `Restaurant ID`, `Restaurant Name` FROM Restaurant"))
  server_map <- as.data.table(dbGetQuery(con, "SELECT `Server ID`, `Server Emp ID` FROM Server"))
  meal_map <- as.data.table(dbGetQuery(con, "SELECT `Meal ID`, `Meal Type` FROM `Meal Type`"))
  
  # Normalize Data
  setDT(visits)
  visits[, Restaurant := trimws(Restaurant)]
  visits[, MealType := tolower(trimws(MealType))]
  visits[, ServerEmpID := as.integer(ServerEmpID)]
  
  #Merge Foreign Keys
  visits <- merge(visits, customer_map, by.x = c("CustomerPhone", "CustomerEmail"), 
                  by.y = c("Customer Phone", "Customer Email"), all.x = TRUE, allow.cartesian = FALSE)
  visits[is.na(`Customer ID`), `Customer ID` := 1]  # Default to 1 if missing
  
  visits <- merge(visits, restaurant_map, by.x = "Restaurant", by.y = "Restaurant Name", all.x = TRUE, allow.cartesian = FALSE)
  visits[is.na(`Restaurant ID`), `Restaurant ID` := 1]  # Default to 1 if missing
  
  visits <- merge(visits, server_map, by.x = "ServerEmpID", by.y = "Server Emp ID", all.x = TRUE, allow.cartesian = FALSE)
  visits[is.na(`Server ID`), `Server ID` := 1]  # Default to 1 if missing
  
  visits <- merge(visits, meal_map, by.x = "MealType", by.y = "Meal Type", all.x = TRUE, allow.cartesian = FALSE)
  visits[is.na(`Meal ID`), `Meal ID` := 1]  # Default to 1 if missing
  
  #Insert `visits` into the DB (WITHOUT `Party ID` for now)
  batch_insert(con, "Visit", visits[, .(`Visit Date` = VisitDate, 
                                        `Visit Time` = VisitTime, 
                                        `Wait Time` = WaitTime, 
                                        `Meal ID`, 
                                        `Restaurant ID`, 
                                        `Customer ID`, 
                                        `Server ID`)],
               c("Visit Date", "Visit Time", "Wait Time", "Meal ID", "Restaurant ID", "Customer ID", "Server ID"),
               batch_size = 5000)
  
  #fetch `Visit ID` mapping from DB
  visit_map <- as.data.table(dbGetQuery(con, "
    SELECT `Visit ID`, `Visit Date`, `Restaurant ID`, `Customer ID`
    FROM Visit;
"))
  
  # Ensure Visit Date is properly formatted as Date
  visit_map[, `Visit Date` := as.Date(`Visit Date`, format="%Y-%m-%d")]
  
  #Remove duplicate entries in visit_map (Optimized)
  visit_map <- visit_map[, .SD[1], by = c("Visit Date", "Restaurant ID", "Customer ID")]
  
  #Merge `Visit ID` back into `visits`
  visits <- merge(visits, visit_map, by.x = c("VisitDate", "Restaurant ID", "Customer ID"),
                  by.y = c("Visit Date", "Restaurant ID", "Customer ID"),
                  all.x = TRUE, allow.cartesian = FALSE)
  
  
  #Fetch `Party ID` mapping from DB
  party_map <- as.data.table(dbGetQuery(con, "
    SELECT `Party ID`, `Visit ID`
    FROM Party;
  "))
  
  #Merge `Party ID` using `Visit ID`
  visits <- merge(visits, party_map, by = "Visit ID", all.x = TRUE, allow.cartesian = FALSE)
  
  # Assign default `Party ID` if missing
  visits[is.na(`Party ID`), `Party ID` := 1]
  
  #Use a batch UPDATE instead of looping
  update_query <- "
      UPDATE Visit v
      JOIN (
          SELECT `Visit ID`, `Party ID`
          FROM Party
      ) p ON v.`Visit ID` = p.`Visit ID`
      SET v.`Party ID` = p.`Party ID`
      WHERE v.`Party ID` IS NULL;
  "
  
  dbExecute(con, update_query)
}

#------------------------------------------------------------
# Insert Parties
#------------------------------------------------------------
insert_parties <- function(con, df, batch_size = 5000) {
  
  #Fetch Visit IDs from the database
  visit_db_map <- dbGetQuery(con, "SELECT `Visit ID` FROM `Visit`")
  visit_db_map <- as.data.table(visit_db_map)
  
  #Extract relevant party data for each visit
  parties <- unique(df[, .(VisitID, PartySize)])  
  
  #Ensure missing Party Size is set to a default value**
  parties[is.na(PartySize), PartySize := 0]  
  
  #Fetch Existing Parties from the database**
  existing_parties <- dbGetQuery(con, "SELECT `Party ID`, `Visit ID` FROM `Party`")
  existing_parties <- as.data.table(existing_parties)
  
  #Identify visits that need a Party ID (no duplicate Visit IDs)
  new_parties <- parties[!(VisitID %in% existing_parties$`Visit ID`)]
  
  if (nrow(new_parties) > 0) {
    # Add placeholder column for `Genders`
    new_parties[, `Genders` := NA]
    
    # **6️⃣ Batch insert new parties**
    batch_insert(con, "Party", new_parties, c("Visit ID", "Party Size", "Genders"), batch_size)
  }
  
  #Fetch Updated Party Mapping (AFTER INSERTION)
  updated_party_map <- dbGetQuery(con, "SELECT `Party ID`, `Visit ID` FROM `Party`")
  
  return(as.data.table(updated_party_map))  # Return mapping for merging with `Visit` table
}


#------------------------------------------------------------
# Insert Bills (Batch Insert)
#------------------------------------------------------------

insert_bills <- function(con, df, batch_size = 5000) {
  
  # Select relevant columns
  bills <- df[, .(FoodBill, TipAmount, DiscountApplied, AlcoholBill, PaymentMethod, VisitDate, Restaurant)]
  
  #Ensure VisitDate is stored as DATE
  bills[, VisitDate := as.Date(VisitDate)]
  
  #Fetch Restaurant Mapping
  restaurant_map <- dbGetQuery(con, "SELECT `Restaurant ID`, `Restaurant Name` FROM Restaurant")
  restaurant_map <- as.data.table(restaurant_map)
  
  #Merge Restaurant Name to get Restaurant ID
  bills <- merge(bills, restaurant_map, by.x = "Restaurant", by.y = "Restaurant Name", all.x = TRUE)
  
  #Map PaymentMethod to Payment ID
  payment_map <- dbGetQuery(con, "SELECT `Payment ID`, `Type` FROM `Payment Method`")
  bills <- merge(bills, payment_map, by.x = "PaymentMethod", by.y = "Type", all.x = TRUE)
  
  #Fetch Visit Map 
  visit_map <- dbGetQuery(con, "SELECT `Visit ID`, `Visit Date`, `Restaurant ID` FROM Visit")
  visit_map <- as.data.table(visit_map)
  visit_map[, `Visit Date` := as.Date(`Visit Date`)]
  visit_map <- unique(visit_map, by = c("Visit Date", "Restaurant ID"))
  
  #Merge with Visit ID
  bills <- merge(bills, visit_map, by.x = c("VisitDate", "Restaurant ID"), by.y = c("Visit Date", "Restaurant ID"), all.x = TRUE)
  
  #Filter out rows with missing Visit ID
  missing_visit_ids <- sum(is.na(bills$`Visit ID`))
  
  # Select only the required columns
  bills <- bills[, .(`Food Bill` = FoodBill, 
                     `Tip Amount` = TipAmount, 
                     `Discount Applied` = DiscountApplied, 
                     `Alcohol Bill` = AlcoholBill, 
                     `Payment ID`, 
                     `Visit ID`)]
  
  #Insert batch data
  batch_insert(con, "Bill", bills, 
               c("Food Bill", "Tip Amount", "Discount Applied", "Alcohol Bill", "Payment ID", "Visit ID"), 
               batch_size = 5000)
  
}


#------------------------------------------------------------
# Update Foreign Key References
#------------------------------------------------------------

update_visit_with_billID <- function(con) {
  query_update_bill_id <- "
    UPDATE Visit v
    JOIN Bill b ON v.`Visit ID` = b.`Visit ID`
    SET v.`Bill ID` = b.`Bill ID`
    WHERE v.`Bill ID` IS NULL;"
  dbExecute(con, query_update_bill_id)
}

#------------------------------------------------------------
# Main Execution Function
#------------------------------------------------------------

main <- function() {
  print("Connecting to the database...")
  con <- connect_db()
  print("Database connected successfully.")
  
  # Disable foreign key checks
  print("Disabling foreign key checks...")
  dbExecute(con, "SET FOREIGN_KEY_CHECKS = 0;")
  
  print("Inserting lookup data (Meal Type, Payment Method)...")
  insert_lookup_data(con)
  print("Lookup data inserted successfully.")
  
  print("Inserting restaurant data...")
  insert_restaurants(con, df)
  print("Restaurant data inserted successfully.")
  
  print("Inserting server data...")
  insert_servers(con, df)
  print("Server data inserted successfully.")
  
  print("Inserting customer data...")
  insert_customers(con, df)
  print("Customer data inserted successfully.")
  
  print("Inserting party data...")
  insert_parties(con, df)
  print("Party data inserted successfully.")
  
  print("Inserting visit data...")
  insert_visits(con, df)
  print("Visit data inserted successfully.")
  
  print("Inserting bill data...")
  insert_bills(con, df)
  print("Bill data inserted successfully.")
  
  print("Updating Visit table with Bill ID foreign key...")
  update_visit_with_billID(con)
  print("Visit table updated successfully.")
  
  #Enable foreign key checks
  print("Enabling foreign key checks...")
  dbExecute(con, "SET FOREIGN_KEY_CHECKS = 1;")
  
  print("Disconnecting from the database...")
  dbDisconnect(con)
  print("Database connection closed.")
  
  print("Database population complete.")
}

# Run main function
main()
