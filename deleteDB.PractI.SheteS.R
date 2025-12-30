# *********************************************
# Program: deleteDB.PractI.SheteS.R
# Author: Sukanya Sudhir Shete
# Course: CS5200 - Database Management Systems
# Semester: Fall 2025
# **********************************************


# Install and load required packages
if (!require("RMySQL")) install.packages("RMySQL")
if (!require("DBI")) install.packages("DBI")

library(RMySQL)
library(DBI)

# Aiven/MySQL cloud credentials
db_user <- "xxxxx"       
db_password <- "xxxxx"  
db_host <- "xxxxxx"  
db_port <- 99999
db_name <- "defaultdb"

# Connect to MySQL database
conn <- dbConnect(
  MySQL(),
  user = db_user,
  password = db_password,
  host = db_host,
  port = db_port,
  dbname = db_name
)

# Drop a table only if it exists
# Parameters: 
#   - conn: database connection object
#   - table_name: name of the table to drop
dropTableIfExists <- function(conn, table_name) {
  tryCatch({
    tables <- dbListTables(conn)
    if (table_name %in% tables) {
      # Special handling since order is also a keyword
      if (table_name == "order") {
        drop_sql <- "DROP TABLE IF EXISTS `order`"
      } else {
        drop_sql <- sprintf("DROP TABLE IF EXISTS %s", table_name)
      }
      dbExecute(conn, drop_sql)
      cat(sprintf("Table '%s' dropped successfuly\n", table_name))
      return(TRUE)
    } else {
      cat(sprintf("Table '%s' does not exist\n", table_name))
      return(FALSE)
    }
  }, error = function(e) {
    cat(sprintf("Error dropping table '%s': %s\n", table_name, e$message))
    return(FALSE)
  })
}

# List of tables to drop
tables_to_drop <- c(
  "payment",            
  "payment_type",       
  "order",                
  "meal",                 
  "visit",                
  "customer",             
  "restaurant_server",    
  "server",               
  "restaurant"  
)

# Display Current Tables Before Deletion
existing_tables <- dbListTables(conn)

if (length(existing_tables) > 0) {
  
  # Track deletions count
  deleted_count <- 0
  failed_count <- 0
  
  cat("Tables in database before deletion:\n")
  for (table in existing_tables) {
    cat(sprintf("  - %s\n", table))
  }
  cat(sprintf("No.of tables: %d\n\n", length(existing_tables)))

  # Disable foreign key checks temporarily
  tryCatch({
    dbExecute(conn, "SET FOREIGN_KEY_CHECKS = 0")
  }, error = function(e) {
    cat(sprintf("Could not disable foreign key checks: %s\n\n", e$message))
  })

  # Drop each table
  for (table in tables_to_drop) {
    if (dropTableIfExists(conn, table)) {
       deleted_count <- deleted_count + 1
    } else {
      if (table %in% existing_tables) {
        failed_count <- failed_count + 1
      }
    }
  }

  # Reenable foreign key checks
  tryCatch({
    dbExecute(conn, "SET FOREIGN_KEY_CHECKS = 1")
  }, error = function(e) {
    cat(sprintf("Could not re-enable foreign key checks: %s\n", e$message))
  })

  # Check if all tables are dropped
  remaining_tables <- dbListTables(conn)
  if (length(remaining_tables) == 0) {
    cat("All tables dropped successfully\n")
  } else {
    cat("Not all tables dropped. Remaining:\n")
    for (table in remaining_tables) {
      cat(sprintf("  - %s\n", table))
    }
  }

  # Deletion summary
  if (failed_count > 0){
    cat(sprintf("Failed to drop %d tables\n Successfully dropped %d tables\n", failed_count, deleted_count))
  }
} else {
  cat("No tables found in database to delete.\n")
}

# Disconnect database
dbDisconnect(conn)