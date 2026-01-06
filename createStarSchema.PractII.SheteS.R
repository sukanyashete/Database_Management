#=============================================
#
# PROGRAM: createStarSchema.PractII.SheteS.R
# AUTHOR: Sukanya Sudhir Shete
# SEMESTER: Fall 2025
#
#=============================================


# *********************************
# Package installation and Loading
# *********************************

# Install required packages
if (!require("RMySQL")) install.packages("RMySQL")
if (!require("DBI")) install.packages("DBI")

# Loads the packages
library(RMySQL)
library(DBI)


# ***************************************
# Establish connection to cloud database
# ***************************************

# Cloud MySQL database credentials
db_user <- "xxxx" 
db_password <- "xxxx" 
db_host <- "xxxx" 
db_port <- 9999           
db_name <- "xxxx"

# Connect to MySQL database
conn <- tryCatch({
  dbConnect(
  MySQL(),
  user = db_user,
  password = db_password,
  host = db_host,
  port = db_port,
  dbname = db_name
  )
}, error = function(e) {
  message("Database connection failed")
  stop(e)
})
message("Database connection successfull.")


# *****************
# Helper functions
# *****************

# Helper function to safely drop and create tables
# Parameters:
#   conn: Database connection object
#   table_name: Name of the table to drop and create
#   create_sql: SQL CREATE TABLE statement
exec_drop_create <- function(conn, table_name, create_sql) {
  tryCatch({
    # drop table if it exists
    dbExecute(conn, paste("DROP TABLE IF EXISTS", table_name))
    
    # create a new table
    dbExecute(conn, create_sql)
    message(paste("Table", table_name, "created successfully."))

      }, error = function(e) {
    message(paste("Error creating table", table_name, ":", e$message))
    stop(e)
  })
}


# *************************
# Dimension Table creation
# *************************

# Creating dimension table: dim_country
sql_country <- "
    CREATE TABLE dim_country (
        country_pk INT NOT NULL PRIMARY KEY,
        country_name VARCHAR(255) NOT NULL,
        INDEX idx_country_name (country_name)
    ) ENGINE=InnoDB;
"
exec_drop_create(conn, "dim_country", sql_country)

# Creating dimension table: dim_sport
sql_sport <- "
    CREATE TABLE dim_sport (
        sport_pk INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
        sport_name VARCHAR(255) NOT NULL UNIQUE
    ) ENGINE=InnoDB;
"
exec_drop_create(conn, "dim_sport", sql_sport)

# Creating dimension table: dim_date
sql_date <- "
    CREATE TABLE dim_date (
        date_pk INT NOT NULL PRIMARY KEY, 
        full_date DATE NOT NULL UNIQUE,
        day_of_week VARCHAR(10),
        week_of_year INT,
        month_name VARCHAR(10),
        quarter INT,
        year INT,
        
        INDEX idx_date_year (year),
        INDEX idx_date_year_month (year, month_name)
    ) ENGINE=InnoDB;
"
exec_drop_create(conn, "dim_date", sql_date)


# ********************
# Fact Table creation
# ********************

# Fact Table creation with indexing and partitioning
sql_fact <- "
    CREATE TABLE fact_streaming_transactions (
        fact_pk BIGINT NOT NULL, 
        streaming_year INT NOT NULL, 
        
        date_pk INT NOT NULL,
        country_pk INT NOT NULL,
        sport_pk INT NOT NULL,
      
        streaming_time_sec INT NOT NULL,
        streaming_event_count INT NOT NULL DEFAULT 1,
        device_type VARCHAR(50), 
        
        PRIMARY KEY (fact_pk, streaming_year), 
        INDEX idx_fact_date (date_pk),
        INDEX idx_fact_country (country_pk),
        INDEX idx_fact_sport (sport_pk)

    ) ENGINE=InnoDB

    PARTITION BY RANGE (streaming_year) (
        PARTITION p2021 VALUES LESS THAN (2022),
        PARTITION p2022 VALUES LESS THAN (2023),
        PARTITION p2023 VALUES LESS THAN (2024),
        PARTITION p2024 VALUES LESS THAN (2025),
        PARTITION p2025 VALUES LESS THAN (2026),
        PARTITION pFuture VALUES LESS THAN MAXVALUE
    );
"
exec_drop_create(conn, "fact_streaming_transactions", sql_fact)


# ****************************
# Closing database connection
# ****************************

dbDisconnect(conn)
message("Database connection closed.")
