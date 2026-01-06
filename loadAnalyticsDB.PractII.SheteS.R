#============================================
#
# PROGRAM: loadAnalyticsDB.PractII.SheteS.R
# AUTHOR: Sukanya Sudhir Shete
# SEMESTER: Fall 2025
#
#============================================


# ***********************************
# Package installation and Loading
# ***********************************

# Install required packages
if (!require("RMySQL")) install.packages("RMySQL")
if (!require("DBI")) install.packages("DBI")

# Loads the packages
library(RMySQL)
library(DBI)


# ****************************
# Configuration and Constants
# ****************************

# File path to SQLite operational database containing raw data
SQLITE_DB_PATH <- "data/subscribersDB.sqlitedb"

# File path to CSV file containing streaming transaction records
CSV_FILE_PATH  <- "data/new-streaming-transactions-98732.csv"

# Cloud MySQL database credentials
db_user <- "xxxx" 
db_password <- "xxxx" 
db_host <- "xxxx" 
db_port <- 9999          
db_name <- "xxxxx"

# Batch size for processing large datasets in chunks 
BATCH_SIZE <- 50000

# Suppress non-critical warnings
options(warn = -1)


# **************************
# Database connection setup
# **************************

# Connect to MySQL database
conn_mysql <- tryCatch({
  dbConnect(
    MySQL(),
    user = db_user,
    password = db_password,
    host = db_host,
    port = db_port,
    dbname = db_name
  )
}, error = function(e) {
  message("Cloud database connection failed")
  stop(e)
})
message("Cloud database connection successful")

# Connect to SQLite operational database
conn_sqlite <- tryCatch({
  dbConnect(RSQLite::SQLite(), SQLITE_DB_PATH)
}, error = function(e) {
  message("SQLite db connection failed")
  stop(e)
})
message("SQLite db connected")


# ************************
# Load Dimension tables
# ************************

# Populating dim_country dimension table
message("\nPopulating dim_country...")
dbExecute(conn_mysql, "TRUNCATE TABLE dim_country")
countries <- dbGetQuery(conn_sqlite, "SELECT country_id, country FROM countries")

# Building bulk INSERT statement to load all countries at once
sql_countries <- paste0(
  "INSERT INTO dim_country VALUES ",
  paste0("(", countries$country_id, ",'", countries$country, "')", collapse=", ")
)
dbExecute(conn_mysql, sql_countries)
message(paste0("Countries loaded ", nrow(countries)))


# Populating dim_sport dimension table
message("Populatiing dim_sport...")
dbExecute(conn_mysql, "TRUNCATE TABLE dim_sport")
sports <- dbGetQuery(conn_sqlite, "SELECT DISTINCT sport FROM assets ORDER BY sport")

# Insert sports one at a time since sport_pk uses AUTO_INCREMENT
for(i in 1:nrow(sports)) {
  dbExecute(conn_mysql, 
            paste0("INSERT INTO dim_sport (sport_name) VALUES ('", sports$sport[i], "')"))
}
message(paste0("Sports loaded ", nrow(sports)))


# Populating dim_date dimension table
message("Populating dim_date...")
dbExecute(conn_mysql, "TRUNCATE TABLE dim_date")

# Generate date sequence from 2019-01-01 to 2025-12-31
dates <- seq(as.Date("2019-01-01"), as.Date("2025-12-31"), by="day")
# Divides year into 4 quarters (Q1-Q4)
quarters <- ceiling(as.numeric(format(dates, "%m")) / 3)
# Calculate week number within the year
weeks <- as.numeric(format(dates, "%W"))
# Full name of day (Monday, Tuesday, ..)
day_names <- format(dates, "%A")

# Inserting dates in batches
batch_size_dates <- 1000
for(i in seq(1, length(dates), batch_size_dates)) {
  end <- min(i + batch_size_dates - 1, length(dates))
  batch_dates <- dates[i:end]
  
  values_str <- paste0(
    "('", batch_dates, "',", format(batch_dates, "%Y%m%d"), ",", 
    format(batch_dates, "%Y"), ",'", format(batch_dates, "%B"), "',",
    quarters[i:end], ",", weeks[i:end], ",'", day_names[i:end], "')",
    collapse=", "
  )
  
  sql <- paste0("INSERT INTO dim_date (full_date, date_pk, year, month_name, quarter, week_of_year, day_of_week) VALUES ",
                values_str)
  dbExecute(conn_mysql, sql)
}
message(paste0("Dates loaded ", length(dates)))

# Get dimension keys for joins
sports_map <- dbGetQuery(conn_mysql, "SELECT sport_pk, sport_name FROM dim_sport")
countries_list <- dbGetQuery(conn_sqlite, "SELECT country_id, country FROM countries")

message(paste0("Dimension tables populated."))


# ***********************
# Load Transaction Data
# ***********************

# Create temporary staging table for transactions
dbExecute(conn_mysql, "DROP TABLE IF EXISTS temp_txns")
dbExecute(conn_mysql, "CREATE TABLE temp_txns (
  transaction_id VARCHAR(50), 
  user_id VARCHAR(50), 
  asset_id VARCHAR(50),
  streaming_date DATE, 
  minutes_streamed INT, 
  device_type VARCHAR(50),
  source_system VARCHAR(20),
  UNIQUE(transaction_id, asset_id, streaming_date),
  INDEX(user_id), INDEX(asset_id), INDEX(streaming_date)) ENGINE=InnoDB")

# Load SQLite transactions
message("\nLoading SQLite transactions...")
sqlite_txns <- dbGetQuery(conn_sqlite, "
  SELECT transaction_id, user_id, asset_id, streaming_date, minutes_streamed, device_type
  FROM streaming_txns WHERE user_id IS NOT NULL")

total_sqlite <- nrow(sqlite_txns)
message(paste0("Total SQLite rows: ", total_sqlite))

# Process SQLite transactions in batches
for(i in seq(1, total_sqlite, BATCH_SIZE)) {
  end <- min(i + BATCH_SIZE - 1, total_sqlite)
  batch <- sqlite_txns[i:end, ]
  
  values_str <- paste0(
    "('", batch$transaction_id, "','", batch$user_id, "','",
    batch$asset_id, "','", batch$streaming_date, "',",
    batch$minutes_streamed, ",'", batch$device_type, "','sqlite')",
    collapse=", "
  )
  sql <- paste0("INSERT IGNORE INTO temp_txns VALUES ", values_str)
  dbExecute(conn_mysql, sql)
  message(paste0("SQLite batch ", ceiling(end/BATCH_SIZE), ": rows ", format(i, scientific=FALSE), "-", format(end, scientific=FALSE)))
  # Clean up to prevent memory overflow
  rm(batch)
  gc()
}

# Load transactions from CSV source file
message("\nLoading CSV transactions...")
csv_txns <- read.csv(CSV_FILE_PATH, header=TRUE, stringsAsFactors=FALSE)
csv_txns$streaming_date <- as.Date(csv_txns$streaming_date)
csv_txns$minutes_streamed <- as.integer(csv_txns$minutes_streamed)

total_csv <- nrow(csv_txns)
message(paste0("Total CSV rows: ", total_csv))

# Process CSV transactions in batches
for(i in seq(1, total_csv, BATCH_SIZE)) {
  end <- min(i + BATCH_SIZE - 1, total_csv)
  batch <- csv_txns[i:end, ]
  
  values_str <- paste0(
    "('", batch$transaction_id, "','", batch$user_id, "','",
    batch$asset_id, "','", batch$streaming_date, "',",
    batch$minutes_streamed, ",'", batch$device_type, "','csv')",
    collapse=", "
  )
  sql <- paste0("INSERT IGNORE INTO temp_txns VALUES ", values_str)
  dbExecute(conn_mysql, sql)
  message(paste0("CSV batch: rows ", i, "-", end))
  
  # Clean up to prevent memory overflow
  rm(batch)
  gc()
}

# Verify total transactions loaded from both sources
temp_count <- dbGetQuery(conn_mysql, "SELECT COUNT(*) as cnt FROM temp_txns")$cnt
message(paste0("Total transactions loaded: ", temp_count))


# ***************************************
# Create dimensional mappings
# ***************************************

# Create user-to-country mapping to compute facts
message("\nCreating user-to-country mapping...")
dbExecute(conn_mysql, "DROP TABLE IF EXISTS user_country_map")
dbExecute(conn_mysql, "CREATE TABLE user_country_map (
  user_id VARCHAR(50) PRIMARY KEY,
  country_id INT,
  INDEX(country_id)) ENGINE=InnoDB")

user_country_data <- dbGetQuery(conn_sqlite, "
  SELECT DISTINCT s.user_id, MIN(c.country_id) as country_id
  FROM subscribers s
  JOIN postal2city p2c ON s.postal_code = p2c.postal_code
  JOIN cities c ON p2c.city_id = c.city_id
  GROUP BY s.user_id
")
# Load user-country mappings in batches
for(i in seq(1, nrow(user_country_data), BATCH_SIZE)) {
  end <- min(i + BATCH_SIZE - 1, nrow(user_country_data))
  batch <- user_country_data[i:end, ]
  
  values_str <- paste0(
    "('", batch$user_id, "',", batch$country_id, ")",
    collapse=", "
  )
  
  sql <- paste0("INSERT INTO user_country_map VALUES ", values_str)
  dbExecute(conn_mysql, sql)
}
message(paste0("User mappings ", nrow(user_country_data)))


# Create asset-to-sport mapping to compute facts
message("Creating asset-to-sport mapping...")
dbExecute(conn_mysql, "DROP TABLE IF EXISTS asset_sport_map")
dbExecute(conn_mysql, "CREATE TABLE asset_sport_map (
  asset_id VARCHAR(50) PRIMARY KEY,
  sport_name VARCHAR(50),
  INDEX(sport_name)) ENGINE=InnoDB")

asset_sport_data <- dbGetQuery(conn_sqlite, "
  SELECT DISTINCT asset_id, sport FROM assets
")
# Load asset-sport mappings in batches
for(i in seq(1, nrow(asset_sport_data), BATCH_SIZE)) {
  end <- min(i + BATCH_SIZE - 1, nrow(asset_sport_data))
  batch <- asset_sport_data[i:end, ]
  
  values_str <- paste0(
    "('", batch$asset_id, "','", batch$sport, "')",
    collapse=", "
  )

  sql <- paste0("INSERT INTO asset_sport_map VALUES ", values_str)
  dbExecute(conn_mysql, sql)
}
message(paste0("Asset mappings: ", nrow(asset_sport_data)))


# *******************
# Load Fact Table
# *******************

message("\nLoading Fact table...")
# Aggregate transactions and load to fact table
dbExecute(conn_mysql, "TRUNCATE TABLE fact_streaming_transactions")
sql_fact <- "
  INSERT INTO fact_streaming_transactions 
  (fact_pk, streaming_year, date_pk, country_pk, sport_pk, streaming_time_sec, streaming_event_count, device_type)
  SELECT
    ROW_NUMBER() OVER (ORDER BY dd.date_pk, ucm.country_id, ds.sport_pk) as fact_pk,
    dd.year as streaming_year,
    dd.date_pk,
    ucm.country_id as country_pk,
    COALESCE(ds.sport_pk, 0) as sport_pk,
    SUM(t.minutes_streamed * 60) as streaming_time_sec,
    COUNT(t.transaction_id) as streaming_event_count,
    t.device_type
  FROM temp_txns t
  JOIN dim_date dd ON t.streaming_date = dd.full_date
  LEFT JOIN user_country_map ucm ON t.user_id = ucm.user_id
  LEFT JOIN asset_sport_map asm ON t.asset_id = asm.asset_id
  LEFT JOIN dim_sport ds ON asm.sport_name = ds.sport_name
  GROUP BY dd.date_pk, ucm.country_id, ds.sport_pk, t.device_type
"
dbExecute(conn_mysql, sql_fact)

# Verify fact table load
fact_count <- dbGetQuery(conn_mysql, "SELECT COUNT(*) as cnt FROM fact_streaming_transactions")$cnt
message(paste0("Populated ", fact_count, " rows"))


# *****************************
# Fact Table validation tests
# *****************************

message("\nFact Table validation tests")

# TEST 1: Referential Integrity - No Invalid Foreign Keys
# Ensure all dimension keys in fact table reference valid dimensions
message("\nTEST 1: Referential Integrity")
invalid_refs <- dbGetQuery(conn_mysql, "
  SELECT
    SUM(CASE WHEN country_pk NOT IN (SELECT country_pk FROM dim_country) THEN 1 ELSE 0 END) as invalid_countries,
    SUM(CASE WHEN date_pk NOT IN (SELECT date_pk FROM dim_date) THEN 1 ELSE 0 END) as invalid_dates,
    SUM(CASE WHEN sport_pk NOT IN (SELECT sport_pk FROM dim_sport) AND sport_pk != 0 THEN 1 ELSE 0 END) as invalid_sports
  FROM fact_streaming_transactions
")
total_invalid <- sum(invalid_refs)
pass_refs <- total_invalid == 0

if(pass_refs){
  message("PASS: All foreign keys are valid")
} else {
  message(paste0("FAIL: Invalid foreign keys found - ", total_invalid))
}

# TEST 2: No NULL Values in Critical Dimensions
# Ensure all critical dimension keys are populated
message("TEST 2: NULL Values Check")
null_check <- dbGetQuery(conn_mysql, "
  SELECT
    SUM(CASE WHEN date_pk IS NULL THEN 1 ELSE 0 END) as null_dates,
    SUM(CASE WHEN country_pk IS NULL THEN 1 ELSE 0 END) as null_countries,
    SUM(CASE WHEN streaming_time_sec IS NULL THEN 1 ELSE 0 END) as null_time,
    SUM(CASE WHEN streaming_event_count IS NULL THEN 1 ELSE 0 END) as null_events
  FROM fact_streaming_transactions
")
total_nulls <- sum(null_check)
pass_nulls <- total_nulls == 0
if(pass_nulls){
  message("PASS: No NULL values in critical dimensions")
} else {
  message(paste0("FAIL: NULL values found - ", total_nulls))
}

# TEST 3: Measure Validity - No Negative or Zero Anomalies
# Ensure streaming time and event counts are logically valid
message("TEST 3: Measure Validity")

measure_check <- dbGetQuery(conn_mysql, "
  SELECT
    SUM(CASE WHEN streaming_time_sec <= 0 THEN 1 ELSE 0 END) as invalid_time,
    SUM(CASE WHEN streaming_event_count <= 0 THEN 1 ELSE 0 END) as invalid_events
  FROM fact_streaming_transactions
")

total_invalid_measures <- sum(measure_check)
pass_measures <- total_invalid_measures == 0

if(pass_measures){
  message("PASS: All measures are positive and valid")
} else {
  message(paste0("FAIL: Invalid measure values found - ", total_invalid_measures))
}

# TEST 4: Measure Relationship Validation
# Verify streaming_time_sec and streaming_event_count have logical relationship
message("TEST 4: Measure Relationship Validation")

measure_relationship <- dbGetQuery(conn_mysql, "
  SELECT
    AVG(streaming_time_sec / streaming_event_count) as avg_sec_per_event,
    MIN(streaming_time_sec / streaming_event_count) as min_sec_per_event,
    MAX(streaming_time_sec / streaming_event_count) as max_sec_per_event
  FROM fact_streaming_transactions
  WHERE streaming_event_count > 0
")
avg_sec <- measure_relationship$avg_sec_per_event
pass_relationship <- avg_sec >= 1000 && avg_sec <= 10000
if(pass_relationship) {
  message(paste0("PASS: Measure relationship valid (avg ", round(avg_sec, 0), " sec/event)"))
} else {
  message(paste0("FAIL: Measure relationship invalid"))
}

# TEST 5: Dimensional Aggregation Balance
# Verify data distribution across countries is reasonable
message("TEST 5: Dimensional Aggregation Balance")

country_balance <- dbGetQuery(conn_mysql, "
  SELECT
    MIN(total_events) as min_country_events,
    MAX(total_events) as max_country_events
  FROM (
    SELECT SUM(streaming_event_count) as total_events
    FROM fact_streaming_transactions
    GROUP BY country_pk
  ) as country_totals
")

min_events <- country_balance$min_country_events
max_events <- country_balance$max_country_events
balance_ratio <- max_events / min_events
pass_balance <- balance_ratio <= 50

if(pass_balance) {
  message(paste0("PASS: Dimensional distribution acceptable (ratio ", round(balance_ratio, 2), "x)"))
} else {
  message(paste0("FAIL: Extreme dimensional imbalance seen"))
}

# Test Summary
test_results <- c(pass_refs, pass_nulls, pass_measures, pass_relationship, pass_balance)
passed <- sum(test_results)
total <- length(test_results)

message(paste0("Tests Passed: ", passed, "/", total))


# *********************************
# Cleanup and Closing connections
# *********************************

# Cleanup temporary tables
dbExecute(conn_mysql, "DROP TABLE IF EXISTS temp_txns")
dbExecute(conn_mysql, "DROP TABLE IF EXISTS user_country_map")
dbExecute(conn_mysql, "DROP TABLE IF EXISTS asset_sport_map")

# Database disconnection
dbDisconnect(conn_mysql)
dbDisconnect(conn_sqlite)
message("\nDatabase connection closed")
