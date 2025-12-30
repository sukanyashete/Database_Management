# **********************************************
# Program: loadDB.PractI.SheteS.R
# Author: Sukanya Sudhir Shete
# Course: CS5200 - Database Management Systems
# Semester: Fall 2025
# *********************************************


# Install and load required packages
if (!require("RMySQL")) install.packages("RMySQL")
if (!require("DBI")) install.packages("DBI")
if (!require("RSQLite")) install.packages("RSQLite")

library(RMySQL)
library(DBI)
library(RSQLite)

# Aiven/MySQL cloud credentials
db_user <- "xxxxx"       
db_password <- "xxxxxxx"  
db_host <- "xxxxx"  
db_port <- 99999
db_name <- "defaultdb"

# Establish database connections
csv_file <- "restaurant-visits-179719.csv"
sqlite_file <- "restaurants-db.sqlitedb"

conn_mysql <- dbConnect(MySQL(), user = db_user, password = db_password, 
                        host = db_host, port = db_port, dbname = db_name)
conn_sqlite <- dbConnect(SQLite(), sqlite_file)

# Load restaurant visits data from CSV
df.orig <- read.csv(csv_file, stringsAsFactors = FALSE)
cat(sprintf("Loaded %d rows from CSV file.\n", nrow(df.orig)))

# Load restaurant information from SQLite database
restaurants_sqlite <- dbGetQuery(conn_sqlite, "SELECT * FROM restaurants")
names(restaurants_sqlite)[names(restaurants_sqlite) == "rname"] <- "rest_name"
names(restaurants_sqlite)[names(restaurants_sqlite) == "hasService"] <- "has_service"
restaurants_sqlite$has_service <- tolower(restaurants_sqlite$has_service) == "yes"

# Remove invalid date values like "0000-00-00" or empty strings
cleanDate <- function(date_str) {
  if (is.na(date_str) || date_str == "" || date_str == "0000-00-00") return(NA)
  return(date_str)
}

# Convert dates from various formats to MySQL format (YYYY-MM-DD)
convertDate <- function(date_str) {
  cleaned <- cleanDate(date_str)
  if (is.na(cleaned)) return(NA)
  tryCatch({
    if (grepl("/", cleaned)) {
      date_obj <- as.Date(cleaned, format = "%m/%d/%Y")
      if (is.na(date_obj)) date_obj <- as.Date(cleaned, format = "%d/%m/%Y")
    } else if (grepl("-", cleaned)) {
      date_obj <- as.Date(cleaned, format = "%Y-%m-%d")
    } else {
      return(NA)
    }
    if (!is.na(date_obj)) return(format(date_obj, "%Y-%m-%d")) else return(NA)
  }, error = function(e) { return(NA) })
}

# Handle party size sentinel value (99 means unknown)
cleanPartySize <- function(size) {
  if (is.na(size) || size == 99) return(NA)
  return(size)
}

# Ensure wait time is non-negative (negative values become 0)
cleanWaitTime <- function(wait) {
  if (is.na(wait) || wait < 0) return(0)
  return(wait)
}

# Remove empty strings and "N/A" values
cleanString <- function(str) {
  if (is.na(str) || str == "" || str == "N/A") return(NA)
  return(str)
}

# Convert gender string (e.g., "mfmf") to category (all_male, all_female, mixed, unknown)
categorizeGender <- function(gender_str) {
  if (is.na(gender_str) || gender_str == "") return("unknown")
  known_genders <- strsplit(gender_str, "")[[1]]
  known_genders <- known_genders[known_genders %in% c('f', 'm')]
  if (length(known_genders) == 0) return("unknown")
  unique_known <- unique(known_genders)
  if (length(unique_known) == 1) {
    if (unique_known == 'f') return("all_female")
    else if (unique_known == 'm') return("all_male")
  }
  return("mixed")
}

# Escape single quotes for SQL (prevents SQL injection and syntax errors)
escapeSingleQuotes <- function(str) {
  if (is.na(str)) return("NULL")
  str <- gsub("'", "''", str)
  return(sprintf("'%s'", str))
}

# Optimize database performance for bulk loading
dbExecute(conn_mysql, "SET FOREIGN_KEY_CHECKS=0")
dbExecute(conn_mysql, "SET AUTOCOMMIT=0")

# Populate RESTAURANT table with data from SQLite
cat("\nPopulating RESTAURANT Table\n")
unique_restaurants <- unique(df.orig$Restaurant)
restaurant_map <- list()

rest_values <- c()
for (rest_name in unique_restaurants) {
  # Try to match restaurant name from CSV with SQLite data
  sqlite_rest <- restaurants_sqlite[tolower(trimws(restaurants_sqlite$rest_name)) == 
                                      tolower(trimws(rest_name)), ]
  if (nrow(sqlite_rest) == 0) {
    sqlite_rest <- restaurants_sqlite[grepl(rest_name, restaurants_sqlite$rest_name, ignore.case = TRUE), ]
  }
  
  if (nrow(sqlite_rest) > 0) {
    city <- sqlite_rest$city[1]
    state <- sqlite_rest$state[1]
    has_service <- sqlite_rest$has_service[1]
  } else {
    city <- "Unknown"
    state <- "XX"
    has_service <- TRUE
  }
  
  rest_values <- c(rest_values, sprintf("(%s, %s, %s, %d)",
                                        escapeSingleQuotes(rest_name), escapeSingleQuotes(city), 
                                        escapeSingleQuotes(state), as.integer(has_service)))
}

if (length(rest_values) > 0) {
  insert_sql <- paste0("INSERT INTO restaurant (rest_name, city, state, has_service) VALUES ",
                       paste(rest_values, collapse = ", "))
  dbExecute(conn_mysql, insert_sql)
  dbExecute(conn_mysql, "COMMIT")
}

# Create mapping of restaurant names to their database IDs
rest_ids <- dbGetQuery(conn_mysql, "SELECT rest_id, rest_name FROM restaurant")
for (i in 1:nrow(rest_ids)) {
  restaurant_map[[rest_ids$rest_name[i]]] <- rest_ids$rest_id[i]
}

# Populate SERVER table (excluding "N/A" servers)
cat("Populating SERVER table\n")
df_servers <- df.orig[df.orig$ServerName != "N/A" & df.orig$ServerName != "", ]
unique_servers <- unique(df_servers[, c("ServerEmpID", "ServerName", "ServerBirthDate", 
                                        "StartDateHired", "EndDateHired", 
                                        "HourlyRate", "ServerTIN")])
server_map <- list()

if (nrow(unique_servers) > 0) {
  server_values <- c()
  for (i in 1:nrow(unique_servers)) {
    tin <- cleanString(unique_servers$ServerTIN[i])
    birth_date <- convertDate(unique_servers$ServerBirthDate[i])
    work_start <- convertDate(unique_servers$StartDateHired[i])
    work_end <- convertDate(unique_servers$EndDateHired[i])
    
    server_values <- c(server_values, sprintf("(%s, %s, %s, %s, %s, %s)",
                                              escapeSingleQuotes(unique_servers$ServerName[i]),
                                              if (is.na(tin)) "NULL" else escapeSingleQuotes(tin),
                                              if (is.na(birth_date)) "NULL" else escapeSingleQuotes(birth_date),
                                              if (is.na(work_start)) "NULL" else escapeSingleQuotes(work_start),
                                              if (is.na(work_end)) "NULL" else escapeSingleQuotes(work_end),
                                              unique_servers$HourlyRate[i]))
  }
  
  if (length(server_values) > 0) {
    insert_sql <- paste0("INSERT INTO server (server_name, tin, birth_date, work_start, work_end, hourly_rate) VALUES ",
                         paste(server_values, collapse = ", "))
    dbExecute(conn_mysql, insert_sql)
    dbExecute(conn_mysql, "COMMIT")
  }
  
  # Create mapping of employee IDs to server database IDs
  server_ids <- dbGetQuery(conn_mysql, "SELECT server_id, server_name FROM server")
  server_lookup <- data.frame(
    emp_id = unique_servers$ServerEmpID,
    server_name = unique_servers$ServerName,
    stringsAsFactors = FALSE
  )
  
  for (i in 1:nrow(server_lookup)) {
    matching <- server_ids[server_ids$server_name == server_lookup$server_name[i], ]
    if (nrow(matching) > 0) {
      server_map[[server_lookup$emp_id[i]]] <- matching$server_id[1]
    }
  }
}

# Populate RESTAURANT_SERVER junction table (which server works at which restaurant)
cat("Populating RESTAURANT_SERVER table\n")
server_restaurant_pairs <- unique(df_servers[, c("Restaurant", "ServerEmpID")])
rs_values <- c()

for (i in 1:nrow(server_restaurant_pairs)) {
  rest_name <- server_restaurant_pairs$Restaurant[i]
  emp_id <- server_restaurant_pairs$ServerEmpID[i]
  
  if (!is.null(restaurant_map[[rest_name]]) && !is.null(server_map[[emp_id]])) {
    rs_values <- c(rs_values, sprintf("(%d, %d)", 
                                      restaurant_map[[rest_name]], 
                                      server_map[[emp_id]]))
  }
}

if (length(rs_values) > 0) {
  insert_sql <- paste0("INSERT IGNORE INTO restaurant_server (rest_id, server_id) VALUES ",
                       paste(rs_values, collapse = ", "))
  dbExecute(conn_mysql, insert_sql)
  dbExecute(conn_mysql, "COMMIT")
}

# Populate CUSTOMER table (only for customers with identifying information)
cat("Populating CUSTOMER Table\n")
df_customers <- df.orig[df.orig$CustomerName != "" | 
                          df.orig$CustomerPhone != "" | 
                          df.orig$CustomerEmail != "", ]
customer_map <- list()

if (nrow(df_customers) > 0) {
  unique_customers <- unique(df_customers[, c("CustomerName", "CustomerPhone", 
                                              "CustomerEmail", "LoyaltyMember")])
  cust_values <- c()
  
  for (i in 1:nrow(unique_customers)) {
    cust_name <- cleanString(unique_customers$CustomerName[i])
    cust_phone <- cleanString(unique_customers$CustomerPhone[i])
    cust_email <- cleanString(unique_customers$CustomerEmail[i])
    
    cust_values <- c(cust_values, sprintf("(%s, %s, %s, %d)",
                                          if (is.na(cust_name)) "NULL" else escapeSingleQuotes(cust_name),
                                          if (is.na(cust_phone)) "NULL" else escapeSingleQuotes(cust_phone),
                                          if (is.na(cust_email)) "NULL" else escapeSingleQuotes(cust_email),
                                          as.integer(unique_customers$LoyaltyMember[i])))
  }
  
  if (length(cust_values) > 0) {
    insert_sql <- paste0("INSERT INTO customer (cust_name, cust_phone, cust_email, loyalty_member) VALUES ",
                         paste(cust_values, collapse = ", "))
    dbExecute(conn_mysql, insert_sql)
    dbExecute(conn_mysql, "COMMIT")
  }
  
  # Create mapping of customer info to their database IDs
  cust_ids <- dbGetQuery(conn_mysql, "SELECT cust_id, cust_name, cust_phone, cust_email FROM customer")
  for (i in 1:nrow(cust_ids)) {
    cust_key <- paste(cust_ids$cust_name[i], cust_ids$cust_phone[i], 
                      cust_ids$cust_email[i], sep = "|")
    customer_map[[cust_key]] <- cust_ids$cust_id[i]
  }
}

# Populate VISIT, ORDER, and PAYMENT tables in batches for efficiency
cat("Populating VISIT, ORDER, and PAYMENT Tables\n")
meal_type_map <- list("Breakfast" = 1, "Lunch" = 2, "Dinner" = 3, "Take-Out" = 4)
payment_method_map <- list("Mobile Payment" = 1, "Cash" = 2, "Credit Card" = 3)

batch_size <- 1500
num_batches <- ceiling(nrow(df.orig) / batch_size)

for (batch_num in 1:num_batches) {
  start_idx <- (batch_num - 1) * batch_size + 1
  end_idx <- min(batch_num * batch_size, nrow(df.orig))
  
  cat(sprintf("Processing batch %d/%d (rows %d-%d)...\n", 
              batch_num, num_batches, start_idx, end_idx))
  
  # Build bulk INSERT for VISIT table
  visit_values <- c()
  
  for (i in start_idx:end_idx) {
    row <- df.orig[i, ]
    rest_id <- restaurant_map[[row$Restaurant]]
    if (is.null(rest_id)) next
    
    server_id <- if (!is.null(server_map[[row$ServerEmpID]])) server_map[[row$ServerEmpID]] else NULL
    
    cust_key <- paste(cleanString(row$CustomerName), cleanString(row$CustomerPhone),
                      cleanString(row$CustomerEmail), sep = "|")
    cust_id <- if (!is.null(customer_map[[cust_key]])) customer_map[[cust_key]] else NULL
    
    visit_date <- convertDate(row$VisitDate)
    visit_time <- row$VisitTime
    wait_time <- cleanWaitTime(row$WaitTime)
    party_size <- cleanPartySize(row$PartySize)
    if (is.na(party_size)) party_size <- 1
    gender <- categorizeGender(row$Genders)
    
    visit_values <- c(visit_values, sprintf("(%d, %s, %s, %s, %s, %d, %d, %s)",
                                            rest_id,
                                            if (is.null(server_id)) "NULL" else server_id,
                                            if (is.null(cust_id)) "NULL" else cust_id,
                                            if (is.na(visit_date)) "NULL" else escapeSingleQuotes(visit_date),
                                            if (is.na(visit_time) || visit_time == "") "NULL" else escapeSingleQuotes(visit_time),
                                            wait_time, party_size, escapeSingleQuotes(gender)))
  }
  
  # Insert all visits in this batch
  if (length(visit_values) > 0) {
    insert_sql <- paste0("INSERT INTO visit (rest_id, server_id, cust_id, visit_date, visit_time, wait_time, party_size, gender) VALUES ",
                         paste(visit_values, collapse = ", "))
    dbExecute(conn_mysql, insert_sql)
    dbExecute(conn_mysql, "COMMIT")
  }
  
  # Get the visit IDs that were just inserted
  visit_ids <- dbGetQuery(conn_mysql, 
                          sprintf("SELECT visit_id FROM visit ORDER BY visit_id DESC LIMIT %d", 
                                  length(visit_values)))$visit_id
  visit_ids <- rev(visit_ids)
  
  # Build bulk INSERT for ORDER table
  order_values <- c()
  
  for (j in 1:length(visit_ids)) {
    i <- start_idx + j - 1
    if (i > end_idx) break
    
    row <- df.orig[i, ]
    visit_id <- visit_ids[j]
    
    meal_type_id <- meal_type_map[[row$MealType]]
    if (is.null(meal_type_id)) meal_type_id <- 4
    
    ordered_alcohol <- as.integer(tolower(row$orderedAlcohol) == "yes")
    food_bill <- as.numeric(row$FoodBill)
    alcohol_bill <- as.numeric(row$AlcoholBill)
    
    order_values <- c(order_values, sprintf("(%d, %d, %d, %.2f, %.2f)",
                                            visit_id, meal_type_id, ordered_alcohol, food_bill, alcohol_bill))
  }
  
  # Insert all orders in this batch
  if (length(order_values) > 0) {
    insert_sql <- paste0("INSERT INTO `order` (visit_id, meal_type, ordered_alcohol, food_bill, alcohol_bill) VALUES ",
                         paste(order_values, collapse = ", "))
    dbExecute(conn_mysql, insert_sql)
    dbExecute(conn_mysql, "COMMIT")
  }
  
  # Get the order IDs that were just inserted
  order_ids <- dbGetQuery(conn_mysql, 
                          sprintf("SELECT order_id FROM `order` ORDER BY order_id DESC LIMIT %d", 
                                  length(order_values)))$order_id
  order_ids <- rev(order_ids)
  
  # Build bulk INSERT for PAYMENT table
  payment_values <- c()
  
  for (j in 1:length(order_ids)) {
    i <- start_idx + j - 1
    if (i > end_idx) break
    
    row <- df.orig[i, ]
    order_id <- order_ids[j]
    
    method_id <- payment_method_map[[row$PaymentMethod]]
    if (is.null(method_id)) method_id <- 2
    
    tip <- as.numeric(row$TipAmount)
    discount <- as.numeric(row$DiscountApplied)
    
    payment_values <- c(payment_values, sprintf("(%d, %d, %.2f, %.2f)",
                                                order_id, method_id, tip, discount))
  }
  
  # Insert all payments in this batch
  if (length(payment_values) > 0) {
    insert_sql <- paste0("INSERT INTO payment (order_id, method_id, tip, discount) VALUES ",
                         paste(payment_values, collapse = ", "))
    dbExecute(conn_mysql, insert_sql)
    dbExecute(conn_mysql, "COMMIT")
  }
}

# Restore database settings to normal
dbExecute(conn_mysql, "SET FOREIGN_KEY_CHECKS=1")
dbExecute(conn_mysql, "SET AUTOCOMMIT=1")

# Display summary of inserted rows
cat("\n")
tables <- c("restaurant", "server", "restaurant_server", "customer", 
            "visit", "`order`", "payment")
for (table in tables) {
  count <- dbGetQuery(conn_mysql, sprintf("SELECT COUNT(*) as cnt FROM %s", table))$cnt
  cat(sprintf("Inserted %d rows in %s\n", count, table))
}

# Disconnect from both databases
dbDisconnect(conn_mysql)
dbDisconnect(conn_sqlite)