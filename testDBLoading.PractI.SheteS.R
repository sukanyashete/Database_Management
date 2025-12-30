# *********************************************
# Program: testDBLoading.PractI.SheteS.R
# Author: Sukanya Sudhir Shete
# Course: CS5200 - Database Management Systems
# Semester: Fall 2025
# *********************************************

# Install and load required packages
if (!require("RMySQL")) install.packages("RMySQL")
if (!require("DBI")) install.packages("DBI")

library(RMySQL)
library(DBI)

# Aiven/MySQL cloud credentials
db_user <- "xxxx"       
db_password <- "xxxx"   
db_host <- "xxxx"  
db_port <- 99999  
db_name <- "defaultdb"

# Database used
csv_file <- "restaurant-visits-179719.csv"
  
# Connect to MySQL
conn <- dbConnect(
  MySQL(),
  user = db_user,
  password = db_password,
  host = db_host,
  port = db_port,
  dbname = db_name
)

# Load CSV file
df.orig <- read.csv(csv_file, stringsAsFactors = FALSE)
cat(sprintf("Loaded CSV file with %d rows.\n\n", nrow(df.orig)))

# Test 1: Count Unique Restaurants
cat("Test 1: Restaurant Count\n")

# Count in CSV
csv_restaurants <- length(unique(df.orig$Restaurant))

# Count in Database
db_restaurants <- dbGetQuery(conn, "SELECT COUNT(*) as count FROM restaurant")$count

cat(sprintf("Unique restaurants in CSV: %d\n", csv_restaurants))
cat(sprintf("Total restaurants in DB: %d\n", db_restaurants))

if (csv_restaurants == db_restaurants) {
  cat("Restaurant counts match!\n\n")
} else {
  cat("Restaurant counts do NOT match!\n\n")
}

# Test 2: Count Unique Customers
cat("Test 2: Customer Count\n")

# Count unique customers in CSV (only those with identifying information)
csv_customers_with_info <- df.orig[
  df.orig$CustomerName != "" | 
    df.orig$CustomerPhone != "" | 
    df.orig$CustomerEmail != "", 
]
csv_unique_customers <- nrow(unique(csv_customers_with_info[, c("CustomerName", "CustomerPhone", "CustomerEmail")]))

# Count in Database
db_customers <- dbGetQuery(conn, "SELECT COUNT(*) as count FROM customer")$count

cat(sprintf("Unique customers with info in CSV: %d\n", csv_unique_customers))
cat(sprintf("Total customers in DB: %d\n", db_customers))

if (csv_unique_customers == db_customers) {
  cat("Customer counts match!\n\n")
} else {
  cat("Customer counts differ (this may be due to duplicate handling)\n\n")
}

# Test 3: Count Unique Servers
cat("Test 3: Server Count\n")

# Count unique servers in CSV (excluding N/A)
csv_servers <- df.orig[df.orig$ServerName != "N/A" & df.orig$ServerName != "", ]
csv_unique_servers <- length(unique(csv_servers$ServerEmpID))

# Count in Database
db_servers <- dbGetQuery(conn, "SELECT COUNT(*) as count FROM server")$count

cat(sprintf("Unique servers in CSV (excluding N/A): %d\n", csv_unique_servers))
cat(sprintf("Total servers in DB: %d\n", db_servers))

if (csv_unique_servers == db_servers) {
  cat("Server counts match!\n\n")
} else {
  cat("Server counts do not match\n\n")
}

# Test 4: Count Visits
cat("Test 4: Visit Count\n")

# Count in CSV (all rows are visits)
csv_visits <- nrow(df.orig)

# Count in Database
db_visits <- dbGetQuery(conn, "SELECT COUNT(*) as count FROM visit")$count

cat(sprintf("Total visits in CSV: %d\n", csv_visits))
cat(sprintf("Total visits in DB:  %d\n", db_visits))

if (csv_visits == db_visits) {
  cat("Visit counts match!\n\n")
} else {
  cat("Visit counts do NOT match!\n\n")
}

# Test 5: Sum Food Bills
cat("Test 5: Total Food Bill Amount\n")

# Sum in CSV
csv_food_total <- sum(as.numeric(df.orig$FoodBill), na.rm = TRUE)

# Sum in Database
db_food_total <- dbGetQuery(conn, "SELECT SUM(food_bill) as total FROM `order`")$total

cat(sprintf("Total food bill in CSV: $%.2f\n", csv_food_total))
cat(sprintf("Total food bill in DB:  $%.2f\n", db_food_total))

# Allow small floating point differences
difference <- abs(csv_food_total - db_food_total)
if (difference < 0.01) {
  cat("Food bill totals match!\n\n")
} else {
  cat(sprintf("Food bill totals differ by $%.2f\n\n", difference))
}

# Test 6: Sum Alcohol Bills
cat("Test 6: Total Alcohol Bill Amount\n")

# Sum in CSV
csv_alcohol_total <- sum(as.numeric(df.orig$AlcoholBill), na.rm = TRUE)

# Sum in Database
db_alcohol_total <- dbGetQuery(conn, "SELECT SUM(alcohol_bill) as total FROM `order`")$total

cat(sprintf("Total alcohol bill in CSV: $%.2f\n", csv_alcohol_total))
cat(sprintf("Total alcohol bill in DB: $%.2f\n", db_alcohol_total))

# Allow small floating point differences
difference <- abs(csv_alcohol_total - db_alcohol_total)
if (difference < 0.01) {
  cat("Alcohol bill totals match!\n\n")
} else {
  cat(sprintf("Alcohol bill totals differ by $%.2f\n\n", difference))
}

# Test 7: Sum Tips
cat("Test 7: Total Tip Amount\n")
# Sum in CSV
csv_tip_total <- sum(as.numeric(df.orig$TipAmount), na.rm = TRUE)
# Sum in Database
db_tip_total <- dbGetQuery(conn, "SELECT SUM(tip) as total FROM payment")$total

cat(sprintf("Total tips in CSV: $%.2f\n", csv_tip_total))
cat(sprintf("Total tips in DB:  $%.2f\n", db_tip_total))

# Allow small floating point differences
difference <- abs(csv_tip_total - db_tip_total)
if (difference < 0.01) {
  cat("Tip totals match!\n\n")
} else {
  cat(sprintf("Tip totals differ by $%.2f\n\n", difference))
}

# Total revenue
total_revenue <- dbGetQuery(conn, "
  SELECT SUM(food_bill + alcohol_bill) as total 
  FROM `order`
")$total

cat(sprintf("Total revenue (food + alcohol): $%.2f\n\n", total_revenue))

# Test 8: Total Amount Spent (Food + Alcohol + Tips Combined)
cat("Test 8: Total Amount Spent (Food + Alcohol + Tips)\n")

# Sum in CSV
csv_total_spent <- sum(as.numeric(df.orig$FoodBill), na.rm = TRUE) + 
  sum(as.numeric(df.orig$AlcoholBill), na.rm = TRUE) +
  sum(as.numeric(df.orig$TipAmount), na.rm = TRUE)

# Sum in Database
db_total_spent <- dbGetQuery(conn, "
  SELECT SUM(o.food_bill + o.alcohol_bill + p.tip) as total
  FROM `order` o
  JOIN payment p ON o.order_id = p.order_id
")$total

cat(sprintf("Total amount spent in CSV: $%.2f\n", csv_total_spent))
cat(sprintf("Total amount spent in DB:  $%.2f\n", db_total_spent))

# Allow small floating point differences
difference <- abs(csv_total_spent - db_total_spent)
if (difference < 0.01) {
  cat("Total amount spent matches!\n\n")
} else {
  cat(sprintf("Total amount spent differs by $%.2f\n\n", difference))
}

# Cleanup
dbDisconnect(conn)