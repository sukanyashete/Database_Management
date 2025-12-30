# **********************************************
# Program: createDB.PractI.SheteS.R
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
db_password <- "xxxxxx" 
db_host <- "xxxxx" 
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

# Connect to local database.
conn_sqlite <- dbConnect(SQLite(), "restaurants-db.sqlitedb")


# Function to create a table only if it doesn't exist
# Parameters -
#   conn: database connection object
#   table_name: name of the table to create
#   create_sql: SQL CREATE TABLE statement
createTableIfNotExists <- function(conn, table_name, create_sql) {
  tryCatch({
    dbExecute(conn, create_sql)
    cat(sprintf("Table '%s' created successfully.\n", table_name))
  }, error = function(e) {
    cat(sprintf("Table '%s' may already exist or error occurred: %s\n", 
                table_name, e$message))
  })
}

# Creating table RESTAURANT 
sql_restaurant <- "
CREATE TABLE IF NOT EXISTS restaurant (
  rest_id INT PRIMARY KEY AUTO_INCREMENT,
  rest_name VARCHAR(100) NOT NULL,
  city VARCHAR(50) NOT NULL,
  state CHAR(2) NOT NULL,
  has_service BOOLEAN NOT NULL DEFAULT TRUE,
  UNIQUE KEY uk_restaurant_name (rest_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"
createTableIfNotExists(conn, "restaurant", sql_restaurant)

# Creating table SERVER
sql_server <- "
CREATE TABLE IF NOT EXISTS server (
  server_id INT PRIMARY KEY AUTO_INCREMENT,
  server_name VARCHAR(100) NOT NULL,
  tin VARCHAR(11) DEFAULT NULL,
  birth_date DATE DEFAULT NULL,
  work_start DATE NOT NULL,
  work_end DATE DEFAULT NULL,
  hourly_rate DECIMAL(5,2) NOT NULL DEFAULT 10.00,
  CHECK (hourly_rate >= 0),
  CHECK (work_end IS NULL OR work_end >= work_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"
createTableIfNotExists(conn, "server", sql_server)

#Creating table RESTAURANT_SERVER 
sql_restaurant_server <- "
CREATE TABLE IF NOT EXISTS restaurant_server (
  rest_id INT NOT NULL,
  server_id INT NOT NULL,
  PRIMARY KEY (rest_id, server_id),
  FOREIGN KEY (rest_id) REFERENCES restaurant(rest_id) 
    ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (server_id) REFERENCES server(server_id) 
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"
createTableIfNotExists(conn, "restaurant_server", sql_restaurant_server)

# Creating table CUSTOMER
sql_customer <- "
CREATE TABLE IF NOT EXISTS customer (
  cust_id INT PRIMARY KEY AUTO_INCREMENT,
  cust_name VARCHAR(100) DEFAULT NULL,
  cust_phone VARCHAR(20) DEFAULT NULL,
  cust_email VARCHAR(100) DEFAULT NULL,
  loyalty_member BOOLEAN NOT NULL DEFAULT FALSE,
  UNIQUE KEY uk_customer_contact (cust_phone, cust_email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"
createTableIfNotExists(conn, "customer", sql_customer)

# Creating table VISIT
sql_visit <- "
CREATE TABLE IF NOT EXISTS visit (
  visit_id INT PRIMARY KEY AUTO_INCREMENT,
  rest_id INT NOT NULL,
  server_id INT DEFAULT NULL,
  cust_id INT DEFAULT NULL,
  visit_date DATE NOT NULL,
  visit_time TIME DEFAULT NULL,
  wait_time INT DEFAULT 0,
  party_size INT NOT NULL DEFAULT 1,
  gender VARCHAR(50) DEFAULT 'unknown',
  FOREIGN KEY (rest_id) REFERENCES restaurant(rest_id) 
    ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (server_id) REFERENCES server(server_id) 
    ON DELETE SET NULL ON UPDATE CASCADE,
  FOREIGN KEY (cust_id) REFERENCES customer(cust_id) 
    ON DELETE SET NULL ON UPDATE CASCADE,
  CHECK (gender IN ('all_male', 'all_female', 'mixed', 'unknown')),
  CHECK (party_size > 0),
  CHECK (wait_time >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"
createTableIfNotExists(conn, "visit", sql_visit)

# Creating table MEAL(Lookup table)
sql_meal <- "
CREATE TABLE IF NOT EXISTS meal (
  meal_type INT PRIMARY KEY AUTO_INCREMENT,
  meal_name VARCHAR(20) NOT NULL,
  UNIQUE KEY uk_meal_name (meal_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"
createTableIfNotExists(conn, "meal", sql_meal)

# Insert meal types
sql_insert_meals <- "
INSERT IGNORE INTO meal (meal_type, meal_name) VALUES
  (1, 'Breakfast'),
  (2, 'Lunch'),
  (3, 'Dinner'),
  (4, 'Take-Out');
"
tryCatch({
  dbExecute(conn, sql_insert_meals)
  cat("Meal lookup data inserted.\n")
}, error = function(e) {
  cat(sprintf("%s: Meal data may already exist\n", e$message))
})

# Creating table ORDER
sql_order <- "
CREATE TABLE IF NOT EXISTS `order` (
  order_id INT PRIMARY KEY AUTO_INCREMENT,
  visit_id INT NOT NULL,
  meal_type INT NOT NULL,
  ordered_alcohol BOOLEAN NOT NULL DEFAULT FALSE,
  food_bill DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  alcohol_bill DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  FOREIGN KEY (visit_id) REFERENCES visit(visit_id) 
    ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (meal_type) REFERENCES meal(meal_type) 
    ON DELETE RESTRICT ON UPDATE CASCADE,
  CHECK (food_bill >= 0),
  CHECK (alcohol_bill >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"
createTableIfNotExists(conn, "`order`", sql_order)

# Creating table PAYMENT_TYPE(Lookup Table)
sql_payment_type <- "
CREATE TABLE IF NOT EXISTS payment_type (
  method_id INT PRIMARY KEY AUTO_INCREMENT,
  method_name VARCHAR(50) NOT NULL,
  UNIQUE KEY uk_method_name (method_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"
createTableIfNotExists(conn, "payment_type", sql_payment_type)

# Insert payment types
sql_insert_payment_types <- "
INSERT IGNORE INTO payment_type (method_id, method_name) VALUES
  (1, 'Mobile Payment'),
  (2, 'Cash'),
  (3, 'Credit Card');
"
tryCatch({
  dbExecute(conn, sql_insert_payment_types)
  cat("Payment type lookup data inserted.\n")
}, error = function(e) {
  cat(sprintf("%s: Payment type data may already exist\n", e$message))
})

# Creating table PAYMENT
sql_payment <- "
CREATE TABLE IF NOT EXISTS payment (
  pay_id INT PRIMARY KEY AUTO_INCREMENT,
  order_id INT NOT NULL,
  method_id INT NOT NULL,
  tip DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  discount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  FOREIGN KEY (order_id) REFERENCES `order`(order_id) 
    ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (method_id) REFERENCES payment_type(method_id) 
    ON DELETE RESTRICT ON UPDATE CASCADE,
  CHECK (tip >= 0),
  CHECK (discount >= 0 AND discount <= 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"
createTableIfNotExists(conn, "payment", sql_payment)

# Verify all tables created
tables <- dbListTables(conn)
cat("\nTables created:\n")
for (table in tables) {
 cat(sprintf("  - %s\n", table))
}

# Disconnect connections
dbDisconnect(conn)
dbDisconnect(conn_sqlite)