# *********************************************
# Program: configBusinessLogic.PractI.SheteS.R
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
db_user <- "xxxxxx"
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

# ******************************************************************************
# Stored Procedure 1: storeVisit
# Description: Adds a new visit when restaurant, customer, and server already exist
# Parameters:
#   - p_rest_id: Restaurant ID (existing)
#   - p_server_id: Server ID (existing, can be NULL)
#   - p_cust_id: Customer ID (existing, can be NULL)
#   - p_visit_date: Date of visit
#   - p_visit_time: Time of visit (can be NULL)
#   - p_wait_time: Wait time in minutes
#   - p_party_size: Number of people in party
#   - p_gender: Gender composition ('all_male', 'all_female', 'mixed', 'unknown')
#   - p_meal_type: Meal type ID (1=Breakfast, 2=Lunch, 3=Dinner, 4=Take-Out)
#   - p_ordered_alcohol: Boolean (0 or 1)
#   - p_food_bill: Food bill amount
#   - p_alcohol_bill: Alcohol bill amount
#   - p_method_id: Payment method ID (1=Mobile Payment, 2=Cash, 3=Credit Card)
#   - p_tip: Tip amount
#   - p_discount: Discount percentage (0.0 to 1.0)
# ******************************************************************************

cat("Creating Stored Procedure: storeVisit\n")

# Drop procedure if exists
drop_proc1 <- "DROP PROCEDURE IF EXISTS storeVisit"
dbExecute(conn, drop_proc1)

# Create stored procedure
create_proc1 <- "
CREATE PROCEDURE storeVisit(
  IN p_rest_id INT,
  IN p_server_id INT,
  IN p_cust_id INT,
  IN p_visit_date DATE,
  IN p_visit_time TIME,
  IN p_wait_time INT,
  IN p_party_size INT,
  IN p_gender VARCHAR(50),
  IN p_meal_type INT,
  IN p_ordered_alcohol BOOLEAN,
  IN p_food_bill DECIMAL(10,2),
  IN p_alcohol_bill DECIMAL(10,2),
  IN p_method_id INT,
  IN p_tip DECIMAL(10,2),
  IN p_discount DECIMAL(10,2),
  OUT out_visit_id INT
)
BEGIN
  DECLARE v_order_id INT;
  
  -- Insert visit
  INSERT INTO visit (rest_id, server_id, cust_id, visit_date, visit_time, 
                     wait_time, party_size, gender)
  VALUES (p_rest_id, p_server_id, p_cust_id, p_visit_date, p_visit_time,
          p_wait_time, p_party_size, p_gender);
  
  -- Get the visit ID
  SET out_visit_id = LAST_INSERT_ID();
  
  -- Insert order
  INSERT INTO `order` (visit_id, meal_type, ordered_alcohol, food_bill, alcohol_bill)
  VALUES (out_visit_id, p_meal_type, p_ordered_alcohol, p_food_bill, p_alcohol_bill);
  
  -- Get the order ID
  SET v_order_id = LAST_INSERT_ID();
  
  -- Insert payment
  INSERT INTO payment (order_id, method_id, tip, discount)
  VALUES (v_order_id, p_method_id, p_tip, p_discount);
END
"

tryCatch({
  dbExecute(conn, create_proc1)
  cat("Stored procedure 'storeVisit' creation successfull.\n\n")
}, error = function(e) {
  cat(sprintf("Error creating 'storeVisit': %s\n\n", e$message))
})

# ******************************************************************************
# Stored Procedure 2: storeNewVisit
# Description: Adds a new visit and creates restaurant, customer, or server if needed
# Parameters: Same as storeVisit, plus:
#   - p_rest_name: Restaurant name (creates if doesn't exist)
#   - p_city: City
#   - p_state: State
#   - p_has_service: Boolean (0 or 1)
#   - p_cust_name: Customer name (creates if doesn't exist)
#   - p_cust_phone: Customer phone
#   - p_cust_email: Customer email
#   - p_loyalty_member: Boolean (0 or 1)
#   - p_server_name: Server name (creates if doesn't exist)
#   - p_server_tin: Server TIN
#   - p_birth_date: Server birth date
#   - p_work_start: Server work start date
#   - p_work_end: Server work end date
#   - p_hourly_rate: Server hourly rate
# ******************************************************************************

cat("Creating Stored Procedure: storeNewVisit\n")

# Drop procedure if exists
drop_proc2 <- "DROP PROCEDURE IF EXISTS storeNewVisit"
dbExecute(conn, drop_proc2)

# Create stored procedure
create_proc2 <- "
CREATE PROCEDURE storeNewVisit(
  IN p_rest_name VARCHAR(100),
  IN p_city VARCHAR(50),
  IN p_state CHAR(2),
  IN p_has_service BOOLEAN,
  IN p_server_name VARCHAR(100),
  IN p_server_tin VARCHAR(11),
  IN p_birth_date DATE,
  IN p_work_start DATE,
  IN p_work_end DATE,
  IN p_hourly_rate DECIMAL(5,2),
  IN p_cust_name VARCHAR(100),
  IN p_cust_phone VARCHAR(20),
  IN p_cust_email VARCHAR(100),
  IN p_loyalty_member BOOLEAN,
  IN p_visit_date DATE,
  IN p_visit_time TIME,
  IN p_wait_time INT,
  IN p_party_size INT,
  IN p_gender VARCHAR(50),
  IN p_meal_type INT,
  IN p_ordered_alcohol BOOLEAN,
  IN p_food_bill DECIMAL(10,2),
  IN p_alcohol_bill DECIMAL(10,2),
  IN p_method_id INT,
  IN p_tip DECIMAL(10,2),
  IN p_discount DECIMAL(10,2),
  OUT out_visit_id INT
)
BEGIN
  DECLARE v_rest_id INT;
  DECLARE v_server_id INT;
  DECLARE v_cust_id INT;
  DECLARE v_order_id INT;
  
  -- Check if restaurant exists, if not create it
  SELECT rest_id INTO v_rest_id 
  FROM restaurant 
  WHERE rest_name = p_rest_name 
  LIMIT 1;
  
  IF v_rest_id IS NULL THEN
    INSERT INTO restaurant (rest_name, city, state, has_service)
    VALUES (p_rest_name, p_city, p_state, p_has_service);
    SET v_rest_id = LAST_INSERT_ID();
  END IF;
  
  -- Check if server exists (if name provided), if not create it
  IF p_server_name IS NOT NULL AND p_server_name != 'N/A' THEN
    SELECT server_id INTO v_server_id 
    FROM server 
    WHERE server_name = p_server_name 
    LIMIT 1;
    
    IF v_server_id IS NULL THEN
      INSERT INTO server (server_name, tin, birth_date, work_start, work_end, hourly_rate)
      VALUES (p_server_name, p_server_tin, p_birth_date, p_work_start, p_work_end, p_hourly_rate);
      SET v_server_id = LAST_INSERT_ID();
      
      -- Link server to restaurant
      INSERT IGNORE INTO restaurant_server (rest_id, server_id)
      VALUES (v_rest_id, v_server_id);
    END IF;
  ELSE
    SET v_server_id = NULL;
  END IF;
  
  -- Check if customer exists (if name/phone/email provided), if not create it
  IF p_cust_name IS NOT NULL OR p_cust_phone IS NOT NULL OR p_cust_email IS NOT NULL THEN
    SELECT cust_id INTO v_cust_id 
    FROM customer 
    WHERE (cust_name = p_cust_name OR (cust_name IS NULL AND p_cust_name IS NULL))
      AND (cust_phone = p_cust_phone OR (cust_phone IS NULL AND p_cust_phone IS NULL))
      AND (cust_email = p_cust_email OR (cust_email IS NULL AND p_cust_email IS NULL))
    LIMIT 1;
    
    IF v_cust_id IS NULL THEN
      INSERT INTO customer (cust_name, cust_phone, cust_email, loyalty_member)
      VALUES (p_cust_name, p_cust_phone, p_cust_email, p_loyalty_member);
      SET v_cust_id = LAST_INSERT_ID();
    END IF;
  ELSE
    SET v_cust_id = NULL;
  END IF;
  
  -- Insert visit
  INSERT INTO visit (rest_id, server_id, cust_id, visit_date, visit_time, 
                     wait_time, party_size, gender)
  VALUES (v_rest_id, v_server_id, v_cust_id, p_visit_date, p_visit_time,
          p_wait_time, p_party_size, p_gender);
  
  SET out_visit_id = LAST_INSERT_ID();
  
  -- Insert order
  INSERT INTO `order` (visit_id, meal_type, ordered_alcohol, food_bill, alcohol_bill)
  VALUES (out_visit_id, p_meal_type, p_ordered_alcohol, p_food_bill, p_alcohol_bill);
  
  SET v_order_id = LAST_INSERT_ID();
  
  -- Insert payment
  INSERT INTO payment (order_id, method_id, tip, discount)
  VALUES (v_order_id, p_method_id, p_tip, p_discount);
END
"

tryCatch({
  dbExecute(conn, create_proc2)
  cat("Stored procedure 'storeNewVisit' creation success.\n\n")
}, error = function(e) {
  cat(sprintf("Error creating 'storeNewVisit': %s\n\n", e$message))
})


# Test Stored Procedure 1: storeVisit
cat("Testing storeVisit procedure with existing IDs\n")

# Get existing IDs for testing
test_rest <- dbGetQuery(conn, "SELECT rest_id FROM restaurant LIMIT 1")
test_server <- dbGetQuery(conn, "SELECT server_id FROM server LIMIT 1")
test_customer <- dbGetQuery(conn, "SELECT cust_id FROM customer LIMIT 1")

if (nrow(test_rest) > 0 && nrow(test_server) > 0) {
  # Call stored procedure with OUT parameter
  tryCatch({
    dbExecute(conn, sprintf("
      CALL storeVisit(
        %d,           -- p_rest_id
        %d,           -- p_server_id
        %s,           -- p_cust_id
        '2025-01-15', -- p_visit_date
        '12:30:00',   -- p_visit_time
        5,            -- p_wait_time
        4,            -- p_party_size
        'mixed',      -- p_gender
        2,            -- p_meal_type (Lunch)
        0,            -- p_ordered_alcohol (FALSE)
        85.50,        -- p_food_bill
        0.00,         -- p_alcohol_bill
        3,            -- p_method_id (Credit Card)
        12.83,        -- p_tip
        0.00,         -- p_discount
        @new_visit_id -- OUT parameter
      )
    ", test_rest$rest_id[1], test_server$server_id[1], 
                            if(nrow(test_customer) > 0) test_customer$cust_id[1] else "NULL"))
    
    result <- dbGetQuery(conn, "SELECT @new_visit_id AS new_visit_id")
    
    cat(sprintf("Test successful! New visit ID: %d\n", result$new_visit_id))
    cat("  Visit details:\n")
    cat(sprintf("    Restaurant ID: %d\n", test_rest$rest_id[1]))
    cat(sprintf("    Server ID: %d\n", test_server$server_id[1]))
    cat("    Date: 2025-01-15, Time: 12:30:00\n")
    cat("    Party Size: 4, Food Bill: $85.50\n\n")
  }, error = function(e) {
    cat(sprintf("Test failed: %s\n\n", e$message))
  })
} else {
  cat("Skipping test - no existing data found in database.\n\n")
}


# Test Stored Procedure 2: storeNewVisit

cat("Testing storeNewVisit procedure\n")
# Call stored procedure with new restaurant/server/customer
tryCatch({
  # Call procedure
  dbExecute(conn, "
    CALL storeNewVisit(
      'Test Restaurant',      -- p_rest_name
      'Test City',            -- p_city
      'MA',                   -- p_state
      1,                      -- p_has_service
      'Test Server',          -- p_server_name
      '123-45-6789',          -- p_server_tin
      '1995-06-15',           -- p_birth_date
      '2025-01-01',           -- p_work_start
      NULL,                   -- p_work_end
      15.00,                  -- p_hourly_rate
      'Test Customer',        -- p_cust_name
      '(555) 123-4567',       -- p_cust_phone
      'test@example.com',     -- p_cust_email
      1,                      -- p_loyalty_member
      '2025-01-16',           -- p_visit_date
      '18:45:00',             -- p_visit_time
      10,                     -- p_wait_time
      2,                      -- p_party_size
      'mixed',                -- p_gender
      3,                      -- p_meal_type (Dinner)
      1,                      -- p_ordered_alcohol (TRUE)
      65.00,                  -- p_food_bill
      22.50,                  -- p_alcohol_bill
      2,                      -- p_method_id (Cash)
      15.00,                  -- p_tip
      0.10,                   -- p_discount
      @new_visit_id           -- OUT parameter
    )
  ")
  result <- dbGetQuery(conn, "SELECT @new_visit_id AS new_visit_id")
  
  cat(sprintf("Test successful! New visit ID: %d\n", result$new_visit_id))
  cat("  Created new entities:\n")
  cat("    Restaurant: Test Restaurant (Test City, MA)\n")
  cat("    Server: Test Server\n")
  cat("    Customer: Test Customer (test@example.com)\n")
  cat("  Visit details:\n")
  cat("    Date: 2025-01-16, Time: 18:45:00\n")
  cat("    Party Size: 2, Total Bill: $87.50\n\n")
}, error = function(e) {
  cat(sprintf("Test failed: %s\n\n", e$message))
})

# Verify Stored Procedures
cat("Verifying Stored Procedures\n\n")

# List all stored procedures
procedures <- dbGetQuery(conn, "
  SELECT ROUTINE_NAME, ROUTINE_TYPE 
  FROM information_schema.ROUTINES 
  WHERE ROUTINE_SCHEMA = 'defaultdb' 
  AND ROUTINE_TYPE = 'PROCEDURE'
")

if (nrow(procedures) > 0) {
  cat("Stored procedures in database:\n")
  for (i in 1:nrow(procedures)) {
    cat(sprintf("  - %s\n", procedures$ROUTINE_NAME[i]))
  }
} else {
  cat("No stored procedures found.\n")
}

# Disconnect
dbDisconnect(conn)