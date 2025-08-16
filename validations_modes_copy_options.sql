-- TASK 1:- Using Custom File Formats with the COPY Command
-- Use the demo warehouse for this assignment
USE WAREHOUSE demo;

-- Create a database to store sales data
CREATE OR REPLACE DATABASE sales_db_custom_format;

-- Create the table to store sales data
CREATE OR REPLACE TABLE sales_custom_format (
    order_id INTEGER,            -- Unique identifier for the order
    customer_id INTEGER,         -- Unique identifier for the customer
    customer_name VARCHAR,       -- Name of the customer
    order_date DATE,             -- Date of the order
    product VARCHAR,             -- Product purchased
    quantity INTEGER,            -- Quantity of the product ordered
    price FLOAT,                 -- Price of the product
    complete_address VARCHAR     -- Full address of the customer
);

-- Create a custom file format to handle the specific CSV structure
CREATE OR REPLACE FILE FORMAT csv_custom_format
TYPE = 'CSV'                          -- Specify file type as CSV
FIELD_DELIMITER = ','                 -- Use comma as the delimiter
SKIP_HEADER = 1                       -- Skip the header row
FIELD_OPTIONALLY_ENCLOSED_BY = '"';     -- Enclose fields with double quotes

-- Create a stage to reference the S3 bucket where the data file is stored
CREATE OR REPLACE STAGE s3_stage_custom_format
URL = 's3://snowflake-hands-on-data/sample_data_basic/sales_sample_data.csv'
CREDENTIALS = (
  AWS_KEY_ID = 8888888888888 
  AWS_SECRET_KEY = 8888888888888
);       

-- Load data from the S3 bucket into the table using the custom file format
COPY INTO sales_custom_format
FROM @s3_stage_custom_format
FILE_FORMAT = csv_custom_format;

-- Verify that the data has been loaded correctly into the table
SELECT * FROM sales_custom_format;

-- Drop the stage after completing the assignment to free up resources
DROP STAGE IF EXISTS s3_stage_custom_format;

-- Drop the table after completing the assignment
DROP TABLE IF EXISTS sales_custom_format;

-- Drop the database to ensure no resources are left behind
DROP DATABASE IF EXISTS sales_db_custom_format;




-- TASK 2:- Data Validation Using VALIDATION_MODE = RETURN_2_ROWS
-- Use the demo warehouse for this assignment
USE WAREHOUSE demo;

-- Create a database to store sales data for validation
CREATE OR REPLACE DATABASE sales_db_validation;

-- Create the table to store sales data
CREATE OR REPLACE TABLE sales_validation (
    order_id INTEGER,            -- Unique identifier for the order
    customer_id INTEGER,         -- Unique identifier for the customer
    customer_name VARCHAR,       -- Name of the customer
    order_date DATE,             -- Date of the order
    product VARCHAR,             -- Product purchased
    quantity INTEGER,            -- Quantity of the product ordered
    price FLOAT,                 -- Price of the product
    complete_address VARCHAR     -- Full address of the customer
);

-- Create a stage to reference the S3 bucket where the data file is stored
CREATE OR REPLACE STAGE s3_stage_validation
URL = 's3://snowflake-hands-on-data/sample_data_basic/'
CREDENTIALS = (
  AWS_KEY_ID = 8888888888888 
  AWS_SECRET_KEY = 8888888888888
);

-- Validate the first two rows of data from the S3 bucket using the COPY command
COPY INTO sales_validation
FROM @s3_stage_validation/sales_sample_data.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
VALIDATION_MODE = RETURN_2_ROWS;  -- Only validate the first two rows

--select statement to verify
select * from sales_validation;

-- Drop the stage after completing the assignment to free up resources
DROP STAGE IF EXISTS s3_stage_validation;

-- Drop the table after completing the assignment
DROP TABLE IF EXISTS sales_validation;

-- Drop the database to ensure no resources are left behind
DROP DATABASE IF EXISTS sales_db_validation;




--TASK 3:- Handling Data Load Errors with RETURN_FAILED_ONLY = TRUE
-- Use the demo warehouse
USE WAREHOUSE demo;

-- Create a new database for this assignment
CREATE OR REPLACE DATABASE sales_db_continue;

-- Create the sales_continue table with the specified schema
CREATE OR REPLACE TABLE sales_continue (
  order_id INTEGER,
  customer_id INTEGER,
  customer_name STRING,
  order_date DATE,
  product STRING,
  quantity INTEGER,
  price FLOAT,
  complete_address STRING
);

-- Create a named stage pointing to the S3 bucket
CREATE OR REPLACE STAGE s3_stage_continue
URL = 's3://snowflake-hands-on-data/sample_data_with_errors/basic/'
CREDENTIALS = (
  AWS_KEY_ID = 8888888888
  AWS_SECRET_KEY = 88888888888888888
);

-- Load data from the CSV file, continuing on errors and returning only failed rows
COPY INTO sales_continue
FROM @s3_stage_continue/sales_sample_data_with_errors.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = TRUE;

select * from sales_continue;

-- Drop the stage, table, and database after completing the task
DROP STAGE IF EXISTS s3_stage_continue;
DROP TABLE IF EXISTS sales_continue;
DROP DATABASE IF EXISTS sales_db_continue;




-- TASK 4:- Using String Length Limit
-- Use the demo warehouse
USE WAREHOUSE demo;

-- Create a new database for this assignment
CREATE OR REPLACE DATABASE sales_db_fixed_limit;

-- Create the table with a fixed length of 10 characters for complete_address
CREATE OR REPLACE TABLE sales_fixed_limit (
    order_id INTEGER,
    customer_id INTEGER,
    customer_name STRING,
    order_date DATE,
    product STRING,
    quantity INTEGER,
    price FLOAT,
    complete_address STRING(10)  -- Fixed length of 10 characters
);

-- Create the stage for loading data from the S3 bucket
CREATE OR REPLACE STAGE s3_stage_fixed_limit
URL = 's3://snowflake-hands-on-data/sample_data_with_errors/basic/'
CREDENTIALS = (
     AWS_KEY_ID = 888888888888
     AWS_SECRET_KEY = 8888888888888
);

-- Load data from the S3 bucket, expecting failures due to the fixed length of complete_address
COPY INTO sales_fixed_limit
FROM @s3_stage_fixed_limit/sales_sample_data_with_errors.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';  -- Continue loading despite errors

-- Verify the data loaded into the table
SELECT * FROM sales_fixed_limit;

-- Drop the stage, table, and database after the task is done
DROP STAGE IF EXISTS s3_stage_fixed_limit;
DROP TABLE IF EXISTS sales_fixed_limit;
DROP DATABASE IF EXISTS sales_db_fixed_limit;




-- TASK 5:- Handling String Lengths with TRUNCATECOLUMNS
-- Use the demo warehouse
USE WAREHOUSE demo;

-- Create a new database for this assignment
CREATE OR REPLACE DATABASE sales_db_truncate_columns;

-- Create the table with a fixed length of 10 characters for complete_address
CREATE OR REPLACE TABLE sales_truncate_columns (
    order_id INTEGER,
    customer_id INTEGER,
    customer_name STRING,
    order_date DATE,
    product STRING,
    quantity INTEGER,
    price FLOAT,
    complete_address STRING(10)  -- Fixed length of 10 characters
);

-- Create the stage for loading data from the S3 bucket
CREATE OR REPLACE STAGE s3_stage_truncate_columns
URL = 's3://snowflake-hands-on-data/sample_data_with_errors/basic/'
CREDENTIALS = (
    AWS_KEY_ID = 8888888888888
    AWS_SECRET_KEY = 88888888888888
);

-- Load data from the S3 bucket, truncating columns that exceed their length
COPY INTO sales_truncate_columns
FROM @s3_stage_truncate_columns/sales_sample_data_with_errors.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE'
TRUNCATECOLUMNS = TRUE;  -- Truncate data that exceeds the length limit

-- Verify that the data was loaded and truncated where necessary
SELECT * FROM sales_truncate_columns;

-- Drop the stage, table, and database after the task is done
DROP STAGE IF EXISTS s3_stage_truncate_columns;
DROP TABLE IF EXISTS sales_truncate_columns;
DROP DATABASE IF EXISTS sales_db_truncate_columns;




-- TASK 6:- Using Snowflake’s Load History to Analyze Data Loads
-- Use the demo warehouse
USE WAREHOUSE demo;

-- Query to find all rows where error_count is greater than 0
SELECT * 
FROM snowflake.account_usage.load_history
WHERE schema_name = 'PUBLIC'
AND error_count > 0;

-- Query to find all loads where the LAST_LOAD_TIME occurred before yesterday
SELECT * 
FROM snowflake.account_usage.load_history
WHERE DATE(LAST_LOAD_TIME) <= DATEADD(days, -1, CURRENT_DATE);
