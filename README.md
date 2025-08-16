# snflk_mini_proj4_validation_modes_copy_options

TASK 1:- Using Custom File Formats with the COPY Command
Use the demo Warehouse: Ensure you are using the demo warehouse for this assignment.
Create a Database: Create a database called sales_db_custom_format.

Create a Table: Create a table named sales_custom_format with the following schema:
order_id INTEGER
customer_id INTEGER
customer_name STRING
order_date DATE
product STRING
quantity INTEGER
price FLOAT
complete_address STRING

Create a Custom File Format: Define a custom file format for loading CSV files that:
Uses comma (,) as the field delimiter.
Encloses fields optionally within double quotes (").
Skips the header row.

Create a Named Stage: Create a named stage s3_stage_custom_format that points to the following S3 bucket:
S3 Path: s3://snowflake-hands-on-data/sample_data_basic/sales_sample_data.csv
AWS Access Key: 8888888888888
AWS Secret Key: 8888888888888

Load Data Using the Custom File Format: Load data into the sales_custom_format table from the S3 bucket using the custom file format created in Step 4.

Verify the Data: Query the table to ensure the data has been loaded correctly.

Clean Up: Once done, drop the stage, table, and database.



TASK 2:- Data Validation Using VALIDATION_MODE = RETURN_2_ROWS
Use the demo Warehouse: Ensure you are using the demo warehouse for this assignment.
Create a Database: Create a database called sales_db_validation.

Create a Table: Create a table named sales_validation with the following schema:
order_id INTEGER
customer_id INTEGER
customer_name STRING
order_date DATE
product STRING
quantity INTEGER
price FLOAT
complete_address STRING

Create a Named Stage: Create a named stage s3_stage_validation that points to the following S3 bucket:
S3 Path: s3://snowflake-hands-on-data/sample_data_basic/sales_sample_data.csv
AWS Access Key: 88888888888
AWS Secret Key: 88888888888

Validate Data Using VALIDATION_MODE: Use VALIDATION_MODE = RETURN_2_ROWS in the COPY command to validate the first two rows of the file. This allows to check if the file is properly formatted before loading the full dataset.

Observe the Validation Output: After executing the COPY command, observe the output for any validation errors or issues.

Clean Up: Once done, drop the stage, table, and database.



TASK 3:-  Handling Data Load Errors with RETURN_FAILED_ONLY = TRUE
Use the demo Warehouse: Ensure you are using the demo warehouse for this assignment.
Create a Database: Create a database named sales_db_continue.

Create a Table: Create a table named sales_continue with the following schema:
order_id INTEGER
customer_id INTEGER
customer_name STRING
order_date DATE
product STRING
quantity INTEGER
price FLOAT
complete_address STRING

Create a Named Stage: Create a named stage s3_stage_continue that points to the S3 bucket containing the CSV file.
S3 Path: s3://snowflake-hands-on-data/sample_data_with_errors/basic/sales_sample_data_with_errors.csv
AWS Access Key: 888888888888
AWS Secret Key: 88888888888

Load Data Using ON_ERROR = 'CONTINUE' and RETURN_FAILED_ONLY = TRUE: Load the data from the file into the sales_continue table using the ON_ERROR = 'CONTINUE' option. This will ensure that the valid rows are loaded, while the rows with errors are skipped.
Use the RETURN_FAILED_ONLY = TRUE option to return only the rows that failed during the load process.

Observe the Outcome: Check the data loaded into the sales_continue table.
Review the failed rows returned by RETURN_FAILED_ONLY = TRUE and observe the errors that caused them to fail.

Clean Up: Once done, drop the stage, table, and database.



TASK 4:- Using String Length Limit
Use the demo Warehouse: Ensure you are using the demo warehouse for this assignment.
Create a Database: Create a database called sales_db_length_limit.

Create a Table: Create a table named sales_length_limit with the following schema:
order_id INTEGER
customer_id INTEGER
customer_name STRING
order_date DATE
product STRING
quantity INTEGER
price FLOAT
complete_address STRING(10) — Limit the string length to 10 characters.

Load Data from S3: Load the data from the following S3 path into the sales_length_limit table:
S3 Path: s3://snowflake-hands-on-data/sample_data_basic/sales_sample_data.csv
Use the COPY INTO command to load the data, but note that the complete_address column is restricted to 10 characters. This will trigger errors if any address exceeds this limit.

Observe the Error: Verify that Snowflake throws an error when loading the data because some of the complete_address values exceed the defined length.

Clean Up: Once done, drop the stage, table, and database.



TASK 5:- Handling String Lengths with TRUNCATECOLUMNS
Use the demo Warehouse: Ensure you are using the demo warehouse for this assignment.
Create a Database: Create a database called sales_db_truncate_columns.

Create a Table: Create a table named sales_truncate_columns with the following schema:
order_id INTEGER
customer_id INTEGER
customer_name STRING
order_date DATE
product STRING
quantity INTEGER
price FLOAT
complete_address STRING(10) — Limit the string length to 10 characters.

Load Data with TRUNCATECOLUMNS: Load the data from the same S3 path used in Assignment 15, but this time, include TRUNCATECOLUMNS = TRUE in your COPY INTO command.
S3 Path: s3://snowflake-hands-on-data/sample_data_basic/sales_sample_data.csv

Command Example:
COPY INTO sales_truncate_columns
FROM @s3_stage/sales_sample_data.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
TRUNCATECOLUMNS = TRUE;

Verify the Outcome: Observe that Snowflake truncates the complete_address values that exceed 10 characters and loads the data successfully without errors.

Clean Up: Once done, drop the stage, table, and database.



TASK 6:- Using Snowflake’s Load History to Analyze Data Loads
Use the demo Warehouse: Ensure you are using demo warehouse for this assignment.

Perform Load History Query 1: Query the snowflake.account_usage.load_history view and select all rows where error_count > 0. This will give you insights into loads that encountered issues.

Perform Load History Query 2: Query the snowflake.account_usage.load_history view to retrieve all loads where the LAST_LOAD_TIME occurred before yesterday's date. This will help you track historical data loads over time.

Analyze Results: Review the results of the queries to understand how Snowflake tracks load history, errors, and timing of data loads.



