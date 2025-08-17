# Snowflake Project 4: Validation Modes & Copy Options

This project demonstrates advanced data loading techniques in Snowflake, focusing on **validation modes, custom file formats, error handling options, and column-specific settings**. The exercises cover:

- Using custom file formats for flexible CSV loading  
- Validating files before full ingestion  
- Handling errors with ON_ERROR and RETURN_FAILED_ONLY  
- Managing string length constraints and truncation  
- Analyzing load history for monitoring and troubleshooting  

The goal is to understand Snowflake’s capabilities for controlled and reliable data ingestion.

---

## Prerequisites

- Active Snowflake account  
- Access to Snowflake Web UI or SnowSQL  
- AWS credentials to access the specified S3 buckets  
- Demo warehouse created in Project 1  

---

## Tasks Performed

### TASK 1: Using Custom File Formats
- Created a database and table for sales data  
- Defined a custom CSV file format (comma delimiter, optional quotes, skip header)  
- Created a named stage pointing to the S3 bucket  
- Loaded data using the custom file format and verified successful ingestion  
- Cleaned up all objects after verification  

---

### TASK 2: Data Validation Using VALIDATION_MODE = RETURN_2_ROWS
- Created a database and table for sales data  
- Created a named stage pointing to the S3 bucket  
- Validated the first two rows of the CSV file before loading the full dataset  
- Observed any formatting issues and cleaned up objects  

---

### TASK 3: Handling Data Load Errors with RETURN_FAILED_ONLY = TRUE
- Created a database and table for sales data containing errors  
- Loaded data using ON_ERROR = CONTINUE while returning only the failed rows  
- Verified that valid rows were loaded and reviewed the failed rows  
- Cleaned up all objects  

---

### TASK 4: Using String Length Limits
- Created a database and table with a length constraint on the complete_address column  
- Attempted to load data from S3, which triggered errors due to exceeding length  
- Verified error behavior and cleaned up objects  

---

### TASK 5: Handling String Lengths with TRUNCATECOLUMNS
- Created a database and table with a string length constraint  
- Loaded data from S3 using TRUNCATECOLUMNS = TRUE  
- Observed that exceeding values were truncated and data was successfully ingested  
- Cleaned up all objects  

---

### TASK 6: Using Snowflake Load History to Analyze Data Loads
- Queried the `snowflake.account_usage.load_history` view to track past loads  
- Identified load operations with errors and historical load times  
- Analyzed results for monitoring and troubleshooting  

---

## Real-World Relevance

- Custom file formats provide flexibility for various data sources  
- Validation modes prevent bad data from being ingested into production  
- Error handling options ensure robust pipelines that skip or log problematic rows  
- String length management and truncation help maintain schema constraints without manual data cleaning  
- Load history analysis supports auditing, monitoring, and troubleshooting of data pipelines
