-- Attempt to create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS KRA_SLA_ETL_Project;

-- Switch to the newly created or existing database
USE KRA_SLA_ETL_Project;

-- Displaying a message indicating the database in use
SELECT 'Using database: KRA_SLA_ETL_Project' AS Message;

-- Creating the schemas
