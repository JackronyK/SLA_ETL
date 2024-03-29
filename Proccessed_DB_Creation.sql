-- Attempt to create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS KRA_SLA_ETL_Project;

-- Switch to the newly created or existing database
USE KRA_SLA_ETL_Project;

-- Displaying a message indicating the database in use
SELECT 'Using database: KRA_SLA_ETL_Project' AS Message;

-- Creating the schemas
-- now that we will need to create the schemas I will comment out this part
/*
-- SLA Combined Table
-- Dropping it if it exists
DROP TABLE IF EXISTS SLA_Combined_df;
CREATE TABLE IF NOT EXISTS SLA_Combined_df (
  `Link_ID` VARCHAR(255) NOT NULL,
  `SLA_Date` text,
  `Last_Mile` VARCHAR(255),
  `Capacity_in_Mbps` INT,
  `Location` VARCHAR(255),
  `MRC_Excl` DECIMAL(10,2),
  `SLM_Comments` TEXT,
  `QRC_Incl` DECIMAL(10,2),
  `SLA_ID` VARCHAR(255),
  `Service_Provider` VARCHAR(255),
  PRIMARY KEY (`Link_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Invoice Combined DF
-- Dropping it if it exists
DROP TABLE IF EXISTS Invoice_Combined_df;
CREATE TABLE IF NOT EXISTS Invoice_Combined_df (
  `Link_ID` VARCHAR(255) NOT NULL,
  `Invoice_Period` VARCHAR(255),
  `Invoice_Description` TEXT,
  `Invoice_Reference` VARCHAR(255),
  `Total_QRC` DECIMAL(10,2),
  `Invoice_Date` TEXT,
  `Service_Provider` VARCHAR(255),
  PRIMARY KEY (`Link_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

*/
