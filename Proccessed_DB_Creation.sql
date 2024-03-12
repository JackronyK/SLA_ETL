-- Attempt to create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS KRA_SLA_ETL_Project;

-- Switch to the newly created or existing database
USE KRA_SLA_ETL_Project;

-- Displaying a message indicating the database in use
SELECT 'Using database: KRA_SLA_ETL_Project' AS Message;

-- Creating the schemas
-- SLA Combined Table
drop table  if exists SLA_Combined_df;
create table if not exists SLA_Combined_df(
Link_ID varchar(20) not null primary key,
SLA_Date datetime,
Last_Mile varchar(20),
`Capacity(Mbps)` Int,
Location varchar(100),
SLA_ID varchar(30),
Service_Provider varchar(20),
MRC_Excl float,
QRC_Incl float,
SLM_Comments varchar(50)
);

-- Invoice Combined DF
drop table  if exists Invoice_Combined_df;
create table if not exists Invoice_Combined_df(
Link_ID varchar(20) not null primary key,
Invoice_Date datetime,
Invoice_Reference varchar(20),
Invoice_Perid varchar(30),
Invoice_Description varchar(30),
Service_Provider varchar(20),
Total_QRC float
);

