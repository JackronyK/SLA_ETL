import pymysql
import pandas as pd
import sqlite3
from sqlalchemy import create_engine,inspect

class DB_Manager:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = pymysql.connect(host=self.host,
                                            user=self.user,
                                            password=self.password,
                                            database=self.database)
            self.cursor = self.connection.cursor()
            print("Connected to database successfully.")
        except Exception as e:
            print(f"Error connecting to database: {e}")
    
    def disconnect(self):
        if self.connection:
            self.connection.close()
    
    def execute_query(self, query):
        try:
            self.connect()
            self.cursor.execute(query)
            self.connection.commit()
        except Exception as e:
            print(f"Jack Error executing query: {e}")
            self.connection.rollback()
        finally:
            self.disconnect()

    def insert_data(self, table_name, data):
        columns = ', '.join(data.keys())
        values = ', '.join(f"'{value}'" if isinstance(value, str) else str(value) for value in data.values())
        
        # Add single quotes around date values
        values = values.replace("2020-10-01 00:00:00", "'2020-10-01 00:00:00'")
        
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        
        # Add backticks around column names
        columns_with_backticks = ', '.join(f"`{col}`" for col in data.keys())
        query = f"INSERT INTO {table_name} ({columns_with_backticks}) VALUES ({values})"
        
        print("Query:", query)  # Print the query for debugging
        self.execute_query(query)



#### Class DataFrameToSQL 

class DataFrameToSQL:
    def __init__(self, user, password, host, database):
        '''
        Initializes the class with a MySQL database connection.
        :param user: MySQL Username
        :param password: MySQL Password
        :param host: MySQL host.
        :param database: MySQL database name
        '''
        self.database = database
        
        self.engine = create_engine(
            f'mysql+pymysql://{user}:{password}@{host}:3306/{database}'
        )


    def save_to_mysql(self, df, table_name,primary_key = 'Link_ID',if_exists='replace'):
        '''
        Saves the DataFrame to the specified table in MySQL database.
        :param df: The DataFrame to Save.
        :param table_name: The name of the table to save the DataFrame to.
        :param if_exists: What to do if the table already exists. Options are fail, replace, append. Default is fail.
        '''
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
                
            # Alter table to add primary key constraint
            with self.engine.connect() as con:
                con.execute(f'ALTER TABLE {table_name} MODIFY {primary_key} VARCHAR(255) PRIMARY KEY')
                #con.execute(f'ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key})') 

            print(f'DataFrame saved to {table_name} table in MySQL database {self.database}.')
        except Exception as e:
            print(f'An error occurred while saving DataFrame to MySQL: {e}')