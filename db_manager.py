import pymysql

class DB_Manager:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = pymysql(
            host = self.host,
            user = self.user,
            password = self.password,
            database = self.database        
        )
        self.cursor = self.connection.cursor()
    
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
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        self.execute_query(query)