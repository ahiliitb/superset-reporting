import configparser
import logging
import time
from typing import List, Tuple, Dict, Union
import psycopg2
from psycopg2 import pool, extras
from psycopg2.extras import Json

import applog

class TableColumn:
    """
        Represents a column in the table with name, datatype, and constraints. 
    """
    def __init__(self, name: str, datatype: str, isArray:bool = False, isPrimary:bool = False, data_format: str = None, *args, **kwargs):
        self.name = name
        self.datatype = datatype
        self.isArray = isArray
        self.isPrimary = isPrimary
        self.data_format = data_format

    def __repr__(self) -> str:
        return f"{self.name} {self.datatype}{'[]' if self.isArray else ''}{' PRIMARY KEY' if self.isPrimary else ''}"

class DatabaseConnectionPool:
    def __init__(self, username: str, password: str, host: str, port: str, dbname: str, max_conns=1000):
        self.conn_str = f"postgres://{username}:{password}@{host}:{port}/{dbname}"
        self.pool = pool.SimpleConnectionPool(1, max_conns, self.conn_str)

    def get_connection(self, timeout=10) -> psycopg2.extensions.connection:
        """Attempt to retrieve a connection from the pool with a timeout."""
        for _ in range(timeout):
            try:
                conn = self.pool.getconn()
                if conn:
                    return conn
            except pool.PoolError:
                applog.logger.debug("Connection pool exhausted. Waiting to retry...")
                time.sleep(1)
        raise Exception("Failed to obtain connection within timeout period")

    def release_connection(self, conn: psycopg2.extensions.connection) -> None:
        self.pool.putconn(conn)

    def execute_command(self, sql_commands: str, values: Tuple = None) -> List[Tuple]:
        conn = self.get_connection()
        with conn.cursor() as cursor:
            try:
                cursor.execute('BEGIN;')
                applog.logger.debug(cursor.mogrify(sql_commands, values).decode('utf-8'))

                # Convert Python lists to PostgreSQL arrays
                cursor.execute(sql_commands, values)
                result = cursor.fetchall() if cursor.description else []
            except Exception as e:
                conn.rollback()  # Rollback in case of error
                raise e
            else:
                conn.commit()  # Commit the transaction if all commands succeed
            finally:
                self.release_connection(conn)
        return result

    def close_connection(self) -> None:
        self.pool.closeall()
        applog.logger.debug("Database connection closed.")
    
    def __del__(self):
        self.close_connection()

    def fetch_table_schema(self) -> Dict[str, List[Tuple[str, str]]]:
        query = """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position;
        """
        schema: Dict[str, List[Tuple[str, str]]] = {}
        for table_name, column_name, data_type in self.execute_command(query):
                if (table_name not in schema):
                    schema[table_name] = []
                schema[table_name].append((column_name, data_type))
        return schema

    def fetch_table_row_counts(self) -> Dict[str, int]:
        query = """
        SELECT table_name, 
               (xpath('/row/c/text()', query_to_xml(format('SELECT COUNT(*) AS c FROM %s', table_name), false, true, '')))[1]::text::int AS row_count
        FROM information_schema.tables
        WHERE table_schema = 'public';
        """        
        row_counts: Dict[str, int] = {table_name: row_count for table_name, row_count in self.execute_command(query)}
        return row_counts
    

    def fetch_all_table_data(self, table_name: str) -> List[Dict[str, Union[str, int, float, None]]]:
        """
        Fetch all content of the specified table.

        Parameters:
            table_name (str): The name of the table to fetch data from.

        Returns:
            List[Dict[str, Union[str, int, float, None]]]: A list of dictionaries where each dictionary represents a row.
        """
        # Fetch column names of the table
        query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        column_names = [row[0] for row in self.execute_command(query)]


        # Fetch all rows from the table
        fetch_query = f"SELECT * FROM {table_name};"
        rows = self.execute_command(fetch_query)

        # Convert rows to a list of dictionaries
        all_data = [dict(zip(column_names, row)) for row in rows]
        return all_data

    def clear_database(self, db_name:str=None) -> None:
        if db_name:
            self.execute_command(f"DROP TABLE {db_name}_logs CASCADE")
            applog.logger.debug(f"Table {db_name} dropped successfully.")
        else:
            table_names = self.fetch_table_names()
            for table_name in table_names:
                self.execute_command(f"DROP TABLE {table_name} CASCADE")
            applog.logger.debug("All tables dropped successfully.")

    def fetch_table_names(self) -> List[str]:
        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
        """
        table_names = [row[0] for row in self.execute_command(query)]
        return table_names
    
    def fetch_lookup_table_as_dict(self, table_name: str, schema: List[TableColumn]) -> Dict[str, int]:
        """
        Fetches data from a lookup table and returns a dictionary with a string column as key
        and an integer column as value.

        Parameters:
            database (DatabaseConnectionPool): The database connection pool.
            table_name (str): The name of the lookup table.
            schema (List[TableColumn]): The schema of the lookup table.

        Returns:
            Dict[str, int]: A dictionary with string keys and integer values.
        """
        # Find the string and integer columns in the schema
        string_column = None
        int_column = None
        for column in schema:
            if column.datatype.lower() in {"varchar", "text", "char"}:
                string_column = column.name
            elif column.datatype.lower() in {"int", "integer", "bigint", "smallint"}:
                int_column = column.name

            # Break the loop if both columns are identified
            if string_column and int_column:
                break

        if not string_column or not int_column:
            raise ValueError("Schema must contain one string column and one integer column.")

        # SQL query to fetch data from the table
        query = f"SELECT {string_column}, {int_column} FROM {table_name};"

        # Execute the query and fetch results
        results = self.execute_command(query)

        # Convert the results into a dictionary
        lookup_dict = {row[0]: row[1] for row in results}

        return lookup_dict


    def create_table(self, table_name:str, schema: List[TableColumn]):
        """Creates a table with the given name and schema."""

        columns = ', '.join(str(col) for col in schema)
        query = f"CREATE TABLE {table_name} ({columns});"
        self.execute_command(query)
        applog.logger.debug(f"Table {table_name} created successfully.")
    
    def insert_data(self, table_name: str, columns: List[TableColumn], values: Tuple) -> None:
        assert len(columns) == len(values), f"Number of columns ({len(columns)}) and values ({len(values)}) do not match"
        
        columns_str = ', '.join([col.name for col in columns])
        placeholders = []
        formatted_values = []
        
        for col, val in zip(columns, values):
            if col.datatype == "TIMESTAMP":    
                if col.data_format:
                    placeholders.append(f"to_timestamp(%s, '{col.data_format}')")
                    formatted_values.append(val)
                else:
                    placeholders.append(f"to_timestamp(%s)")
                    formatted_values.append(val)
            else:
                placeholders.append('%s')
                formatted_values.append(val if not col.isArray else [val])
        
        placeholders_str = ', '.join(placeholders)
        query = f'INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders_str}) ON CONFLICT DO NOTHING'
        self.execute_command(query, formatted_values)

    def print_table_content(self, table_name: str):
        table_data = self.fetch_all_table_data(table_name)
        for row in table_data:
            print(row)
        return
    
    def get_database_size(self) -> int:
        """
        Fetches the size of the connected database with optimized querying.
        
        Returns:
            int: The size of the database in bytes.
        """
        try:
            with self.pool.getconn() as conn:  # Get a connection from the pool
                with conn.cursor() as cur:
                    # Execute the database size query
                    cur.execute("SELECT pg_database_size(current_database());")
                    result = cur.fetchone()
                    return result[0] if result else 0
        except Exception as e:
            print(f"Error fetching database size: {e}")
            return 0
        finally:
            self.pool.putconn(conn)  # Always return the connection to the pool

    
    def __repr__(self) -> str:
        schema = self.fetch_table_schema()
        row_counts = self.fetch_table_row_counts()
        repr_str = "Database Schema and Row Counts:\n"
        for table, columns in schema.items():
            repr_str += f"Table: {table}\n"
            for column_name, data_type in columns:
                repr_str += f"  Column: {column_name}, Type: {data_type}\n"
            row_count = row_counts.get(table, 0)
            repr_str += f"  Row count: {row_count}\n\n"
        return repr_str