import logging
from typing import List, Tuple, Union, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import csv
from io import StringIO
from abc import ABC, abstractmethod
import xml.etree.ElementTree as ET
import gzip

from database import DatabaseConnectionPool, TableColumn

import applog

class LogColumn:
    """
        Represents a column in the log table with name, datatype, and constraints. 
    """
    def __init__(self, name: str, datatype: str, isArray:bool = False, isPrimary:bool = False, data_format:str = None, *args, **kwargs):
        self.name = name
        self.datatype = datatype
        self.isArray = isArray
        self.isPrimary = isPrimary
        self.data_format = data_format

    def __repr__(self) -> str:
        return f"{self.name} {self.datatype}{'[]' if self.isArray else ''}{f' {self.data_format}' if self.data_format else ''}{' PRIMARY KEY' if self.isPrimary else ''}"


class LogParser:
    def __init__(self, database: DatabaseConnectionPool, ftype="tsv",*args, **kwargs):
        self.database = database
        self.log_schema: List[LogColumn] = []
        self.file_type = ftype

    def create_tables(self) -> None:
        raise NotImplementedError("Method 'create_tables' must be implemented by a subclass.")

    def insert_log(self, log_data: Tuple) -> None:
        raise NotImplementedError("Method 'insert_log' must be implemented by a subclass.")
    
    def load_log_schema(self, log_type: str, xml_file: str) -> None:
        # Parse the XML file
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        # Find the specified log type
        log_schema = root.find(log_type)
        
        if log_schema is None:
            raise ValueError(f"Log type '{log_type}' not found in XML schema.")
        
        # Retrieve the type attribute from the log type element
        self.file_type = log_schema.get('type')
        
        # Convert the schema to a list of LogColumn objects
        for column in log_schema.findall('column'):
            name = column.get('name')
            datatype = column.get('datatype')
            isArray = column.get('array') == 'true'
            isPrimary = column.get('primaryKey') == 'true'
            data_format = column.get('format')
            
            self.log_schema.append(LogColumn(name, datatype, isArray, isPrimary, data_format))

    def insert_log_file(self, file_path: str, max_workers: int = 10) -> None:
        if file_path.endswith('.gz'):
            open_func = gzip.open
            mode = 'rt'  # Read text mode for gzip
        else:
            open_func = open
            mode = 'r'

        with open_func(file_path, mode) as file:
            lines = file.readlines()
        
        # Skip the header line
        lines = lines[1:]
        
        # # ## for testing 
        # k = 0
        # newlines = lines.copy()
        # while(len(newlines) < 30000):
        #     # print(len(newlines))
        #     for line in lines:
        #         newlines.append(f"{k}" + line)
        #         k += 1
        
        # lines = newlines
        # #### TEsting end here

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            with tqdm(total=len(lines), desc="Inserting log lines") as pbar:
                futures = {executor.submit(self.insert_log, line.strip()): line.strip() for line in lines if line.strip()}
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        line = futures[future]
                        applog.logger.warning(f"An error occurred while inserting the log line {line}: {e}")
                    finally:
                        pbar.update(1)
    
    def insert_log_files(self, log_files: List[str], workers:int = 10) -> None:
        for log_file in log_files:
            self.insert_log_file(log_file, workers)
            applog.logger.debug(f"Log file {log_file} inserted successfully.")
        

    def parse_log_line(self, log_line: str) -> Tuple:
        try:
            f = StringIO(log_line)

            if self.file_type == "tsv":
                reader = csv.reader(f, delimiter='\t')
                fields = next(reader)

                # Use list comprehension to process fields
                processed_fields = [
                    field.split(',') if ',' in field else field for field in fields
                ]
            elif self.file_type == "csv":
                reader = csv.reader(f)
                fields = next(reader)
                processed_fields = fields
            else:
                raise ValueError(f"Log file type '{type}' not supported.")
        except Exception as e:
            applog.logger.warning(f"Error parsing file: {e}")
            return None
        return tuple(processed_fields)

    def __repr__(self) -> str:
        str_logparser = f"Database: {self.database}\n"
        str_logparser += f"Log Schema:\n"
        for column in self.log_schema:
            str_logparser += f"{column}\n"
        str_logparser += f"File Type: {self.file_type}"
        return str_logparser
            
class BasicLogParser(LogParser):

    def __init__(self, database: DatabaseConnectionPool, main_table:str, *args, **kwargs):
        super().__init__(database, *args, **kwargs)
        self.main_table = main_table
        self.table_schema: List[TableColumn] = []

    def load_log_schema(self, log_type: str, xml_file: str) -> None:
        super().load_log_schema(log_type, xml_file)

        self.set_table_schema()
        for col in self.table_schema:
            applog.logger.debug(col)

    def set_table_schema(self) -> None:
        self.table_schema = [TableColumn(column.name, column.datatype, column.isArray, column.isPrimary, column.data_format) for column in self.log_schema]
    
    def create_tables(self) -> None:
        self.database.create_table(self.main_table, self.table_schema)

    def insert_log(self, log_line: str) -> None:
        # Parse the TSV string
        try:
            log_data = self.parse_log_line(log_line)
        except Exception as e:
            applog.logger.warning(f"Error parsing TSV: {e}")
            return None
        try:
            # Insert log entry
            self.database.insert_data(self.main_table, self.table_schema, log_data)
        except Exception as e:
            applog.logger.warning(f"Error inserting log: {e}")

    def __repr__(self) -> str:
        str_basiclogparser = super().__repr__()
        str_basiclogparser += f"\nMain Table: {self.main_table}"
        str_basiclogparser += "\nTable Schema:\n"
        for column in self.table_schema:
            str_basiclogparser += f"{column}\n"
        return str_basiclogparser
    

# Look up feature class
class LogParserWithLookup(LogParser):

    def __init__(self, database: DatabaseConnectionPool, main_table:str, lookup_columns_name: List[str], *args, **kwargs):
        super().__init__(database, *args, **kwargs)
        self.main_table = main_table # main table name
        self.lookup_columns: List[LogColumn] = [] # contains all lookup column obj
        self.lookuptables: Dict[str, dict] = {} # all lookup table stored as dict of dict
        self.main_table_schema: List[TableColumn] = [] # main table schema
        self.lookuptable_schema: List[TableColumn] = [] # lookup table schema
        self.columnindex: Dict[str, int] = {} # lookup log column index as dict
        self.initialise_columnindex_with_null(lookup_columns_name)

    def initialise_columnindex_with_null(self, lookup_columns_name: List[str]):
        for colname in lookup_columns_name:
            self.columnindex[colname] = None

    def initialise_lookup(self):
        self.set_lookuptable_schema()
        self.initialise_logcolumn_index_and_logcolumn()
        self.initialise_lookuptables()

    def set_lookuptable_schema(self):
        self.lookuptable_schema = [TableColumn( name = "key", datatype = "INTEGER", isArray = False, isPrimary = True), TableColumn(name = "value", datatype = "TEXT", isArray = False, isPrimary = False)]

    # initialise single lookup table for a column
    def initialise_single_lookuptable(self, single_log_column: LogColumn):
        tables_name = self.database.fetch_table_names()

        # if single_log_column.name not in tables_name:
        if "lookuptable" not in tables_name:
            # creating new lookup table
            # self.database.create_table(single_log_column.name, self.lookuptable_schema)
            self.database.create_table("lookuptable", self.lookuptable_schema)
            lookupdict = {}
        else:
            # lookupdict: dict = self.database.fetch_lookup_table_as_dict(single_log_column.name, self.lookuptable_schema)
            lookupdict: dict = self.database.fetch_lookup_table_as_dict("lookuptable", self.lookuptable_schema)

        return lookupdict
    
    # initialise all lookup table and store it as dict of dict
    def initialise_lookuptables(self):
        for lookup_log_col in self.lookup_columns:
            # self.lookuptables[lookup_log_col.name] = self.initialise_single_lookuptable(lookup_log_col)
            self.lookuptables["lookuptable"] = self.initialise_single_lookuptable(lookup_log_col)
        return

    # get log column index and col
    def get_logcolumn_index_and_col(self, column_name: str):
        for i, col in enumerate(self.log_schema):
            if(col.name == column_name): return i, col
        
        return -1, None

    # initialise lookup columns index
    def initialise_logcolumn_index_and_logcolumn(self):

        for lookup_log_col_name in self.columnindex.keys():
            index, col = self.get_logcolumn_index_and_col(lookup_log_col_name)
            if index == -1: 
                raise ValueError("Column not found in the logcolumn")
            else:
                self.columnindex[lookup_log_col_name] = index
                self.lookup_columns.append(col)
        return
    
    def load_log_schema(self, log_type: str, xml_file: str) -> None:
        super().load_log_schema(log_type, xml_file)
        self.initialise_lookup()
        self.set_main_table_schema()
        for col in self.main_table_schema:
            applog.logger.debug(col)
    
    def set_main_table_schema(self) -> None:
        self.main_table_schema = [TableColumn(column.name, column.datatype, column.isArray, column.isPrimary, column.data_format) if column.name not in self.columnindex.keys() else TableColumn(column.name, "INTEGER" if not column.isArray else "TEXT", not column.isArray, column.isPrimary, column.data_format) for column in self.log_schema]

    # handles single column and convert it to required type
    def update_single_column(self, value: Union[str, list], log_col: LogColumn):

        # col_dict: dict = self.lookuptables[log_col.name]
        col_dict: dict = self.lookuptables["lookuptable"]
        if type(value) == str and not log_col.isArray:
            if value in col_dict.keys():
                return col_dict[value]
            else:
                # self.lookuptables[log_col.name][value] = len(col_dict)
                self.lookuptables["lookuptable"][value] = len(col_dict)
                return len(col_dict)
        elif log_col.isArray:
            if type(value) == str:
                if value in col_dict.keys():
                    return str(col_dict[value])
                else:
                    # self.lookuptables[log_col.name][value] = len(col_dict)
                    self.lookuptables["lookuptable"][value] = len(col_dict)
                    return str(len(col_dict))
                    
            else:
                list_as_str_with_comma: str = ""
                for val in value:
                    # col_dict: dict = self.lookuptables[log_col.name]
                    col_dict: dict = self.lookuptables["lookuptable"]
                    if val in col_dict.keys():
                        list_as_str_with_comma += (str(col_dict[val]) + ",")
                    else:
                        # self.lookuptables[log_col.name][val] = len(col_dict)
                        self.lookuptables["lookuptable"][val] = len(col_dict)
                        list_as_str_with_comma += (str(len(col_dict)) + ",")
                
                return list_as_str_with_comma[:-1] # leave last comma
        else:
            raise ValueError("Wrong format for the log column in the value")
            
            
    # change all column to required datatype to storage for a single line
    def update_log_line(self, log_line: tuple):

        log_line_list = list(log_line)
        for idx, (_, col_index) in enumerate(self.columnindex.items()):
            log_line_list[col_index] = self.update_single_column(log_line_list[col_index], self.lookup_columns[idx])

        return tuple(log_line_list)
    
    
    def insert_log(self, log_line: str) -> None:
        
        # Parse the TSV string
        try:
            log_data = self.parse_log_line(log_line)

            # Change input columns to INTEGER 
            log_data = self.update_log_line(log_data)
            
        except Exception as e:
            applog.logger.warning(f"Error parsing TSV: {e}")
            return None
        try:
            # Insert log entry
            self.database.insert_data(self.main_table, self.main_table_schema, log_data)
        except Exception as e:
            applog.logger.warning(f"Error inserting log: {e}")

    def create_tables(self) -> None:
        self.database.create_table(self.main_table, self.main_table_schema)
    
    def insert_log_files(self, log_files: List[str], workers:int = 10) -> None:

        super().insert_log_files(log_files, workers)
        for col_name, col_dict in self.lookuptables.items():
            for key, value in col_dict.items():
                self.database.insert_data(col_name, self.lookuptable_schema, (value, key))

        # print(self.database.print_table_content("extended_logs"))
        return
    
    def __repr__(self) -> str:
        str_LogParserWithLookup = super().__repr__()
        str_LogParserWithLookup += f"\nMain Table: {self.main_table}"
        str_LogParserWithLookup += "\nTable Schema:\n"
        for column in self.table_schema:
            str_LogParserWithLookup += f"{column}\n"
        return str_LogParserWithLookup

    

    