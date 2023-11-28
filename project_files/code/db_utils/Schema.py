
import pyodbc
from typing import Protocol
from abc import ABC, abstractmethod
import logging

logger = logging.getLogger()


"""
Object dat de definitie van een tabel kan opslaan. Bedoeld als interface
tussen verschillende bronnen. 
"""

class SchemaProtocol(Protocol):
    table_name: str
    table_schema: list


class BaseTableSchema(ABC):

    required_column_elements = ['column_name','nullable', 'data_type', 'length', 'precision', 'scale', 'default',]  

    def __init__(self, table_name: str = None, table_schema: list = None) -> None:

        self.table_name = table_name

        for column in table_schema:
            missing_list = [e for e in self.required_column_elements if e not in column.keys()]
            if len(missing_list) > 0:
                raise Exception(f'not all required elements are present in table schema {missing_list}')
            else: self.table_schema = table_schema

    def __str__(self):
        return f'{self.table_name}: {self.table_schema}' 


class SQLTableSchema(BaseTableSchema):
        
    # generate sql column definition from data in information schema table
    @staticmethod
    def proces_columns(column) -> dict: 

        def strip_hooks(string: str):
            while True:
                if string[0] != '(':
                    return string
                string = string[1:-1]

        column_elements ={}
        column_elements['column_name'] = (column['COLUMN_NAME'])
        if column['IS_NULLABLE'] == 'YES':
            column_elements['nullable'] = ''
        else:
            column_elements['nullable'] = 'not null'
        column_elements['data_type'] = column['DATA_TYPE']
        column_elements['length'] = column['CHARACTER_MAXIMUM_LENGTH']
        column_elements['precision'] = column['NUMERIC_PRECISION']
        column_elements['scale'] = column['NUMERIC_SCALE']
        if column['COLUMN_DEFAULT']:
            default_value = strip_hooks(column['COLUMN_DEFAULT'])
            column_elements['default'] = f'default  {default_value}' 
        else:
            column_elements['default'] = ''

        return column_elements   


    @classmethod
    def from_connection(cls, connection: str, schema: str, table:str):
        querystring = """
                select c.* from information_schema.columns as c 
                    where c.table_name = ?
                    and c.TABLE_SCHEMA = ?
                    order by c.ORDINAL_POSITION
            """    
        with pyodbc.connect(connection) as conn:
            cursor = conn.cursor()
            cursor.execute(querystring, table, schema )
            res = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            logger.debug(f'queryresult from schema fetch: {columns}')
            column_list_raw = [dict(zip(columns, x)) for x in res]
            table_schema = []
            for column in column_list_raw:
                table_schema.append(cls.proces_columns(column))

        # make instance from output column definition
        return cls(table_name = table, table_schema = table_schema) 




#schema = SQLTableSchema.from_query(connection=CONNECTION, schema='dbo', table='Sessie')


