
import pyodbc
import logging
from abc import ABC, abstractmethod
#from code.Schema import SQLTableSchema, SchemaProtocol

logger = logging.getLogger()




class SqlServerDataWriter:

    def __init__(self, connection, tableschema, target_schema: str, target_prefix: str = None):
        self.tableschema = tableschema.table_schema
        self.target_schema = target_schema
        self.prefix = target_prefix
        self.object_name = tableschema.table_name
        self.target_tablename = f'{self.prefix or ""}{self.object_name}'
        self.connection = connection

    @staticmethod
    def safe_column_name(name):
        return(f'[{name}]')

    def sql_insert_dictionary(self, data):
        logger.info(f'inserting data into: {self.target_tablename}')
        columnlist = [self.safe_column_name(x['column_name']) for x in self.tableschema]

        columnlist_joined = ', '.join(columnlist)
        value_arguments = ', '.join(len(columnlist)*'?')
        sql_insert_into_statement = f'insert into {self.target_schema}.{self.target_tablename} ({columnlist_joined}) values ({value_arguments})'

        logger.debug(f'insert statement: {sql_insert_into_statement}')
    
        cleaned_data = []
        for row in data:
            rowlist = []
            for column in [x['column_name'] for x in self.tableschema]:
                rowlist.append(row.get(column))
            cleaned_data.append(rowlist)

        #logger.debug(f'cleaned data: {clean_data}')

        with pyodbc.connect(self.connection) as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True
            cursor.executemany(sql_insert_into_statement, cleaned_data)
        conn.close()


    def sql_insert_list(self, data):
        logger.info(f'inserting data into: {self.target_tablename}')
        columnlist = [self.safe_column_name(x['column_name']) for x in self.tableschema]

        columnlist_joined = ', '.join(columnlist)
        value_arguments = ', '.join(len(columnlist)*'?')
        sql_insert_into_statement = f'insert into {self.target_schema}.{self.target_tablename} ({columnlist_joined}) values ({value_arguments})'

        logger.debug(f'insert statement: {sql_insert_into_statement}')

        with pyodbc.connect(self.connection) as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True
            cursor.executemany(sql_insert_into_statement, data)
        conn.close()



    def sql_delete(self, filters = None):
        if filters:
            parts = []
            for part in filters:
                clauses = []
                for clause in part:
                    column = self.safe_column_name(clause['id'])
                    operator = clause['operator']
                    criterion =  clause['value']  
                    sql_clause = f'{column} {operator} \'{criterion}\'' 
                    clauses.append(sql_clause)
                complete_clauses = '(' + ' and '.join(clauses) + ')'
                parts.append(complete_clauses)
            complete_parts = ' or '.join(parts)

            sql_delete_statement = f'delete from {self.schema}.{self.tablename} where {complete_parts}'
        else:
            sql_delete_statement = f'delete from {self.schema}.{self.tablename}'

        logger.debug(f'deleting data with statement: {sql_delete_statement}')

        with pyodbc.connect(self.connection) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_delete_statement)
        conn.close()

