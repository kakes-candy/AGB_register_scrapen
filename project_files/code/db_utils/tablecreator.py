import pyodbc
import logging
from abc import ABC, abstractmethod
from code.db_utils.Schema import BaseTableSchema

logger = logging.getLogger()



""" 
Object dat een tabel kan aanmaken op basis van een schema definitie (Schema)
 """

class BaseTableCreator(ABC):

    def __init__(self, connection, tableschema: type[BaseTableSchema], target_schema: str, target_prefix: str = None)  -> None:
        self.connection = connection
        self.tableschema = tableschema.table_schema
        self.target_schema = target_schema
        self.target_prefix = target_prefix
        self.target_tablename = f'{self.target_prefix or ""}{tableschema.table_name}' 

    @abstractmethod
    def check_if_exists(connection, tablename):
        pass
    
    @abstractmethod
    def create_sql_column(col: dict) -> str:
        pass
    
    @abstractmethod
    def create_sql_table_statement(self):
        pass

    @abstractmethod
    def create_table(self, drop = None):
        pass


class SqlServerTableCreator(BaseTableCreator):
 
    @staticmethod
    def check_if_exists(connection, tablename):

        querystring = f"SELECT * FROM sysobjects WHERE name='{tablename}' and xtype='U' "
        logger.debug(f'check if table exists using: {querystring}')

        with pyodbc.connect(connection) as conn:
            cursor = conn.cursor()
            cursor.execute(querystring)
            res = cursor.fetchone()
            if res and len(res) > 0:
                exists = True
            else:
                exists = False  
        conn.close()
        logger.debug(f'table exists check complete: {exists}')
        return(exists)

    @staticmethod
    def create_sql_column(col: dict) -> str:
        data_type = col['data_type']
        column_name = col['column_name']
        if col['length'] == -1:
            length = 'max'
        else: 
            length = col['length'] 
        nullable = col['nullable']
        scale = col['scale']
        precision = col['precision']
        default = col['default']

        if data_type in ('bigint', 'int', 'tinyint', 'datetime', 'datetime2', 'smalldatetime', 'bit', 'binary', 'uniqueidentifier'):
            column_def = f'{column_name} {data_type} {nullable} {default}'
        elif data_type == 'decimal':
            column_def = f'{column_name} decimal({precision}, {scale}) {nullable} {default}'
        elif data_type in ('nvarchar', 'nchar', 'char'):
            column_def = f'{column_name} {data_type}({length}) {nullable} {default}'  
        else:
            raise Exception(f'Data type not implemented: {data_type}')  
        return(column_def.strip())


    def create_sql_table_statement(self, add_time_column: bool):
        columnlist = [self.create_sql_column(x) for x in self.tableschema] 
        if add_time_column: 
            columnlist.append('updatetime datetime default CURRENT_TIMESTAMP')
        columnlist_joined = ', '.join(columnlist)
        sql_create_table_statement = f'create table {self.target_schema}.{self.target_tablename} ({columnlist_joined})'
        return sql_create_table_statement


    def create_table(self, drop=None, add_time_column: bool = None):
        if not add_time_column:
            add_time_column = False
        else:
            add_time_column = True
        statement = self.create_sql_table_statement(add_time_column = add_time_column) 
        logger.info(f'create table statement: {statement}')
        sql_drop_table_statement = f'drop table if exists {self.target_schema}.{self.target_tablename}'

        if self.check_if_exists(self.connection, self.target_tablename) and not drop:
            logger.info('table exists, but not dropping')
            return

        with pyodbc.connect(self.connection) as conn:
            cursor = conn.cursor()
            if drop:
                logger.info('dropping table if exists')
                logger.debug(f'drop table statement: {sql_drop_table_statement}')
                cursor.execute(sql_drop_table_statement)
                cursor.commit()
            logger.debug(f'create table statement: {statement}')
            cursor.execute(statement)
            cursor.commit()
        conn.close()
