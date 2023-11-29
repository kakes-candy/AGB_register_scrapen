from code.db_utils import datagenerator
from code.db_utils.Schema import SQLTableSchema
from code.db_utils.datagenerator import SqlServerDataGenerator
from code.db_utils.databasewriter import SqlServerDataWriter
import logging

logger = logging.getLogger()


def get_cases(connection: str, sqlfile_path: str):

    # construct querystring to get all data from a source table
    with open(sqlfile_path, 'r') as sqlfile:
        querystring = sqlfile.read()

    generator = SqlServerDataGenerator(connection=connection, querystring=querystring)

    cases = list(generator.get_generator())

    return(cases)



def write_results(connection, schema, target_table_name, data):
    
    # Get the table definition
    target_table_schema = SQLTableSchema.from_connection(connection=connection, schema=schema, table=target_table_name)

    writer = SqlServerDataWriter(connection=connection, tableschema=target_table_schema, target_schema=schema)

    writer.sql_insert_dictionary(data)

