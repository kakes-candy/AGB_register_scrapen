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