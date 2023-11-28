
import pyodbc
import logging

logger = logging.getLogger()


"""
Object om per blok van 5000 rijen data op te halen op basis van een query
"""

class SqlServerDataGenerator():
    
    def __init__(self, connection:str, querystring: str, arguments: set = None)  -> None:
        self.connection = connection 
        self.querystring = querystring
        self.args = arguments

    def get_generator(self):

        try: 
            with pyodbc.connect(self.connection) as conn:
                cursor = conn.cursor()
                cursor.execute(self.querystring)
                while True:
                    result = cursor.fetchmany(5000)
                    columns = [column[0] for column in cursor.description]
                    if result:
                        yield([dict(zip(columns, x)) for x in result])
                    else:
                        break
        except Exception as e:
            logger.error(e)

            
        conn.close()

