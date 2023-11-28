from abc import ABC, abstractmethod
import requests
from requests import HTTPError
import json
import base64
import logging
logger = logging.getLogger()
from .Schema import BaseTableSchema


def afas_filter_constructor(filters: list) -> json:
    operator_codes = {'=' : '1', '>=':'2',  '<=':'3', '>': '4', '<':'5', '!=':'7'}
    base_filterjson = {"Filters": {"Filter":[]}}
    
    for i, filter in enumerate(filters):
        filterblank = { "@FilterId": f'Filter {i+1}', "Field" : []}
        for fieldfilter in filter:
            fieldblank = {"@FieldId" : fieldfilter['id'], 
                            "@OperatorType": operator_codes.get(fieldfilter['operator'], "1")
                            ,"#text": fieldfilter['value']
                            }
            filterblank['Field'].append(fieldblank) 

        base_filterjson['Filters']['Filter'].append(filterblank)   
    return(json.dumps(base_filterjson))



class AfasConnection(ABC):

    def __init__(self, user: str, connector: str, token: str) -> None:
        token_base64 = base64.b64encode(bytes(token, "ascii")).decode("ascii")
        self.token = f'AfasToken {token_base64}'
        self.user = user
        self.connector = connector
        
    
    def get(self, skip: int = None, take: int = None, filter: json = None): 

        parameters = {'skip': str(skip), 'take': str(take)}
        if filter is not None:
            parameters['filterjson'] = filter

        try:
            response = requests.get(
                self.url,
                headers={'Authorization': self.token},
                params= parameters
            )
            response.raise_for_status()
        except HTTPError as http_err:
            logger.error(f' HTTP error: {http_err}')
        except requests.exceptions.ConnectionError as errc:
            logger.error(f"Error Connecting: {errc}")
        except requests.exceptions.Timeout as errt:
            logger.error(f"Timeout Error: {errt}")
        except requests.exceptions.RequestException as err:
            logger.error(f"Other error in request: {err}")
        except Exception as e:
            logger.error(f'Error: {e}')

        else:
            return(response.json())




class AfasMetadataConnection(AfasConnection): 

    def __init__(self, user: str, connector: str, token: str) -> None:
        super().__init__(user, connector, token)
        self.url = f"https://{self.user}.rest.afas.online/ProfitRestServices/metainfo/get/{self.connector}"


class AfasDataConnection(AfasConnection): 

    def __init__(self, user: str, connector: str, token: str) -> None:
        super().__init__(user, connector, token)
        self.url = f"https://{self.user}.rest.afas.online/ProfitRestServices/connectors/{self.connector}"


    def get(self, skip: int = None, take: int = None, filter: json = None):
        intermediate = super().get(skip, take, filter)
        return intermediate['rows']





class AfasTableSchema(BaseTableSchema): 

    def proces_column(column):
        fd_id = column['id'].strip().replace(' ', '_')
        fd_type = column['dataType']
        length =column['length'] 
        decimals =  column['decimals']
        columndef = {}

        columndef['column_name'] = fd_id
        # alle potentieel onnodige elementen vullen met lege string
        columndef['length'] = columndef['precision'] = columndef['scale']  = columndef['default'] = columndef['nullable'] = ''

        if fd_id == 'AantalUren' and fd_type == 'date':
            columndef['data_type'] = 'int'
            
        if fd_type == 'string':
            columndef['data_type'] = 'nvarchar'
            columndef['length'] = length
        elif fd_type == 'int':
            columndef['data_type'] = 'int'
        elif fd_type == 'decimal':
            columndef['data_type'] = 'decimal'
            columndef['precision'] = length
            columndef['scale'] = decimals
        elif fd_type == 'date':
            columndef['data_type'] = 'datetime'      
        elif fd_type == 'boolean':
            columndef['data_type'] = 'nvarchar'
            columndef['length'] = 10

        else:
            raise Exception(f'Data type not implemented: {fd_type}')

        return columndef
  
    @classmethod
    def create_from_metadata(cls, connection: AfasMetadataConnection):
        raw_data = connection.get()

        table_name = raw_data.get('name')
        print(raw_data['fields'])

        table_schema = [cls.proces_column(field) for field in raw_data['fields']]


        return  cls(table_name = table_name, table_schema = table_schema)






    



