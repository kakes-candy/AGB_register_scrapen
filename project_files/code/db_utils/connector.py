import logging
import base64
import json
import requests
from requests.exceptions import HTTPError
from csv import DictWriter

logger = logging.getLogger()


class AfasConnector:

    def __init__(self, user: str, connectorname : str):
        self.user = user
        self.connectorname = connectorname
        self.metainfo_url = f"https://{self.user}.rest.afas.online/ProfitRestServices/metainfo/get/{self.connectorname}"
        self.data_baseurl = f"https://{self.user}.rest.afas.online/ProfitRestServices/connectors/{self.connectorname}"

        logger.info(f'Connector created with name: {connectorname}, and user: {user}' )
        logger.debug(f'meta info url: {self.metainfo_url}')
        logger.debug(f'base url: {self.data_baseurl}')


    @staticmethod
    def urlencode(text):
        return requests.utils.quote(str(text))


    def filter_constructor(self, filters):
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


    # zet token xml (string) om tot base64 encoded string.
    def add_token(self, token : str): 
        logger.info(f"adding token string provided: {token}")
        token_base64 = base64.b64encode(bytes(token, "ascii")).decode("ascii")
        logger.debug(f"token string encoded: {token_base64}")
        token_base64_complete = f'AfasToken {token_base64}'
        self.token_base64 = token_base64_complete

    # Meta info ophalen en in de connector opslaan
    def get_metadata(self):
        logger.info('metainfo ophalen')
        try:
            response = requests.get(
                self.metainfo_url,
                headers={'Authorization': self.token_base64},
            )
            response.raise_for_status()
        except HTTPError as http_err:
            logger.error(f' HTTP error bij ophalen metainfo: {http_err}')
        except Exception as e:
            logger.error(f'Error bij ophalen metainfo : {e}')
        
        else:
            data = response.json()
            self.metadata = data
            logger.info('metadata opgehaald')
            logger.debug(f'metadata content: {json.dumps(self.metadata)}')

          

    def get_data_chunk(self, skip: int, take: int, filterjson: str = None):
        logger.info(f'getting lines {skip+1} to {skip + take}')

        parameters = {'skip': str(skip), 'take': str(take)}
        if filter is not None:
            parameters['filterjson'] = filterjson


        try:
            response = requests.get(
                self.data_baseurl,
                headers={'Authorization': self.token_base64},
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
            logger.error('Error: {e}')

        else:
            data = response.json()
            rijen = data['rows']
            logger.info(f'Rijen opgehaald: {len(rijen)}')
            return(rijen)
                    

    def get_data_generator(self, chunksize: int = None, limit: int = None, filterjson: str = None):

        logger.info(f'Data generator created. Chunksize: {str(chunksize)}, limit: {str(limit)}, filterjson: {json.dumps(filterjson)}')
        
        if not chunksize:
            chunksize = 10000
        if limit and chunksize > limit:
            chunksize = limit
        skip = 0    
        while True:
            # If there is a limit and it is reached, break
            if limit and skip >= limit:
                logger.info(f'limit of {limit} reached')
                break                
            result = self.get_data_chunk(skip = skip, take = chunksize, filterjson=filterjson)
            # check if the loop has to continue
            if result and len(result) > 0:
                # start at the end of last fetch
                skip+=chunksize
                # if the next fetch will exceed the limit ajust chunksize to 
                # match the limit in next fetch. 
                if limit and (skip + chunksize) > limit:
                    chunksize = limit - skip
                yield(result)
            else:
                break
    

    def write_to_csv(self, filename: str, generator):
        logger.info('data naar csv wegschrijven')
        # kolommen voor de csv
        if not self.metadata:
            raise Exception("No metadata found. Get it first using 'get_metadata()'")

        kolommen = [x.get('id') for x in self.metadata['fields']]

        data = generator

        with open(filename, 'w', newline='') as csvfile:
            writer = DictWriter(csvfile, fieldnames=kolommen, dialect='excel', delimiter=';')
            writer.writeheader() 

            for rowset in data:
                for row in rowset:
                    writer.writerow(row)    


