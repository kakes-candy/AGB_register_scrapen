import requests
import logging

logger = logging.getLogger()

basis_url = 'https://www.vektis.nl/agb-register/zorgverlener-'


def get_specialist_details(agbcode: str) -> str:

    query_url = basis_url + agbcode
    logger.debug(f'queryurl: {query_url}' )
    data = requests.get(query_url).text
    return(data)
        


