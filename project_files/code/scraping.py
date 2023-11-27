import requests



basis_url = 'https://www.vektis.nl/agb-register/zorgverlener-'


def get_specialist_details(agbcode: str) -> str:

    query_url = basis_url + agbcode
    data = requests.get(query_url).text
    return(data)
        


