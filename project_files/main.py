
from time import sleep
import json
from code.scraping import get_specialist_details
from code.processing import process_details
from data.specialisten import specialisten




if __name__ == '__main__':


    for specialist in specialisten:
        sleep(4)
        agbcode = specialist.get('agbcode')
        html = get_specialist_details(agbcode=agbcode)

        processed = process_details(html=html)

        with open(f'./project_files/data/details_{agbcode}.json', 'w', encoding='utf8') as f:
            json.dump(processed, f, indent = 4)
