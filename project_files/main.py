
from time import sleep
import json
from config.logging_config import log_config
import logging
import xml.etree.ElementTree as ET
from code.scraping import get_specialist_details
from code.processing import process_details
from code.get_cases import get_cases



# -------------------------------------------------------------------------------------------------------
# LOGGING Initialiseren
# -------------------------------------------------------------------------------------------------------
logging.config.dictConfig(log_config)
rootlogger = logging.getLogger()
rootlogger.setLevel(logging.DEBUG)
log_filename = log_config.get('handlers').get('file').get('filename')

rootlogger.info(f"logger initialised, logfilename: {log_filename}")


# -------------------------------------------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------------------------------------------

# configuratue ophalen uit xml
tree_config = ET.parse("config.xml")
root_config = tree_config.getroot()

# connectie strings
targetomgeving = root_config.find("target").text.strip()
connectie_replicatie = root_config.find(f"omgevingen/{targetomgeving}/databases/replicatie/connectiestring").text.strip()
connectie_lookup = root_config.find(f"omgevingen/{targetomgeving}/databases/lookup/connectiestring").text.strip()

#ontvangers van email uit de config halen
mail_ontvangers = [x.text.strip() for x in root_config.findall(f"omgevingen/{targetomgeving}/notificaties/ontvangers/mailadres")] 

rootlogger.debug(f'configuraties uitgelezen. Targetomgeving: {targetomgeving}, source connectie: {connectie_replicatie}, lookup connectie: {connectie_lookup}, emailontvangers: {",".join(mail_ontvangers)},')









if __name__ == '__main__':

    cases = get_cases(connection=connectie_replicatie, sqlfile_path='project_files\code\get_cases.sql')


    for case in cases[0][:2]:
        sleep(4)
        agbcode = case['AgbCode']
        specialist_id = case['specialist_id']

        html = get_specialist_details(agbcode=agbcode)

        processed = process_details(html=html)

        with open(f'./project_files/data/details_{agbcode}.json', 'w', encoding='utf8') as f:
            json.dump(processed, f, indent = 4)