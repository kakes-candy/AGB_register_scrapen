
from time import sleep
from datetime import datetime
import json
from config.logging_config import log_config
import logging
import xml.etree.ElementTree as ET
from code.scraping import get_specialist_details
from code.processing import process_details
from code.database_actions import (get_cases, write_results )




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

    timestamp = datetime.now()

    rootlogger.info('getting cases')
    # get cases to query in the vektis source
    cases = get_cases(connection=connectie_replicatie, sqlfile_path='project_files\code\get_cases.sql')

    rootlogger.info(f'got {len(cases[0])} cases')

    #store results in a list of dicts
    results = []

    for case in cases[0]:

        sleep(4)
        row = {}
        row['timestamp_check'] = timestamp
        row['agbcode'] = case['AgbCode']
        row['specialist_id'] = case['specialist_id']

        rootlogger.info(f"getting vektis info for {case['AgbCode']}")

        # get the data from vektis
        html = get_specialist_details(agbcode=case['AgbCode'])

        rootlogger.info(f"retrieve complete")
        #process and convert to json
        processed = process_details(html=html)
        processed_json = json.dumps(processed, indent = 4)
        rootlogger.debug(processed_json)

        row['result']  = processed_json
        rootlogger.info(f"processing complete")

        results.append(row)


    
    write_results(connection=connectie_lookup, schema = 'dbo', target_table_name='agb_register_gegevens', data=results)
        