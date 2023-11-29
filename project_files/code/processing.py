from bs4 import BeautifulSoup
import json
import logging


logger = logging.getLogger()



def get_personal_info(soup) -> dict:

    pers_info = {}
    pers_info['error'] = None

    # check if there if the agb code was fount
    no_result =  soup.find_all(lambda tag:tag.name=="h1" and "Pagina niet gevonden" in tag.text) 
    no_result += (soup.find_all(lambda tag:tag.name=="p" and "Geen zorgverlener gevonden met de agbcode" in tag.text) )


    if len(no_result) > 0:
        logger.info('no result for this agb code')
        logger.debug(no_result)
        pers_info['error'] = 'Geen zorgverlener gevonden met de agbcode' 
        return(pers_info)

    dahboard_header = soup.find('div', class_= 'dashboard-header' )
    div_agb_code = dahboard_header.find('div', class_ = ['dashboard-header__agb-code', 'title', 'title--h2'])
    pers_info['agb_code'] = "".join(div_agb_code.find_all(string=True, recursive=False)).strip()


    dates_container = dahboard_header.find('div', class_ = 'dashboard-header__agb-code-dates')
    dates = dates_container.find_all('span')
    
    for date_tag in dates:
        text = date_tag.string
        if 'Start:' in text:
            pers_info['agb_start'] = text.replace('Start:', '').strip()
        elif 'Einde:' in text:
            pers_info['agb_eind'] = text.replace('Einde:', '').strip()
    

    info_columns = soup.find('div', class_= 'basic-info').find_all('div', class_ = 'basic-info__column')

    
    for column in info_columns:
        pairs = column.find_all('div', class_ = 'basic-info__pair')

        for pair in pairs:
            label = pair.find('div', class_ = 'data-stack__label').string
            value = pair.find('div', class_ = 'data-stack__value').string

            pers_info[label] = value

    return(pers_info)





def get_qualifications(soup, type_title: str) -> list:

    quali = []

    headers = soup.find_all('h3', class_= ['agb-subtitle','title'] )

    header = None
    for h in headers:
        if h.string ==  type_title :
            header = h
        
    if not header:
        return(None)
    next_element = header.find_parent().find_next_sibling()

    if next_element.has_attr('class'):
        if 'competence-list' in next_element['class']:
            competence_list = next_element
    else:
        return(None)
    

    for competence in competence_list.find_all('li'):
        label = competence.find('div', class_= 'competence').find('h4', class_ = 'title')
    
        dc = {}
        dc['label'] = label.string 

        pairs = competence.find_all('div', class_ = ['data-stack', 'competence__pair'])

        for pair in pairs:
            label = pair.find(class_ = 'data-stack__label').string
            value = pair.find(class_ = 'data-stack__value').string

            dc[label] = value

        quali.append(dc)

    return(quali)



def get_relations(soup, type_title: str) -> list:

    relations = []

    tables = soup.find_all('table', class_= ['card-table-table'] )

    table = None
    for t in tables:
        if not t.find('caption'):
            continue
        caption = t.find('caption').string
        if caption == type_title:
            table = t
        
    if not table:
        return(None)


    rows = table.find_all('tr')

    headers = []
    
    for row in rows:
        is_header = False
        if row.find_all('th'):
            is_header = True
            cells = row.find_all('th')
        else:    
            cells = row.find_all('td')
        
        row_value_list = []

        for cell in cells:

            #default value is empty string
            c = ''
            #waarde direct
            just_cell = cell.string
            # waarde in link
            in_link = cell.find('a', class_ = None)
            #waarde in span
            in_span = cell.find('span')

            if just_cell:
                c = just_cell.string
            elif in_link:
                c = in_link.string
            
            elif in_span:
                c = in_span.string


            row_value_list.append(c.strip())        

        if is_header:
            headers = row_value_list
        else:        
            relations.append(dict(zip(headers, row_value_list)))

    return(relations)



def process_details(html):

    soup = BeautifulSoup(html, 'lxml')

    prof = {}
    prof['pers_info'] = get_personal_info(soup=soup)  
    # if the agb code was not found on vektis return
    if prof['pers_info'].get('error'):
        return(prof)
    prof['erkenningen'] = get_qualifications(soup=soup, type_title='Mijn kwalificaties')
    prof['qualifications'] = get_qualifications(soup=soup, type_title='Mijn erkenningen')
    prof['works_at'] = get_relations(soup, type_title= 'Ik ben werkzaam bij de volgende vestigingen')
    prof['works_for'] = get_relations(soup, type_title='Ik heb een arbeidsrelatie met')

    return(prof)


