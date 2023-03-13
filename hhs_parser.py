import datetime
import json
import os
import shutil
import time
from csv import reader
from zipfile import ZipFile

import requests
import wget
from bs4 import BeautifulSoup
import lxml

headers = ['number', 'enumeration_type', 'basic.replacement_npi', 'basic.ein', 'basic.organization_name',
           'basic.last_name', 'basic.first_name', 'basic.middle_name', 'basic.name_prefix', 'basic.name_suffix',
           'basic.credential', 'other.provider_name1', 'other.provider_name2', 'other.provider_name3',
           'other.provider_name4', 'other.provider_name5', 'other.provider_name6', 'other.provider_name7',
           'other.provider_name8', 'other.provider_name9', 'addresses[1].address_1', 'addresses[1].address_2',
           'addresses[1].city', 'addresses[1].state', 'addresses[1]. postal_code', 'addresses[1].country_code',
           'addresses[1].telephone_number', 'addresses[1]. fax_number',
           'addresses[0].address_1', 'addresses[0].address_2', 'addresses[0].city', 'addresses[0].state',
           'addresses[0]. postal_code', 'addresses[0].country_code', 'addresses[0].telephone_number',
           'addresses[0]. fax_number', 'basic.enumeration_date',
           'basic.last_updated', 'basic.deactivation_reason_code', 'basic.deactivation_date',
           'basic.reactivation_date', 'basic.gender', 'basic.authorized_official_last_name',
           'basic.authorized_official_first_name', 'basic.authorized_official_middle_name',
           'basic.authorized_official_title_or_position', 'basic.authorized_official_telephone_number',
           'taxonomies[0].code', 'taxonomies[0].licence', 'taxonomies[0].state', 'taxonomies[0].primary',
           'taxonomies[1].code', 'taxonomies[1].licence', 'taxonomies[1].state', 'taxonomies[1].primary',
           'taxonomies[2].code', 'taxonomies[2].licence', 'taxonomies[2].state', 'taxonomies[2].primary',
           'taxonomies[3].code', 'taxonomies[3].licence', 'taxonomies[3].state', 'taxonomies[3].primary',
           'taxonomies[4].code', 'taxonomies[4].licence', 'taxonomies[4].state', 'taxonomies[4].primary',
           'taxonomies[5].code', 'taxonomies[5].licence', 'taxonomies[5].state', 'taxonomies[5].primary',
           'taxonomies[6].code', 'taxonomies[6].licence', 'taxonomies[6].state', 'taxonomies[6].primary',
           'taxonomies[7].code', 'taxonomies[7].licence', 'taxonomies[7].state', 'taxonomies[7].primary',
           'taxonomies[8].code', 'taxonomies[8].licence', 'taxonomies[8].state', 'taxonomies[8].primary',
           'taxonomies[9].code', 'taxonomies[9].licence', 'taxonomies[9].state', 'taxonomies[9].primary',
           'taxonomies[10].code', 'taxonomies[10].licence', 'taxonomies[10].state', 'taxonomies[10].primary',
           'taxonomies[11].code', 'taxonomies[11].licence', 'taxonomies[11].state', 'taxonomies[11].primary',
           'taxonomies[12].code', 'taxonomies[12].licence', 'taxonomies[12].state', 'taxonomies[12].primary',
           'taxonomies[13].code', 'taxonomies[13].licence', 'taxonomies[13].state', 'taxonomies[13].primary',
           'taxonomies[14].code', 'taxonomies[14].licence', 'taxonomies[14].state', 'taxonomies[14].primary',
           'identifiers[0].identifier', 'identifiers[0].code', 'identifiers[0].state', 'identifiers[0].issuer',
           'identifiers[1].identifier', 'identifiers[1].code', 'identifiers[1].state', 'identifiers[1].issuer',
           'identifiers[2].identifier', 'identifiers[2].code', 'identifiers[2].state', 'identifiers[2].issuer',
           'identifiers[3].identifier', 'identifiers[3].code', 'identifiers[3].state', 'identifiers[3].issuer',
           'identifiers[4].identifier', 'identifiers[4].code', 'identifiers[4].state', 'identifiers[4].issuer',
           'identifiers[5].identifier', 'identifiers[5].code', 'identifiers[5].state', 'identifiers[5].issuer',
           'identifiers[6].identifier', 'identifiers[6].code', 'identifiers[6].state', 'identifiers[6].issuer',
           'identifiers[7].identifier', 'identifiers[7].code', 'identifiers[7].state', 'identifiers[7].issuer',
           'identifiers[8].identifier', 'identifiers[8].code', 'identifiers[8].state', 'identifiers[8].issuer',
           'identifiers[9].identifier', 'identifiers[9].code', 'identifiers[9].state', 'identifiers[9].issuer',
           'identifiers[10].identifier', 'identifiers[10].code', 'identifiers[10].state', 'identifiers[10].issuer',
           'identifiers[11].identifier', 'identifiers[11].code', 'identifiers[11].state', 'identifiers[11].issuer',
           'identifiers[12].identifier', 'identifiers[12].code', 'identifiers[12].state', 'identifiers[12].issuer',
           'identifiers[13].identifier', 'identifiers[13].code', 'identifiers[13].state', 'identifiers[13].issuer',
           'identifiers[14].identifier', 'identifiers[14].code', 'identifiers[14].state', 'identifiers[14].issuer',
           'identifiers[15].identifier', 'identifiers[15].code', 'identifiers[15].state', 'identifiers[15].issuer',
           'identifiers[16].identifier', 'identifiers[16].code', 'identifiers[16].state', 'identifiers[16].issuer',
           'identifiers[17].identifier', 'identifiers[17].code', 'identifiers[17].state', 'identifiers[17].issuer',
           'identifiers[18].identifier', 'identifiers[18].code', 'identifiers[18].state', 'identifiers[18].issuer',
           'identifiers[19].identifier', 'identifiers[19].code', 'identifiers[19].state', 'identifiers[19].issuer',
           'identifiers[20].identifier', 'identifiers[20].code', 'identifiers[20].state', 'identifiers[20].issuer',
           'identifiers[21].identifier', 'identifiers[21].code', 'identifiers[21].state', 'identifiers[21].issuer',
           'identifiers[22].identifier', 'identifiers[22].code', 'identifiers[22].state', 'identifiers[22].issuer',
           'identifiers[23].identifier', 'identifiers[23].code', 'identifiers[23].state', 'identifiers[23].issuer',
           'identifiers[24].identifier', 'identifiers[24].code', 'identifiers[24].state', 'identifiers[24].issuer',
           'identifiers[25].identifier', 'identifiers[25].code', 'identifiers[25].state', 'identifiers[25].issuer',
           'identifiers[26].identifier', 'identifiers[26].code', 'identifiers[26].state', 'identifiers[26].issuer',
           'identifiers[27].identifier', 'identifiers[27].code', 'identifiers[27].state', 'identifiers[27].issuer',
           'identifiers[28].identifier', 'identifiers[28].code', 'identifiers[28].state', 'identifiers[28].issuer',
           'identifiers[29].identifier', 'identifiers[29].code', 'identifiers[29].state', 'identifiers[29].issuer',
           'identifiers[30].identifier', 'identifiers[30].code', 'identifiers[30].state', 'identifiers[30].issuer',
           'identifiers[31].identifier', 'identifiers[31].code', 'identifiers[31].state', 'identifiers[31].issuer',
           'identifiers[32].identifier', 'identifiers[32].code', 'identifiers[32].state', 'identifiers[32].issuer',
           'identifiers[33].identifier', 'identifiers[33].code', 'identifiers[33].state', 'identifiers[33].issuer',
           'identifiers[34].identifier', 'identifiers[34].code', 'identifiers[34].state', 'identifiers[34].issuer',
           'identifiers[35].identifier', 'identifiers[35].code', 'identifiers[35].state', 'identifiers[35].issuer',
           'identifiers[36].identifier', 'identifiers[36].code', 'identifiers[36].state', 'identifiers[36].issuer',
           'identifiers[37].identifier', 'identifiers[37].code', 'identifiers[37].state', 'identifiers[37].issuer',
           'identifiers[38].identifier', 'identifiers[38].code', 'identifiers[38].state', 'identifiers[38].issuer',
           'identifiers[39].identifier', 'identifiers[39].code', 'identifiers[39].state', 'identifiers[39].issuer',
           'identifiers[40].identifier', 'identifiers[40].code', 'identifiers[40].state', 'identifiers[40].issuer',
           'identifiers[41].identifier', 'identifiers[41].code', 'identifiers[41].state', 'identifiers[41].issuer',
           'identifiers[42].identifier', 'identifiers[42].code', 'identifiers[42].state', 'identifiers[42].issuer',
           'identifiers[43].identifier', 'identifiers[43].code', 'identifiers[43].state', 'identifiers[43].issuer',
           'identifiers[44].identifier', 'identifiers[44].code', 'identifiers[44].state', 'identifiers[44].issuer',
           'identifiers[45].identifier', 'identifiers[45].code', 'identifiers[45].state', 'identifiers[45].issuer',
           'identifiers[46].identifier', 'identifiers[46].code', 'identifiers[46].state', 'identifiers[46].issuer',
           'identifiers[47].identifier', 'identifiers[47].code', 'identifiers[47].state', 'identifiers[47].issuer',
           'identifiers[48].identifier', 'identifiers[48].code', 'identifiers[48].state', 'identifiers[48].issuer',
           'identifiers[49].identifier', 'identifiers[49].code', 'identifiers[49].state', 'identifiers[49].issuer',
           'basic.sole_proprietor', 'basic.organizational_subpart', 'basic.parent_organization_legal_business_name',
           'basic.parent_organization_ein', 'basic.authorized_official_name_prefix',
           'basic.authorized_official_name_suffix', 'basic.authorized_official_credential',
           'taxonomies[0].taxonomy_group', 'taxonomies[1].taxonomy_group',
           'taxonomies[2].taxonomy_group', 'taxonomies[3].taxonomy_group', 'taxonomies[4].taxonomy_group',
           'taxonomies[5].taxonomy_group', 'taxonomies[6].taxonomy_group', 'taxonomies[7].taxonomy_group',
           'taxonomies[8].taxonomy_group', 'taxonomies[9].taxonomy_group', 'taxonomies[10].taxonomy_group',
           'taxonomies[11].taxonomy_group', 'taxonomies[12].taxonomy_group', 'taxonomies[13].taxonomy_group',
           'taxonomies[14].taxonomy_group', 'basic.certification_date']


def download_file(url, file_name):
    try:
        wget.download(url, file_name)
    except:
        download_file(url, file_name)


def delete_directory(path_to_directory):
    if os.path.exists(path_to_directory):
        shutil.rmtree(path_to_directory)
    else:
        print("Directory does not exist")


def create_directory(path_to_dir, name):
    mypath = f'{path_to_dir}/{name}'
    if not os.path.isdir(mypath):
        os.makedirs(mypath)


def get_all_files(nppes_zips_col_url):
    nppes_download_page = requests.get('https://download.cms.gov/nppes/NPI_Files.html').text
    soup = BeautifulSoup(nppes_download_page, 'lxml')
    files_block = soup.find_all('div', class_='bulletlistleft')
    monthly_file_block = files_block[3]
    monthly_file = monthly_file_block.find('a').get('href').lstrip('./')
    week_files_block = files_block[5]
    week_files = [a.get('href').lstrip('./') for a in week_files_block.find_all('a')]
    files = week_files + [monthly_file]
    need_files = []
    for file in files:
        response = requests.get(
            f'{nppes_zips_col_url}?name={file}')
        if response.status_code == 200:
            response_json = json.loads(response.text)
            is_in_database = False if response_json.get('total') == 0 else True
            if not is_in_database:
                need_files.append(file)
    return need_files


def create_exec_str_1(key, value):
    key = key.replace('[', '_').replace(']', '')
    splited_str = key.split(".")
    key_string = ''
    for m in splited_str:
        key_string += f"['{m}']"
    full_exec_str = f"{key_string} = \"{str(value)}\""
    return full_exec_str


def create_exec_str_2(key, value):
    key = key.replace('[', '_').replace(']', '')
    splited_str = key.split(".")
    key_string = '{'
    for index, m in enumerate(splited_str):
        if (index + 1) == len(splited_str):
            key_string += f"'{m}'" + ':'
        else:
            key_string += f"'{m}'" + ':{'
    full_exec_str = '.update(' + key_string + f"\"{str(value)}\"" + '}' * len(splited_str) + ')'
    return full_exec_str


def upload_hhs_data(nppes_file, nppes_data_col_url, nppes_zips_col_url):
    current_directory = os.getcwd()
    directory_name = 'hhs'
    path_to_directory = f'{current_directory}/{directory_name}'
    delete_directory(path_to_directory)
    create_directory(current_directory, directory_name)
    file_to_download = f'https://download.cms.gov/nppes/{nppes_file}'
    path_to_zip = f'{path_to_directory}/{nppes_file}'
    download_file(file_to_download, path_to_zip)
    with ZipFile(path_to_zip, 'r') as zip:
        zip_files = zip.namelist()
        file_name = ''
        for file in zip_files:
            if ('npidata_pfile' in file) and ('fileheader' not in file):
                file_name = file
        zip.extract(file_name, path=path_to_directory, pwd=None)
        with open(f'{path_to_directory}/{file_name}', 'r') as read_obj:
            csv_reader = reader(read_obj)
            for row in csv_reader:
                response = requests.get(
                    f'{nppes_data_col_url}?number={row[0]}')
                if response.status_code == 200:
                    response_json = json.loads(response.text)
                    is_in_database = False if response_json.get('total') == 0 else True
                    if not is_in_database:
                        if row[39] == '':
                            npi_data = {}
                            for index, block in enumerate(row):
                                if block != '':
                                    exec_str1 = f"npi_data{create_exec_str_1(headers[index], str(block))}"
                                    exec_str2 = f"npi_data{create_exec_str_2(headers[index], str(block))}"
                                    try:
                                        exec(exec_str1)
                                    except KeyError:
                                        exec(exec_str2)
                            requests.post(nppes_data_col_url, json=npi_data)
    requests.post(nppes_zips_col_url, json={'name': nppes_file})
    delete_directory(path_to_directory)


if __name__ == '__main__':
    nppes_data_col_link = 'http://62.216.33.167:21005/api/nppes_data'
    nppes_zips_col_link = 'http://62.216.33.167:21005/api/downloaded_zips'
    while True:
        start_time = time.time()
        for file in get_all_files(nppes_zips_col_link):
            print(file)
            upload_hhs_data(file, nppes_zips_col_link, nppes_zips_col_link)
        work_time = int(time.time() - start_time)
        print(work_time)
        print(14400 - work_time)
        time.sleep(14400 - work_time)
