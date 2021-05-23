import urllib
import requests
from sqlalchemy.sql.functions import user
import urllib3
from bs4 import BeautifulSoup
import pandas as pd
from math import ceil
from datetime import date
import pyodbc
import db_passwords as cfg
import sqlalchemy
# URL = 'https://www.rightmove.co.uk/property-for-sale/find.html?searchType=SALE&locationIdentifier=REGION^93968&insId=1&radius=0.25&minPrice=450000&maxPrice=550000&minBedrooms=2&displayPropertyType=&maxDaysSinceAdded=&_includeSSTC=on&sortByPriceDescending=&primaryDisplayPropertyType=&secondaryDisplayPropertyType=&oldDisplayPropertyType=&oldPrimaryDisplayPropertyType=&newHome=&auction=false&index=288'


def get_page_count(http, payload):

    r = http.request(
        'GET', 'https://www.rightmove.co.uk/property-for-sale/find.html', fields=payload)

    soup = BeautifulSoup(r.data, 'html.parser')

    num_of_props = soup.find("span", "searchHeader-resultCount").text
    print("I have found {} properties: ".format(num_of_props))
    num_of_props = int(num_of_props)
    page_count = int(num_of_props / 24)
    if num_of_props % 24 > 0:
        page_count += 1

    if page_count > 42:
        page_count = 42

    print("This is {} pages".format(page_count))

    return page_count


def extract_from_api(save_to_disk, partial_data):
    page_index_num = 0
    http = urllib3.PoolManager()

    payload = {
        'searchType': 'SALE',
        'locationIdentifier': 'REGION^746',
        'insId': 1,
        'radius': 0,
        'minPrice': 450000,
        'maxPrice': 550000,
        'minBedrooms': 2,
        'maxDaysSinceAdded': '',
        'displayPropertyType': '',
        '_includeSSTC': 'on',
        'sortByPriceDescending': '',
        'primaryDisplayPropertyType': '',
        'secondaryDisplayPropertyType': '',
        'oldDisplayPropertyType': '',
        'oldPrimaryDisplayPropertyType': '',
        'newHome': '',
        'auction': 'false',
        'index': page_index_num,
    }

    total_pages = get_page_count(http, payload)

    property_list = []
    property_address = []
    property_price = []
    property_link = []
    key_val = []

    if partial_data:
        total_pages = 1

    for page in range(total_pages):
        # for page in range(3):
        page_index_num = 24 * page
        print("Page: {} - Index I will use is: {}".format(page, page_index_num))

        payload["index"] = page_index_num

        r = http.request(
            'GET', 'https://www.rightmove.co.uk/property-for-sale/find.html', fields=payload)
        # First of all we get the propery names.
        soup = BeautifulSoup(r.data, 'html.parser')

        for prop_name in soup.find_all("h2", "propertyCard-title"):
            clean_prop_name = prop_name.text.strip()
            print(clean_prop_name)
            if clean_prop_name == 'Property':
                print("I'm breaking because the prop name is blank")
                break
            property_list.append(clean_prop_name)

        for prop_address in soup.find_all("address", "propertyCard-address"):
            clean_prop_address = prop_address.text.strip()
            if clean_prop_address == '':
                break
            property_address.append(clean_prop_address)

        for prop_price in soup.find_all("div", "propertyCard-priceValue"):
            clean_prop_price = (prop_price.text.strip().replace(',', ''))
            # Removes the POUND sign for now, it displays kinda funny and doesn't export well to Excel.
            if clean_prop_price == '':
                break
            property_price.append(clean_prop_price[1:])

        for prop_link in soup.find_all("div", "propertyCard-details"):

            link_html = prop_link.find("a", href=True)
            link_value = link_html["href"]
            # print("Using this link value: {}".format(
            #     link_value))
            if link_value == '':
                print("Link value is empty so I will break")
                break
            property_link.append("rightmove.co.uk"+link_value)

            key_val.append(link_value.split("/")[2])

    print("key_val len: {} - property_list len : {} - property_address : {} - property_price : {} - property_link : {}".format(
        len(key_val), len(property_list), len(property_address), len(property_price), len(property_link)))

    df = pd.DataFrame({
        "Property_ID": key_val,
        "Name": property_list,
        "Address": property_address,
        "Price": property_price,
        "Link": property_link,
        "Extracted_Date": date.today(),

    })
    if save_to_disk:
        df.to_csv("Properties" +
                  date.today().strftime("%Y%m%d") + ".csv", index=False)

    return df


def get_data(pull_from_api, save_to_disk, partial_data):
    if pull_from_api:
        print("I will try to extract from the API")
        df = extract_from_api(save_to_disk, partial_data)
    else:
        print("I will just use the stored CSV")
        df = pd.read_csv("Properties.csv")

    return df


def connect_to_db():
    engine = sqlalchemy.create_engine(
        "mssql+pyodbc://{USER}:{PWD}@{SRV}/{DB}?driver={DRIVER}".format(
            USER=cfg.username, PWD=cfg.password, SRV=cfg.server, DB=cfg.database, DRIVER=cfg.driver), echo=False
    )

    df.to_sql(name='Properties', con=engine, schema='rms',
              if_exists='append', index=False)


df = get_data(pull_from_api=True, save_to_disk=False, partial_data=False)

connect_to_db()
print("Finished gathering data")
