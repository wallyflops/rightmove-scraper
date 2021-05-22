import urllib
import requests
import urllib3
from bs4 import BeautifulSoup
import pandas as pd
from math import ceil
from datetime import date
# URL = 'https://www.rightmove.co.uk/property-for-sale/find.html?searchType=SALE&locationIdentifier=REGION^93968&insId=1&radius=0.25&minPrice=475000&maxPrice=550000&minBedrooms=2&displayPropertyType=&maxDaysSinceAdded=&_includeSSTC=on&sortByPriceDescending=&primaryDisplayPropertyType=&secondaryDisplayPropertyType=&oldDisplayPropertyType=&oldPrimaryDisplayPropertyType=&newHome=&auction=false'


def extract_from_api():
    page_index_num = 0
    http = urllib3.PoolManager()

    payload = {
        'searchType': 'SALE',
        'locationIdentifier': 'REGION^93968',
        'insId': 1,
        'radius': 0.25,
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
    r = http.request(
        'GET', 'https://www.rightmove.co.uk/property-for-sale/find.html', fields=payload)

    soup = BeautifulSoup(r.data, 'html.parser')

    num_of_props = soup.find("span", "searchHeader-resultCount").text
    print("I have found: ", num_of_props)
    total_pages = ceil(int(num_of_props)/24)
    print("I think that means there are {} pages: ".format(total_pages))

    property_list = []
    property_address = []
    property_price = []
    property_link = []

    # for page in range(total_pages):
    for page in range(3):
        page_index_num = 24 * page
        # print("Index I will use is:", page_index_num)
        print(page)

        payload["index"] = page_index_num

        r = http.request(
            'GET', 'https://www.rightmove.co.uk/property-for-sale/find.html', fields=payload)
        # First of all we get the propery names.

        for prop_name in soup.find_all("h2", "propertyCard-title"):
            clean_prop_name = prop_name.text.strip()
            property_list.append(clean_prop_name)

        for prop_address in soup.find_all("address", "propertyCard-address"):
            clean_prop_address = prop_address.text.strip()
            property_address.append(clean_prop_address)

        for prop_price in soup.find_all("div", "propertyCard-priceValue"):
            clean_prop_price = prop_price.text.strip()
            # Removes the POUND sign for now, it displays kinda funny and doesn't export well to Excel.
            property_price.append(clean_prop_price[1:])

        for prop_link in soup.find_all("div", "propertyCard-details"):
            link_html = prop_link.find("a", href=True)
            link_value = link_html["href"]
            property_link.append("rightmove.co.uk"+link_value)

    df = pd.DataFrame({
        "Name": clean_prop_name,
        "Address": property_address,
        "Price": property_price,
        "Link": property_link,
        "Extracted_Date": "",

    })
    return df

    # property_zipped = list(
    #     zip(property_list, property_address, property_price, property_link))

    # return pd.DataFrame(property_zipped, columns=[
    #     "Name", "Address", "Prices", "Link"])


def get_data():
    pull_from_api = 0
    if pull_from_api:
        print("I will try to extract from the API")
        df = extract_from_api()
    else:
        print("I will just use the stored CSV")
        df = pd.read_csv("Properties.csv")

    return df


df = get_data()
df.loc[:, "Extracted_Date"] = date.today()
print(df)
