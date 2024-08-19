from bs4 import BeautifulSoup
import time
from datetime import datetime
import requests
from tqdm import tqdm
import pandas as pd
import csv


# function to crawl specific website
def crawler(url: str, result: str, delay_counter=int(0), parity="whole"):

    # price in rupiah, payment in year, and parity for steps, starting page and total page
    units = {
        "Juta": 1000000,
        "Miliar": 1000000000,
        "tahun": 1,
        "bulan": 12,
        "whole": 1,
        "odd": 1,
        "even": 2,
    }

    # for http request sleep time
    delay = [2, 5, 3, 8]

    step = 2 if parity != "whole" else 1
    current_page = units.get(parity)

    # only check date once to save resource
    date = datetime.today().strftime("%Y-%m-%d")

    # dump http request error or not 200
    error_list = []

    # browser header to prevent blocking
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
    }

    # check last page number
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.text, "lxml")
    last_page = int(soup.findAll("li", class_="ui-molecule-paginate__item")[-1].text)

    # iterate until last page
    for i in tqdm(range(last_page // step), desc=f"{url}, {parity}"):

        # nth iteration page request
        link = url + str(current_page)
        page = requests.get(link, headers=headers)

        # documenting if request is denied
        if page.status_code != 200:
            error_list.append(
                "Page: " + str(current_page) + ", Status Code: " + str(page.status_code)
            )

        # parsing to find list of properties
        soup = BeautifulSoup(page.text, "lxml")
        properties = soup.findAll("div", class_="card-featured__middle-section")

        # extract value from each property
        for property in properties:
            temp = {}
            temp["date"] = date
            temp["title"] = property.find("a").text
            temp["location"] = property.findAll("span")[0].text.split(", ")[0]
            temp["city"] = property.findAll("span")[0].text.split(", ")[1]

            # temporary variable that need more processing
            featured_price = property.find(
                "div", class_="card-featured__middle-section__price"
            )
            price = featured_price.find("strong").text.replace(",", ".").split()

            # need more edge case test
            room = property.findAll("span", class_="attribute-text")
            temp["bedroom"] = 0 if len(room) < 1 else int(room[0].text)
            temp["bathroom"] = None if len(room) < 2 else int(room[1].text)

            # set all to 0 if house doesn't have both lot & floor area
            size = property.findAll("div", class_="attribute-info")
            temp["lot area"] = (
                0 if len(size) <= 1 else int(size[0].find("span").text.split()[0])
            )
            temp["floor area"] = (
                0 if len(size) <= 1 else int(size[1].find("span").text.split()[0])
            )

            # not saving the data because it's suspected to be inconsistent or fraudulent.
            if (
                temp["lot area"] > 3500
                or temp["floor area"] > 2600
                or "hari" in price[4]
                or temp["lot area"] == 0
                or temp["bedroom"] == 0
            ):
                continue

            else:
                # convert the price into rupiah per year
                temp["price"] = int(
                    float(price[1])
                    * int(units.get(price[2]) * int(units.get(price[4])))
                )

                # not saving the data because it's suspected to be inconsistent or fraudulent.
                if temp["price"] > 11000000:
                    # saving the data
                    result.append(temp)

        current_page += step

        # pause request to prevent ip blocking
        time.sleep(delay[delay_counter])
        delay_counter = int((int(delay_counter) + 1) % len(delay))

    # printing all the error with each http code and page index
    for error in error_list:
        print(error)


data = []

url_list = [
    "https://www.rumah123.com/sewa/dki-jakarta/rumah/?page=",
    "https://www.rumah123.com/sewa/depok/rumah/?page=",
    "https://www.rumah123.com/sewa/tangerang-selatan/rumah/?page=",
    "https://www.rumah123.com/sewa/bekasi/rumah/?page=",
    "https://www.rumah123.com/sewa/tangerang/rumah/?page=",
]

for url in url_list:
    crawler(url, data)

# skill issue, so using CSV
header = [
    "date",
    "title",
    "location",
    "city",
    "bedroom",
    "bathroom",
    "lot area",
    "floor area",
    "price",
]
with open(f"{date}house.csv", "w") as file:
    writer = csv.DictWriter(file, fieldnames=header)
    writer.writeheader()
    for row in data:
        writer.writerow(row)
