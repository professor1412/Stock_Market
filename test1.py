# import requests
# from bs4 import BeautifulSoup
# import time

# response= requests.get("https://www.google.com/finance/quote/TCS:NSE")
# soup= BeautifulSoup(response.text, 'html.parser')
# print (soup.prettify())
import requests
from bs4 import BeautifulSoup

url = "https://www.google.com/finance/quote/TCS:NSE"
headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, 'html.parser')

# find the element that contains the price
price_element = soup.find("div", class_="YMlKec fxKbKc")

if price_element:
    print("Current TCS Price:", price_element.text)
else:
    print("Price element not found. Page structure may have changed.")
