import csv
import json
import time
import urllib.parse
import requests  # imported by uploading the libraies as a zip
import boto3

# TODO--Use python to get store locations from google maps api  then store results as csv text file in S3 storage.

# Map url
url = "https://maps.googleapis.com/maps/api/place/textsearch/json?"
api_key = 'AIzaSyAG8yW8batVj5n3lnlsspeHuCDGjnbHdEU'

# Array to store the name,location and the next token key
stores = []
nextTokenArray = []

# Use the name of your s3 bucket here
s3 = boto3.resource('s3')
BUCKETNAME = 'al-datascience'
fileName = 'data.csv'
outPutname = "location.json"

# Reading the google api first page and get the name and location to the store array
def readurl():
    query = "Pick n Pay"  # Bind the query value can be change
    valueUrl = urllib.parse.quote(query)
    urljson = requests.get(url + 'query=' + valueUrl + '%20in%20south%20africa' + '&key=' + api_key)
    readJson = urljson.json()
    getJson = readJson['results']
    for i in range(len(getJson)):
        store_info = dict()
        store_info['name'] = json.dumps(getJson[i]['name'])
        store_info['lat'] = json.dumps(getJson[i]['geometry']['location']['lat'])
        store_info['lon'] = json.dumps(getJson[i]['geometry']['location']['lng'])
        stores.append(store_info)
    next_token(readJson)
    return


# Get the next page token, loop through and append the name and location to the store array
def next_token(readJson):
    getNextToken = readJson["next_page_token"]
    nextTokenArray.append(getNextToken)
    for nextPageValue in nextTokenArray:
        time.sleep(3)
        print(nextPageValue)
        nextUrl = requests.get(url + 'pagetoken=' + nextPageValue + '&key=' + api_key)
        toJson = nextUrl.json()
        print(toJson)
        y = toJson['results']
        if toJson['status'] == 'OK':
            for i in range(len(y)):
                store_info = dict()
                store_info['name'] = json.dumps(y[i]['name'])
                store_info['lat'] = json.dumps(y[i]['geometry']['location']['lat'])
                store_info['lon'] = json.dumps(y[i]['geometry']['location']['lng'])
                stores.append(store_info)
            json_to_csv(stores)
            if 'next_page_token' in toJson:
                nextPage = toJson['next_page_token']
                nextTokenArray.append(nextPage)


# Create a data.csv that stores the all the name and locations
def json_to_csv(stores):
    with open("/tmp/" + "data.csv", "w") as file:
        csv_file = csv.writer(file)
        csv_file.writerow(["Name", "Latitude", "Longitude"])
        for item in stores:
            csv_file.writerow([item['name'], item['lat'], item['lon']])
    return stores




if __name__ == "__main__":
    readurl()