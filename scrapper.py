import pyodbc
import re
import json
import csv
from io import StringIO
from bs4 import BeautifulSoup
import requests
import pandas as pd
import datetime as dtmodule
from datetime import datetime,timedelta

#DATE CONVERTER FROM HTML
def htmlconverter(date):
  #dd/mm/yyyy
  if date != None:
    day = date[:2]
    month = date[3:5]
    year = date[6:]
    htmlcode = day + '%2F' + month + '%2F' + year
    return htmlcode
  else:
    return None

#DATE CONVERTER FROM STRING
def datetimeconverter(date_time):
  date_time = date_time.split() #split each component into array

  date_time[2] = str(int(date_time[2]) - 543) #convert year

#dealing with month
  if date_time[1] == 'ม.ค.':
    date_time[1] = '01'

  elif date_time[1] == 'ก.พ.':
    date_time[1] = '02'

  elif date_time[1] == 'มี.ค.':
    date_time[1] = '03'

  elif date_time[1] == 'เม.ย.':
    date_time[1] = '04'

  elif date_time[1] == 'พ.ค.':
    date_time[1] = '05'

  elif date_time[1] == 'มิ.ย.':
    date_time[1] = '06'

  elif date_time[1] == 'ก.ค.':
    date_time[1] = '07'

  elif date_time[1] == 'ส.ค.':
    date_time[1] = '08'

  elif date_time[1] == 'ก.ย.':
    date_time[1] = '09'

  elif date_time[1] == 'ต.ค.':
    date_time[1] = '10'

  elif date_time[1] == 'พ.ย.':
    date_time[1] = '11'

  elif date_time[1] == 'ธ.ค.':
    date_time[1] = '12'

  dt = date_time[0] + '/' + date_time[1] + '/' + date_time[2] + ' ' + date_time[3]
  dt = dtmodule.datetime.strptime(dt, '%d/%m/%Y %H:%M:%S')
  return dt
  

def dt_converter(date):
  if date != None:
    dt = dtmodule.datetime.strptime(date, '%d/%m/%Y')
  else:
    dt = None
  return dt
  

#MSSQL connection
server = ''
database = ''
username = ''
connection = pyodbc.connect('DRIVER={SQL SERVER};SERVER='+server+';DATABASE='+database+';UID='+username+';Trusted_Connection=yes;')
cursor = connection.cursor()

#INVERTAL SCRAPPER
def intervalscraper(start_dt, end_dt, start_url, end_url):
  #Step1: Delete entries in MS SQL database table for start_date
  query = "DELETE FROM `SET_NEWS` WHERE `วันที่และเวลา` BETWEEN " + start_dt + " AND " + end_dt + ";" 
  cursor.execute(query)

  #Step2: Create a dataframe to store data
  df = pd.DataFrame(columns=['วันที่และเวลา', 'หลักทรัพย์', 'แหล่งข่าว', 'หัวข้อข่าว'])

  #Step3: Scraping Data  
  i=0
  page_no = 0
  while len(table_entries) == 20:
    page = requests.get(url_interval.format(start_url, page_no, end_url))
    soup = BeautifulSoup(page.content, 'html.parser')
    table_body = soup.find('tbody')
    table_entries = table_body.find_all('tr')
    
    while i < len(table_entries):
      entry = [j.text.strip() for j in table_entries[i].find_all('td') if j.text.strip() != '' and j.text.strip() != 'รายละเอียด']
      #print(entry)
      length = len(df)
      df.loc[length] = entry
      i += 1

    page_no += 1
    #to update table_entries
    page = requests.get(url_interval.format(start_url, page_no, end_url))
    soup = BeautifulSoup(page.content, 'html.parser')
    table_body = soup.find('tbody')
    table_entries = table_body.find_all('tr')

#the case where there is no next page
  else: 
    while i < len(table_entries):
      entry = [j.text.strip() for j in table_entries[i].find_all('td') if j.text.strip() != '' and j.text.strip() != 'รายละเอียด']
      #print(entry)
      length = len(df)
      df.loc[length] = entry
      i += 1
    
  df['วันที่/เวลา'] = df['วันที่/เวลา'].apply(lambda x: datetimeconverter(x)) #Datetime conversion

  #Step4: Insert to MSSQL
  for i, row in df.iterrows():
    query = "INSERT INTO `SET_NEWS` (`วันที่และเวลา`, `หลักทรัพย์`, `แหล่งข่าว`, `หัวข้อข่าว`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(query, tuple(row))
    connection.commit() #commit to save changes

  connection.close()

#DAY SCRAPPER
def dayscraper(start_dt):
  #Step1: Delete entries in MS SQL database table for start_date
  #from start date 00:00 to the next 23 hours and 59 minutes = one day
  query = "DELETE FROM `SET_NEWS` WHERE `วันที่และเวลา` BETWEEN " + start_dt + " AND " + start_dt + dtmodule.timedelta(hours=23, minutes=59) + ";" 
  cursor.execute(query)

  #Step2: Create a dataframe to store data
  df = pd.DataFrame(columns=['วันที่และเวลา', 'หลักทรัพย์', 'แหล่งข่าว', 'หัวข้อข่าว'])

  #Step3: Scraping Data
  table_body = soup.find_all('tbody')[2] #there are 3 tables on the today's news page but our table of interest is at the index 2.
  table_entries = table_body.find_all('tr')

  i = 0
  while i < len(table_entries):
    entry = [j.text.strip() for j in table_entries[i].find_all('td') if j.text.strip() != '' and j.text.strip() != 'รายละเอียด']
    length = len(df)
    df.loc[length] = entry
    i += 1
  
  df['วันที่/เวลา'] = df['วันที่/เวลา'].apply(lambda x: datetimeconverter(x)) #Datetime conversion

  #Step4: Insert to MSSQL
  for i, row in df.iterrows():
    query = "INSERT INTO `SET_NEWS` (`วันที่และเวลา`, `หลักทรัพย์`, `แหล่งข่าว`, `หัวข้อข่าว`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(query, tuple(row))
    connection.commit() #commit to save changes

  connection.close()

#MAIN FUNCTION
def main(start_date=None, end_date=None):
#Default case is to get today's news (both start and end date are None).
#Case1: both are not None, Normal interval case
#Case2: end_date are None, Single day in the past case

#Web connection
  url_interval = "https://www.set.or.th/set/newslist.do?symbol=&country=TH&submit=ค้นหา&securityType=&from={}&language=th&currentpage={}&source=&to={}&newsGroupId=&headline="
  url_day = "https://www.set.or.th/set/searchtodaynews.do?source=company&symbol=&securityType=S&newsGroupId=&headline=&submit=ค้นหา&language=th&country=TH"
  
  #For URL (html to datetime)
  start_url = htmlconverter(start_date)
  page_no = '0' #always start at page 1 (index 0)
  end_url = htmlconverter(end_date)

  #For functions (string to datetime)
  start_dt = dt_converter(start_date)
  end_dt = dt_converter(end_date)

#INTERVAL CASE
  if start_date != None and end_date != None: 
    try:
      page = requests.get(url_interval.format(start_url, page_no, end_url))
      soup = BeautifulSoup(page.content, 'html.parser')
      table_body = soup.find('tbody')
      table_entries = table_body.find_all('tr')

      intervalscraper(start_dt, end_dt, start_url, end_url)

    except TypeError:
      print('TypeError!')

  elif start_date != None and end_date == None: #a single day in the past
    try:
      page = requests.get(url_interval.format(start_url, page_no, start_url))
      soup = BeautifulSoup(page.content, 'html.parser')
      table_body = soup.find('tbody')
      table_entries = table_body.find_all('tr')
      
      intervalscraper(start_dt, start_dt, start_url, start_url)

    except TypeError:
      print('TypeError!')


#SINGLE-DAY CASE
  elif start_date == None and end_date == None: 
    try:
      page = requests.get(url_day)
      soup = BeautifulSoup(page.content, 'html.parser')
      dayscraper(start_dt)

    except TypeError:
      print('TypeError!')
    
    else:
      print('Error: start_date cannot be None if end_date is not None.')
