import requests;
import csv;
import arrow;
import sys

src="list_of_stocks.csv"
dest="hourly_data.csv"

interval="3600"
numPastTradingDays="3"
dateFormat ="HH:mm MM-DD-YYYY"

def readFromStockList():
  with open(src, 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=",")
    return next(reader)

def getHeaders():
  return ["Ticker", "TIME", "CLOSE","HIGH","LOW","OPEN","VOLUME"]
    
def getStockData(stockList):
  results = []
  for ticker in stockList:
    qurl = "http://www.google.com/finance/getprices?" + "q=" + ticker + "&i=" + interval + "&p=" + numPastTradingDays + "d" + "&f=d,o,h,l,c,v"
    r = requests.get(qurl)
    rows = parseAPIResult(r.text, ticker)
    results += rows;
  return results;

def parseAPIResult(text, ticker):
  split = text.split("\n");
  lastTimestamp = None;
  timezoneOffset = 0
  apiResultList = []
  for row in split:
    if len(row) > 0:
      if '=' in row or "EXCHANGE" in row:
        if "TIMEZONE_OFFSET" in row:
          row = row.split("=")
          timezoneOffset = int(row[1]) * 60
      else:
        row = row.split(",");
        if (isRawTimestamp(row)):
          lastTimestamp = int(row[0][1:])
          row[0] = arrow.Arrow.utcfromtimestamp(int(row[0][1:]) + timezoneOffset).format(dateFormat)
          apiResultList.append([ticker] + row)
        else:
          calculatedTime = int(row[0]) * int(interval) + lastTimestamp;
          row[0] = arrow.Arrow.utcfromtimestamp(int(calculatedTime) + timezoneOffset).format(dateFormat)
          apiResultList.append([ticker] + row)
  return apiResultList;

def isRawTimestamp(row):
  return row[0][0] == "a"

def writeResultsToFile(results):
  with open(dest, 'wb') as destFile:
    writer = csv.writer(destFile, delimiter=",")
    for row in results:
      writer.writerow(row)

def run():
  stockList = readFromStockList()
  print "Parsing " + str(len(stockList)) + " stocks"
  results = getStockData(stockList)
  headers = getHeaders();
  file = [headers] + results
  writeResultsToFile(file)

def parseCommandLineArgs():
  if (len(sys.argv) != 2):
    days = 22
    run()
  else:
    days = sys.argv[1]
    run();

parseCommandLineArgs();


