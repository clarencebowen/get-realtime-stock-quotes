from json import loads
from datetime import datetime
from urllib.request import urlopen
from psycopg2 import connect
from psycopg2.extras import DictCursor
from time import sleep

# get real-time ticker price data from google, and dump into postgres database
# Author: Clarence Bowen

with open('../credentials.txt') as f:
	credentials = [x.strip().split(':') for x in f.readlines()]

for dbname,user,host,password,port in credentials:
	connection = "dbname='" + dbname + "' user='"  + user + "' host='" + host + "' password='" + password + "' port=" + port + ""

try:
    conn = connect(connection)
except:
    print ("Unable to connect to database")
	
cur = conn.cursor(cursor_factory=DictCursor)

extended_hour_price_change = extended_hour_last_price = extended_hour_price_change_pct = extended_hour_is_price_change_non_negative = extended_hour_mode = symbol = localized_last_update_date =  recent_price = ticker_mid = price_change = last_price = price_change_pct = is_price_change_non_negative = None

tickers = {'TSLA':'/m/0ckhqlx', 'AAPL':'/m/07zmbvf'}

symbols = input("Please enter your symbol or list of comma-delimited symbols:").upper() #eg. TSLA,AAPL

ticker_list = symbols.replace (' ', '').split(',')

sleep_delay = int(input("Please enter time delay (seconds):")) #eg. 60

print("Running...press [^C] or equivalent to cancel")

try:
	while True:
		for ticker_mid_val in (tickers[sym] for sym in ticker_list if sym in tickers): #for ticker_mid_val in tickers.values():
			with urlopen('https://www.google.com/async/finance_price_updates?async=lang:en,country:us,rmids:' + ticker_mid_val) as response:
				#2nd line in file is a dictionary
				price_info = loads(response.readlines()[1].decode('utf-8'))["PriceUpdates"]["price_update"][0]

				symbol = price_info["symbol"]
				extended_hour_mode = price_info["extended_hour_mode"]
				ticker_mid = price_info["ticker_mid"]

				last_update_dt = price_info["localized_last_update_date"].split()
				last_update_dt.insert(2,str(datetime.now().year))
				last_update_dt = ' '.join(last_update_dt)
				localized_last_update_date = datetime.strptime(last_update_dt, '%b %d %Y %I:%M %p')

				price_change = price_info["price"]["price_change_dbl"]
				last_price = price_info["price"]["formatted_price"]["double_value"] # same as "last_price_dbl"
				price_change_pct = price_info["price"]["formatted_change_percent"]["double_value"]
				is_price_change_non_negative = price_info["price"]["is_price_change_non_negative"]

				if "extended_hour_price" in price_info:
					extended_hour_price_change = price_info["extended_hour_price"]["price_change_dbl"]
					extended_hour_last_price = price_info["extended_hour_price"]["last_price_dbl"]
					extended_hour_price_change_pct = price_info["extended_hour_price"]["percent_change"].replace("%","")
					extended_hour_is_price_change_non_negative = price_info["extended_hour_price"]["is_price_change_non_negative"]

			#print(symbol, localized_last_update_date, last_price, price_change, price_change_pct, is_price_change_non_negative, extended_hour_mode, extended_hour_last_price, extended_hour_price_change, extended_hour_price_change_pct, extended_hour_is_price_change_non_negative, ticker_mid)

			###
			# execute the below CREATE in case ticker_price_info does not exist
			# 
			# CREATE TABLE IF NOT EXISTS ticker_price_info (
			# symbol text, 
			# last_price double precision, 
			# price_change double precision, 
			# price_change_pct double precision, 
			# is_price_change_non_negative boolean, 
			# extended_hour_mode int, 
			# extended_hour_last_price double precision, 
			# extended_hour_price_change double precision,
			# extended_hour_price_change_pct double precision,
			# extended_hour_is_price_change_non_negative boolean,
			# last_update_dt timestamp,
			# ticker_mid text,
			# insert_dt timestamp default now()
			# );
			###
			
			cur.execute("""
			INSERT INTO ticker_price_info(
			symbol,
			last_price,
			price_change,
			price_change_pct,
			is_price_change_non_negative,
			extended_hour_mode, 
			extended_hour_last_price, 
			extended_hour_price_change,
			extended_hour_price_change_pct,
			extended_hour_is_price_change_non_negative,
			last_update_dt,
			ticker_mid
			) 
			SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s""", 
			(
			symbol,
			last_price,
			price_change,
			price_change_pct,
			is_price_change_non_negative,
			extended_hour_mode, 
			extended_hour_last_price, 
			extended_hour_price_change,
			extended_hour_price_change_pct,
			extended_hour_is_price_change_non_negative,
			localized_last_update_date,
			ticker_mid,
			) ); 

			conn.commit()
			
		sleep(sleep_delay)

except KeyboardInterrupt:
	print ('Canceled!')
