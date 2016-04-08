from json import loads
from datetime import datetime
from urllib.request import urlopen
from psycopg2 import connect
from psycopg2.extras import DictCursor
from time import sleep
from time import time
import threading
import sys

# get real-time ticker price data
# Author: Clarence Bowen

'''
with open('../credentials.txt') as f:
	credentials = [x.strip().split(':') for x in f.readlines()]

for dbname,user,host,password,port in credentials:
	connection = "dbname='" + dbname + "' user='"  + user + "' host='" + host + "' password='" + password + "' port=" + port + ""

try:
    conn = connect(connection)
except:
    print ("Unable to connect to database")
	
cur = conn.cursor(cursor_factory=DictCursor)
'''

class Worker (threading.Thread):
	def __init__(self, threadID, name, ticker, ticker_mid_val, delay):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.name = name
		self.delay = delay
		self.ticker_mid_val = ticker_mid_val
	def run(self):
		print ("Running",self.name,"for",ticker)
		get_stock_quote(self.ticker_mid_val, self.delay)

def get_stock_quote(ticker_mid_val, sleep_delay):
	extended_hour_price_change = extended_hour_last_price = extended_hour_price_change_pct = extended_hour_is_price_change_non_negative = extended_hour_mode = symbol = localized_last_update_date =  recent_price = ticker_mid = price_change = last_price = price_change_pct = is_price_change_non_negative = None

	while True:
		with urlopen('https://www.google.com/async/finance_price_updates?async=lang:en,country:us,rmids:' + ticker_mid_val) as response:
			#2nd line in file is a dictionary
			price_info = loads(response.readlines()[1].decode('utf-8'))["PriceUpdates"]["price_update"][0]

			symbol = price_info["symbol"]
			extended_hour_mode = price_info["extended_hour_mode"]
			ticker_mid = price_info["ticker_mid"]

			last_update_dt = price_info["localized_last_update_date"].replace(",","").replace("EDT","").replace("EST","").strip().split()
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

		# print(symbol, localized_last_update_date, last_price, price_change, price_change_pct, is_price_change_non_negative, extended_hour_mode, extended_hour_last_price, extended_hour_price_change, extended_hour_price_change_pct, extended_hour_is_price_change_non_negative, ticker_mid)

		# write to file
		with open("intraday_stockquotes_{}.txt".format(datetime.utcnow().strftime("%Y%m%d")), "a") as sq:
			sq.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(	symbol,
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
												datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
											)
					)
		'''
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
	'''
		sleep(sleep_delay)

tickers = {'TSLA':'/m/0ckhqlx', 'AAPL':'/m/07zmbvf', 'AMZN':'/m/07zl90k', 'GOOG':'/g/1q4t94b6p', 'FB':'/m/0rz9htl', 
'BABA':'/g/1q6b4f1pf', 'LNKD':'/m/0gmkq6j', 'TWTR':'/g/1ydpvdm0w', 'SCTY':'/g/11x19sc6q', 'GRPN':'/m/0rzpy45',
'YELP':'/g/1hbvw6nn_'}

symbols = input("Please enter your symbol or list of comma-delimited symbols:").upper() #eg. TSLA,AAPL

user_ticker_list = symbols.replace (' ', '').split(',')
ticker_list = []

for sym in user_ticker_list:
	if sym not in tickers:
		print(sym, "doesn't exist in pre-defined list of symbols, Skipping...")
		continue
	ticker_list.append(sym)

if not ticker_list:
	print("Symbols requested do not exist in pre-defined list. Exiting script...")
	sys.exit(-1)

try:
	user_sleep_delay = int(input("Please enter time delay (seconds):")) #eg. 5
except ValueError:
	sleep_delay = 5
	print ("Invalid delay. Delay set to {} second(s) ".format(sleep_delay))
else:
	if user_sleep_delay < 1:
		sleep_delay = 5
		print ("{} second(s) is too high or too low. Delay set to {} second(s) ".format(user_sleep_delay,sleep_delay))
	else:
		sleep_delay = user_sleep_delay

try:
	user_duration = int(input("Please enter how long program should run for (minutes):")) #eg. 60
except ValueError:
	program_duration = 1
	print ("Invalid program duration. Program duration set to {} minute(s) ".format(program_duration))
else:
	if user_duration > 720 or user_duration <= 0:
		program_duration = 1
		print ("{} minute(s) is too high or too low. Program duration set to {} minute(s) ".format(user_duration,program_duration))
	else:
		program_duration = user_duration

end_time = program_duration * 60 + time()

print("\nRunning program...press [^C] or equivalent to cancel\n")

thread_num = 1
thread_set = set()

try:
	while time() < end_time:
		for ticker,ticker_mid_val in ((sym,tickers[sym]) for sym in ticker_list if sym in tickers): #for ticker_mid_val in tickers.values():

			# skip thread creation if already exists
			if "thread_{}".format(ticker_mid_val) in thread_set:
				continue;
			else:
				thread_set.add("thread_{}".format(ticker_mid_val))

			# Create new thread
			sub_thread = Worker(thread_num, "thread_{}".format(ticker_mid_val), ticker, ticker_mid_val, sleep_delay)

			# set thread to daemon thread so that terminating main (non-daemon) thread automatically kills all non-daemon threads
			if sub_thread.isDaemon() is False:
				sub_thread.setDaemon(True)

			# Start thread
			sub_thread.start()

			thread_num += 1
		#sleep(sleep_delay)
except KeyboardInterrupt:
	print ('\nUser canceling request. Terminating threads...')
	for _ in thread_set:
		print ('Terminating', _)
	print('\nAll threads terminated!')
else:
	print ("Time's up. All threads terminated. Program complete!")
