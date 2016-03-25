import sys
import csv
import urllib.request
import codecs
from datetime import datetime
from datetime import timedelta
from psycopg2 import connect
from psycopg2.extras import DictCursor

# get end-of-day stock quote data (src: Yahoo Financial)
# Author: Clarence Bowen

def get_quote_eod_snapshot(symbol = 'AAPL', days_ago = 0):
	curr_date = datetime.now().date()

	diff = timedelta(days=-days_ago)

	prev_date = curr_date + diff

	# get end of date range using curr_date
	end_yr, end_mnth, end_day = map(int,str(curr_date).split('-'))
	end_mnth = (end_mnth-1)%12

	# get start of date range using prev_date
	start_yr, start_mnth, start_day = map(int,str(prev_date).split('-'))
	start_mnth = (start_mnth-1)%12

	url = 'http://real-chart.finance.yahoo.com/table.csv?s={symbol}&d={end_mnth}&e={end_day}&f={end_yr}&g=d&a={start_mnth}&b={start_day}&c={start_yr}&ignore=.csv'.format(
		end_mnth = end_mnth, end_day = end_day, end_yr = end_yr,
		start_yr = start_yr, start_mnth = start_mnth, start_day = start_day,
		symbol = symbol
		)
	#url = 'http://real-chart.finance.yahoo.com/table.csv?s=' + symbol + '&d=2&e=25&f=2016&g=d&a=5&b=29&c=2010&ignore=.csv'

	try:
		ftpstream = urllib.request.urlopen(url)
	except:
		print("could not open", url, "...exiting script...")
		sys.exit(-1)

	csvfile = csv.reader(codecs.iterdecode(ftpstream, 'utf-8'))
	next(csvfile, None) #skip header

	# connect to database
	with open('../credentials.txt') as f:
		credentials = [x.strip().split(':') for x in f.readlines()]

	for dbname,user,host,password,port in credentials:
		connection = "dbname='" + dbname + "' user='"  + user + "' host='" + host + "' password='" + password + "' port=" + port + ""

	try:
		conn = connect(connection)
	except:
		print ("Unable to connect to database")
		
	cur = conn.cursor(cursor_factory=DictCursor)

	###
	# CREATE TABLE IF NOT EXISTS ticker_price_info_daily(
	# symbol text,
	# date date,
	# open double precision,
	# high double precision,
	# low double precision,
	# close double precision, 
	# volume int, 
	# adj_close  double precision,
	# insert_dt timestamp default now(),
	# CONSTRAINT nk_ticker_price_info_daily UNIQUE(symbol,date)
	# );
	###

	for line in csvfile:
		#print(line)

		#ignore duplicates
		cur.execute("SELECT NULL FROM ticker_price_info_daily WHERE symbol = %s AND date = %s", (symbol,line[0],))
		if cur.fetchone():
			continue

		cur.execute("""
		INSERT INTO ticker_price_info_daily(
		symbol,
		date,
		open,
		high,
		low,
		close, 
		volume, 
		adj_close
		) 
		SELECT %s,%s,%s,%s,%s,%s,%s,%s""", 
		(
		symbol,
		line[0],
		line[1],
		line[2],
		line[3],
		line[4], 
		line[5], 
		line[6],
		) ); 

		conn.commit()

if __name__ == '__main__':
	ticker = input("Please enter your symbol:") #eg. TSLA
	days_ago = int(input("Please enter how far back (in calendar days) to fetch quotes:")) #eg. 4000

	get_quote_eod_snapshot(ticker, days_ago)
