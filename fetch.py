#!/usr/bin/python3
import sys
import os
import sqlite3
import time
import twstock
from collections import namedtuple, defaultdict
from datetime import date, datetime

def connect(stockid):
	dbpath = "db/" + str(stockid) + ".db"
#    print(dbpath)
	create = not os.path.exists(dbpath)
	db = sqlite3.connect(dbpath)
	if create:
		print(dbpath + " does not exists, create it")
		cursor = db.cursor()
		cursor.execute("CREATE TABLE stockdata ("
			"date TEXT PRIMARY KEY NOT NULL, " 
			"capacity INT NOT NULL, "
			"turnover INT NOT NULL, "
			"open REAL NOT NULL, "
			"high REAL NOT NULL, "
			"low REAL NOT NULL, "
			"close REAL NOT NULL, "
			"change REAL NOT NULL, "
			"tx INT NOT NULL)")

		db.commit()
	else:
		print(dbpath + " exists")
	return db



#stockid = 2330

def fetch_stockdata(stockid):
	# open db
	db = None
	db = connect(stockid)

	# set fetch start date and last update date
	'''
	fetch last record in db to fetch non-sync data
	date format : (str) 2018-07-23
	'''
	cursor = db.cursor()
	cursor.execute("SELECT date FROM stockdata LIMIT 1")
	#if new db created
	if cursor.fetchone() is None:
		last_record_date = "2000-01-01"
		fetch_start_year = 2000
		fetch_start_month = 1
	else:
		cursor.execute("SELECT * FROM stockdata ORDER BY date DESC")
		last_record_date = cursor.fetchone()[0]
		fetch_start_year, _, _ = last_record_date.split('-')
		fetch_start_year = int(fetch_start_year)
		fetch_start_month = 1

	print("""
	last record date : %r
	fetch start year : %r
	fetch start month : %r
		""" %(last_record_date, fetch_start_year, fetch_start_month))
	# set period
	#fetch_start_year = 2018
	#fetch_start_month = 7
	fetch_period = [(x, y)	for x in range(int(fetch_start_year), int(date.today().year) + 1) \
							for y in range(1, 13) \
							if (x, y) <= (int(date.today().year), int(date.today().month))  ]

	# fetch stock data
	STOCK_DATA = namedtuple("STOCK_DATA", "date capacity turnover open high low close change tx")
	stock = twstock.Stock(str(stockid))
	cursor = db.cursor()

	for i in fetch_period:
		print(i)
		stock_datas = [ STOCK_DATA(a.strftime('%Y-%m-%d'), b, c, d, e, f, g, h, i) for a, b, c, d, e, f, g, h, i in stock.fetch(*i) \
						if last_record_date < a.strftime('%Y-%m-%d')]
		if stock_datas:
			print("There are %r datas for %r" %(len(stock_datas), stockid))
		else:
			print("There is no newer data (last record date : %r)" %(last_record_date))
			continue
		for j in stock_datas:
			print(*j)
	#        print("{0.date}\t".format(j))
	#        print("INSERT INTO stockdata (date, capacity, turnover, open, high, low, close, change, tx) VALUES ({0.date}, {0.capacity}, {0.turnover}, {0.open}, {0.high}, {0.low}, {0.close}, {0.change}, {0.tx})".format(j))
	#        print()
	#        print()
			cursor.execute("INSERT INTO stockdata (date, capacity, turnover, open, high, low, close, change, tx) VALUES ('{0.date}', {0.capacity}, {0.turnover}, {0.open}, {0.high}, {0.low}, {0.close}, {0.change}, {0.tx})".format(j))
			db.commit()

		print(	"""
				===========================
				""")
		time.sleep(10)

	#close db
	if db is not None:
		print("db is not None, close it")
		db.close


if __name__ == '__main__':
	stockid = int(sys.argv[1])
	fetch_stockdata(stockid)
