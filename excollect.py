import datetime
import logging
import requests
import sqlite3
import sys
import time

'''
This is an early test. This program reads all trades of
all pairs from the Kraken.com API and stores them in a DB.
'''

# Logging
logging.basicConfig(filename='excollect.log', level=logging.ERROR)

# Connect to DB and get cursor
db = sqlite3.connect('excollect.sqlite')
dbc = db.cursor()

# Get pairs
r_pairs = requests.get('https://api.kraken.com/0/public/AssetPairs')
pairs = list()
for pair in r_pairs.json()['result']:
    if '.d' in pair:
        continue
    pairs.append(pair)

# Create table
for pair in pairs:
    table = 'Kraken_' + pair + '_Trades'
    create_table = 'CREATE TABLE IF NOT EXISTS ' + table + ''' (
        id              INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        pair            TEXT,
        timestamp       FLOAT UNIQUE,
        price           FLOAT,
        amount          FLOAT,
        buy_sell        TEXT,
        market_limit    TEXT,
        misc            TEXT
    )'''
    dbc.execute(create_table)
    print('Create table', table, 'for pair:', pair)

# Save trades to DB
SLEEP = 0
timer = 0
while True:
    for pair in pairs:
        try:
            r_trades = requests.get('https://api.kraken.com/0/public/Trades?', params = { 'pair': pair })
            print('Retrieving', r_trades.url)
            last = r_trades.json()['result']['last']
            print(datetime.datetime.now(), '- Pair:', pair, '- Timestamp Last Trade:', last)
            table = 'Kraken_' + pair + '_Trades'
            insert_table = 'INSERT OR IGNORE INTO ' + table + ''' (
                pair,
                timestamp,
                price,
                amount,
                buy_sell,
                market_limit,
                misc) VALUES ( ?, ?, ?, ?, ?, ?, ? )'''
            for item in r_trades.json()['result'][pair]:
                dbc.execute(insert_table, (
                memoryview(pair.encode()),
                item[2],
                item[0],
                item[1],
                memoryview(item[3].encode()),
                memoryview(item[4].encode()),
                memoryview(item[5].encode())
                ))
            db.commit()
            timer = 0
            time.sleep(SLEEP)
        except KeyboardInterrupt:
            sys.exit(1)
        except Exception as e:
            timer += 1
            error = 'Error: ' + pair + ' ' + str(sys.exc_info()[0]) + ' - ' + str(e) + ' - Timestamp: ' + str(datetime.datetime.now()) + ' - Timer: ' + str(timer)
            print(error)
            respond = 'Respond from ' + r_trades.url + " - " + r_trades.text[:70].replace('\n', ' ')
            print(respond)
            logging.error(error)
            logging.error(respond)
            time.sleep(timer)
            continue
