import datetime
import logging
import requests
import sqlite3
import sys
import time

'''
This is an early test. This program reads the XMRUSD pair
from the public Kraken.com API and stores it in a DB.
'''

# Logging
logging.basicConfig(filename='apilog.log', level=logging.ERROR)

# Connect to DB and get cursor
db = sqlite3.connect('apilog.sqlite')
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
    create_table = 'CREATE TABLE IF NOT EXISTS ' + table + ' (pair TEXT, timestamp INTEGER UNIQUE, data TEXT)'
    dbc.execute(create_table)
    print('Create table', table, 'for pair:', pair
    )

# Save trades to DB
SLEEP = 0
timer = 0
while True:
    for pair in pairs:
        try:
            r_trades = requests.get('https://api.kraken.com/0/public/Trades?', params = { 'pair': pair })
            print('Retrieving', r_trades.url)
            timestamp = r_trades.json()['result']['last']
            print(datetime.datetime.now(), '- Pair:', pair, '- Timestamp Last Trade:', timestamp)
            table = 'Kraken_' + pair + '_Trades'
            insert_table = 'INSERT OR IGNORE INTO ' + table + ' (pair, timestamp, data) VALUES ( ?, ?, ? )'
            dbc.execute(insert_table, (memoryview(pair.encode()), timestamp, memoryview(r_trades.text.encode())))
            db.commit()
            timer = 0
            time.sleep(SLEEP)
        except KeyboardInterrupt:
            sys.exit(1)
        except Exception as e:
            timer += 1
            error = 'Error: ' + table + ' ' + str(sys.exc_info()[0]) + ' - ' + str(e) + ' - Timestamp: ' + str(datetime.datetime.now()) + ' - Timer: ' + str(timer)
            print(error)
            message = 'Message from ' + r_trades.url + " - " + r_trades.text
            print(message)
            logging.error(error)
            logging.error(message)
            time.sleep(timer)
            continue
