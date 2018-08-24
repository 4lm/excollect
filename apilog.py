import urllib.request, urllib.parse, urllib.error
import json
import sqlite3
import time

'''
This is an early test. This program reads the XMRUSD pair
from the public Kraken.com API and stores it in a DB.
'''
pair = 'XXMRZUSD'
param = dict()
param['pair'] = pair
url = 'https://api.kraken.com/0/public/Trades?' + urllib.parse.urlencode(param)
print(url)
db = sqlite3.connect('apilog.sqlite')
dbc = db.cursor()
count = 0

dbc.executescript('''
CREATE TABLE IF NOT EXISTS Trades (
    pair TEXT,
    stamp TEXT UNIQUE,
    data TEXT
)
''')

while True:
    count += 1
    print('Retrieving', url)
    uh = urllib.request.urlopen(url)
    data = uh.read().decode()
    js = json.loads(data)
    stamp = js['result']['last']
    print('Count:', count, ', Stamp:', stamp, '- Retrieved', len(data), 'characters', data[:20].replace('\n', ' '))
    dbc.execute('INSERT OR IGNORE INTO Trades (pair, stamp, data) VALUES ( ?, ?, ? )', (memoryview(pair.encode()), memoryview(stamp.encode()), memoryview(data.encode())))
    db.commit()
    time.sleep(60)
