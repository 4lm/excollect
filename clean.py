import sqlite3
import json

clean = sqlite3.connect('clean.sqlite')
cleanc = clean.cursor()

cleanc.executescript('''
CREATE TABLE IF NOT EXISTS Transactions (
    id      INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    pair    TEXT,
    stamp   TEXT UNIQUE,
    value   TEXT,
    units   TEXT
)
''')

apilog = sqlite3.connect('apilog.sqlite')
apilogc = apilog.cursor()

apilogc.execute('SELECT * FROM Trades')

count = 0
for row in apilogc:
    data = str(row[2].decode())
    js = json.loads(str(data))
    for item in js['result']['XXMRZUSD']:
        print(item[0], item[1], item[2])
        cleanc.execute('''INSERT OR IGNORE INTO Transactions (pair, stamp, value, units) VALUES ( ?, ?, ?, ?)''',
        (memoryview('XXMRZUSD'.encode()), item[2], memoryview(item[0].encode()), memoryview(item[1].encode()) ) )
        count += 1
    clean.commit()
clean.close()
print("Count:", count + 1)
