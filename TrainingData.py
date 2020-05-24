import sqlite3
import pandas as pd

connection = sqlite3.connect('chatdata.db')
c = connection.cursor()
print("Successfully connected to SQLite")
limit = 5000
lastUnix = 0
currLength = limit
counter = 0
testDone = False

while currLength == limit:
    df = pd.read_sql("SELECT * FROM parent_reply WHERE unix > {} ORDER BY unix ASC LIMIT {}".format(lastUnix, limit), connection)
    lastUnix = df.tail(1)['unix'].values[0]
    currLength = len(df)
    if not testDone:
        with open("test.from", 'a', encoding='utf8') as f:
            for content in df['parent_comment'].values:
                f.write(content+'\n')
        with open("test.to", 'a', encoding='utf8') as f:
            for content in df['reply_comment'].values:
                f.write(content+'\n')
        testDone = True
    else:
        with open("train.from", 'a', encoding='utf8') as f:
            for content in df['parent_comment'].values:
                f.write(content+'\n')
        with open("train.to", 'a', encoding='utf8') as f:
            for content in df['reply_comment'].values:
                f.write(content+'\n')
        testDone = True
    counter += 1
    if counter % 20 == 0:
        print(counter * limit, 'rows completed so far')