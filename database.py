import sqlite3
import json
from datetime import datetime
import zstandard as zstd
import io
import time
import csv

sql_transactions = []
connection = sqlite3.connect('chatdata.db')
c = connection.cursor()
print("Successfully connected to SQLite")

def createTable():
    c.execute("""CREATE TABLE IF NOT EXISTS parent_reply (comment_id TEXT PRIMARY KEY,
                parent_id TEXT, parent_comment TEXT, reply_comment TEXT, subreddit TEXT,
                unix INT, score INT)""")

def formatData(data):
    data = data.replace("\n", " ").replace("\r", " ").replace("\"", "'")
    return data

def findParentComment(cid):
    try:
        sql = "SELECT parent_comment, reply_comment, score FROM parent_reply WHERE comment_id = \"{}\" LIMIT 1".format(cid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result
        else:
            return False
    except Exception as e:
        print ("findParent", e)
        return False

def isAcceptableBody(data):
    if len(data.split(' ')) > 100 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    if data == '[deleted]' or data == '[removed]' or data == 'Thank you!  *I am a bot, and this action was performed automatically. Please [contact the moderators of this subreddit](/message/compose/?to=/r/puns) if you have any questions or concerns.*':
        return False
    else:
        return True

def addReplyComment(commentId, replyComment, score):
    try:
        sql = "UPDATE parent_reply SET reply_comment = \"{}\", score = \"{}\" WHERE comment_id = \"{}\";".format(replyComment, score, commentId) 
        c.execute(sql)
        c.execute('COMMIT')
    except Exception as e:
        print ("addReplyComment", e)
    
def insertComment(commentId, parentId, parentComment, subreddit, createdUTC, score):
    sql = """INSERT INTO parent_reply (comment_id, parent_id, parent_comment, reply_comment, subreddit, unix, score) VALUES ("{}", "{}", "{}", NULL, "{}", {}, {});""".format(commentId, parentId, parentComment, subreddit, createdUTC, score) 
    transaction_bldr(sql)

def transaction_bldr(sql):
    global sql_transactions
    sql_transactions.append(sql)

    if len(sql_transactions) > 10000:
        executeBulkTransactions()

def executeBulkTransactions():
    global sql_transactions
    c.execute('BEGIN TRANSACTION')
    for query in sql_transactions:
        try:
            c.execute(query)
        except:
            pass
    c.execute('COMMIT')
    sql_transactions = []
    
if __name__ == "__main__":
    createTable()
    rowCounter = 0
    totalInserted = 0
    with open('D:/chatbot-data/RC_2019-12.zst', 'rb') as fh:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(fh) as reader:
            wrap = io.BufferedReader(reader)
            line = wrap.readline()
            while line:
                try:
                    row = json.loads(line)
                    commentId = row['id']
                    parentId = row['parent_id'] #t3_ is a root comment, t1_ is a regular/reply comment
                    body = formatData(row['body'])
                    createdUTC = row['created_utc']
                    score = row['score']
                    subreddit = row['subreddit']
                    rowCounter += 1

                    if score >= 2 and isAcceptableBody(body):
                        insertComment(commentId, parentId, body, subreddit, createdUTC, score)
                        totalInserted += 1
                    line = wrap.readline()
                except:
                    line = wrap.readline()
                    pass
                if rowCounter % 100000 == 0:
                    print("Total rows read: {}, total inserted: {}, Time: {}".format(rowCounter, totalInserted, str(datetime.now())))

            if sql_transactions:
                executeBulkTransactions()

    print("Finished Inserting! Total rows read: {}, total inserted: {}, Time: {}".format(rowCounter, totalInserted, str(datetime.now())))
    rowCounter = 0
    pairedRows = 0
    print("Updating rows with their replies...")
    for row in connection.execute('SELECT * FROM parent_reply'):
        parentId = row[1]
        commentId = row[0]
        comment = row[2]
        score = row[6]
        if parentId.startswith("t1_"): #This comment is a reply to another comment
            searchId = parentId[3:] #remove t1_
            result = findParentComment(searchId)
            if result:
                if result[1] == None:
                    addReplyComment(searchId, comment, score)
                    pairedRows += 1
                elif score > result[2]:
                    addReplyComment(searchId, comment, score)
        rowCounter += 1
        if rowCounter % 10000 == 0:
            print("Total rows read: {}, Paired Rows: {}, Time: {}".format(rowCounter, pairedRows, str(datetime.now())))
    print("Finished Updating! Total rows read: {}, Total Paired Rows: {}, Time: {}".format(rowCounter, pairedRows, str(datetime.now())))

    print("Removing rows with empty replies...")
    c.execute("DELETE FROM parent_reply WHERE reply_comment IS NULL")
    c.execute('COMMIT')
    print("Finished removing rows with empty replies!")