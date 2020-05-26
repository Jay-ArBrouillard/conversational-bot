import sqlite3
import json
import time
import zstandard as zstd
import io
import time
import csv

sql_transactions = []
connection = sqlite3.connect('chatdata.db')
c = connection.cursor()
print("Successfully connected to SQLite")

def createTable():
    c.execute("""CREATE TABLE IF NOT EXISTS parent_reply (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent_comment TEXT, reply_comment TEXT, subreddit TEXT,
                unix INT, parent_score INT, reply_score INT)""")

def createIndex():
    c.execute("""CREATE INDEX IF NOT EXISTS "index" ON "parent_reply" (
	            "parent_id"
            );""")

def dropIndex():
    c.execute("""DROP INDEX "index";""")

def formatData(data):
    data = data.replace("\n", " ").replace("\r", " ").replace("\"", "'")
    return data

def findParentComment(cid):
    try:
        c.execute("SELECT * FROM parent_reply WHERE comment_id = \"{}\"".format(cid))
        result = c.fetchone()
        if result != None:
            return result
        else:
            return False
    except Exception as e:
        print ("findParent", e)
        return False

def findScore(cid):
    try:
        sql = "SELECT reply_score FROM parent_reply WHERE comment_id = \"{}\" LIMIT 1".format(cid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        print ("findScore", e)
        return False

def isAcceptableBody(data):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    if data == '[deleted]' or data == '[removed]' or data == 'Thank you!  *I am a bot, and this action was performed automatically. Please [contact the moderators of this subreddit](/message/compose/?to=/r/puns) if you have any questions or concerns.*':
        return False
    else:
        return True

def addReplyComment(parentId, replyComment, replyScore):
    try:
        sql = "UPDATE parent_reply SET reply_comment = \"{}\", reply_score = {} WHERE parent_id = \"{}\";".format(replyComment, replyScore, parentId)
        transaction_bldr(sql)
    except Exception as e:
        print ("addReplyComment", e)
    
def insertComment(commentId, parentId, parentComment, subreddit, createdUTC, parentScore, replyScore):
    sql = """INSERT INTO parent_reply (comment_id, parent_id, parent_comment, reply_comment, subreddit, unix, parent_score, reply_score) VALUES ("{}", "{}", "{}", NULL, "{}", {}, {}, {});""".format(commentId, parentId, parentComment, subreddit, createdUTC, parentScore, replyScore) 
    transaction_bldr(sql)

def deleteComment(parentId):
    sql = "DELETE FROM parent_reply WHERE parent_id = \"{}\"".format(parentId)
    transaction_bldr(sql)

def findReply(parentId):
    try:
        c.execute("SELECT parent_comment, parent_score FROM parent_reply WHERE parent_id = \"{}\"".format(parentId))
        result = c.fetchone()
        if result != None:
            return result
        else:
            return False
    except Exception as e:
        print ("findReply", e)
        return False

def transaction_bldr(sql):
    global sql_transactions
    sql_transactions.append(sql)

    if len(sql_transactions) > 10000:
        executeBulkTransactions()

def executeBulkTransactions():
    global sql_transactions
    c.execute('PRAGMA synchronous = OFF')
    c.execute('PRAGMA cache_size = 100000')
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
    start_time = time.time()
    with open('D:/chatbot-data/RC_2019-12.zst', 'rb') as fh:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(fh) as reader:
            wrap = io.BufferedReader(reader)
            line = wrap.readline()
            while line:
                try:
                    row = json.loads(line)
                    parentId = row['parent_id'] #t3_ is a root comment, t1_ is a regular/reply comment
                    commentId = row['id']
                    body = formatData(row['body'])
                    createdUTC = row['created_utc']
                    score = row['score']
                    subreddit = row['subreddit']
                    rowCounter += 1

                    if score >= 2 and isAcceptableBody(body):
                        insertComment(commentId, parentId, body, subreddit, createdUTC, score, 0)
                        totalInserted += 1
                    line = wrap.readline()
                except:
                    line = wrap.readline()
                    pass
                if rowCounter % 100000 == 0:
                    print("Total rows read: {}, rows inserted: {}, Elapsed Time(hrs): {}".format(rowCounter, totalInserted, (time.time() - start_time) / 60 / 60))
                
    if sql_transactions:
        executeBulkTransactions()
    print("Finished Inserting! Total rows read: {}, total rows inserted: {}, Elapsed Time(hrs): {}".format(rowCounter, totalInserted, (time.time() - start_time) / 60 / 60))

    print("Creating index on parent_id...")
    createIndex()
    print("Finish index on parent_id!")

    print("Removing rows that don't have a parent comment...")
    rowCounter = 0
    deletedRows = 0
    for row in connection.execute('SELECT * FROM parent_reply'):
        parentId = row[0]
        if parentId.startswith("t1_"):
            searchId = parentId[3:]
            comment = findParentComment(searchId)
            if not comment:
                deleteComment(searchId)
                deletedRows += 1
        rowCounter += 1
        if rowCounter % 100000 == 0:
            print("Total rows read: {}, Deleted Rows: {}, Elapsed Time(hrs): {}".format(rowCounter, deletedRows, (time.time() - start_time) / 60 / 60))
    if sql_transactions:
        executeBulkTransactions()
    print("Finished removing rows with empty replies! Total Rows Deleted {}, Elapsed Time(hrs): {}".format(deletedRows, (time.time() - start_time) / 60 / 60))

    print("Updating rows with their reply...")
    rowCounter = 0
    pairedRows = 0
    for row in connection.execute('SELECT * FROM parent_reply'):
        parentId = row[0]
        commentId = row[1]
        replyScore = row[7]
        searchId = "t1_" + commentId
        reply = findReply(searchId)
        if reply and reply[1] > replyScore:
            addReplyComment(parentId, reply[0], reply[1])
            pairedRows += 1

        rowCounter += 1
        if rowCounter % 100000 == 0:
            print("Total rows read: {}, Paired Rows: {}, Elapsed Time(hrs): {}".format(rowCounter, pairedRows, (time.time() - start_time) / 60 / 60))
    if sql_transactions:
        executeBulkTransactions()
    print("Finished updating rows with their reply! Total comment-reply pairs: {}, Elapsed Time(hrs): {}".format(pairedRows, (time.time() - start_time) / 60 / 60))

    print("Removing rows with empty reply column...")
    rowCounter = 0
    removedRows = 0
    for row in connection.execute('SELECT * FROM parent_reply'):
        if not row[3]:
            deleteComment(row[0])
            removedRows += 1
        rowCounter += 1
        if rowCounter % 100000 == 0:
            print("Total rows read: {}, Removed Rows (Empty): {}, Elapsed Time(hrs): {}".format(rowCounter, removedRows, (time.time() - start_time) / 60 / 60))
    if sql_transactions:
        executeBulkTransactions()

    print("Finished! Total Removed Rows (Empty): {}, Total runtime (hrs): {} ".format(removedRows, (time.time() - start_time) / 60 / 60))

    # print("Dropping index...")
    # dropIndex()
    # print("Finish removing index!")