import csv
import math
import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
# Use svg backend for better quality
import matplotlib
import time

matplotlib.use("svg")
plt.style.use('ggplot')
# you should adjust this to fit your screen
matplotlib.rcParams['figure.figsize'] = (10.0, 5.0)

__author__ = 'jialingliu'


def load_twitter_data_sqlite3(conn, users_filepath, edges_filepath, tweets_filepath) :
    """ Load twitter data in the three files as tables into an in-memory SQLite database
    Input:
        conn (sqlite3.Connection) : Connection object corresponding to the database; used to perform SQL commands.
        users_filepath (str) : absolute/relative path to users.csv file
        edges_filepath (str) : absolute/relative path to edges.csv file
        tweets_filepath (str) : absolute/relative path to tweets.csv file
    Output:
        None
    """
    cursor = conn.cursor()
    cursor.execute("drop table if EXISTS users")
    cursor.execute("drop table if EXISTS tweets")
    cursor.execute("drop table if EXISTS edges")
    users_sql = "CREATE TABLE IF NOT EXISTS users (" \
                "name TEXT, screen_name TEXT, " \
                "location TEXT, created_at TEXT, " \
                "friends_count INTEGER, followers_count INTEGER, " \
                "statuses_count INTEGER, favourites_count INTEGER)"
    edges_sql = "CREATE TABLE IF NOT EXISTS edges (screen_name TEXT, friend TEXT)"
    tweets_sql = "CREATE TABLE IF NOT EXISTS tweets (" \
                 "screen_name TEXT, created_at TEXT, " \
                 "retweet_count INTEGER, favorite_count INTEGER, " \
                 "text TEXT)"
    cursor.execute(users_sql)
    cursor.execute(edges_sql)
    cursor.execute(tweets_sql)
    conn.commit()

    with open(users_filepath) as f:
        u = csv.reader(f)
        next(u, None)
        for row in u:
            cursor.execute("INSERT INTO users VALUES (?,?,?,?,?,?,?,?);", row)
    conn.commit()

    with open(edges_filepath) as f:
        e = csv.reader(f)
        next(e, None)
        for row in e:
            cursor.execute("INSERT INTO edges VALUES (?,?);", row)
    conn.commit()

    with open(tweets_filepath) as f:
        t = csv.reader(f)
        next(t, None)
        for row in t:
            cursor.execute("INSERT INTO tweets VALUES (?,?,?,?,?);", row)
    conn.commit()

# test
conn = sqlite3.connect(":memory:")
conn.text_factory = str
# call to your function
load_twitter_data_sqlite3(conn, 'users.csv', 'edges.csv', 'tweets.csv')
# make sure to change the path to csv files appropriately
cursor = conn.cursor()
# prints all tables in the database
for row in cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table';"):
    print row
for row in cursor.execute("SELECT Count(*) FROM users"):
    print row
for row in cursor.execute("SELECT Count(*) FROM edges"):
    print row
for row in cursor.execute("SELECT Count(*) FROM tweets"):
    print row


def trending_tweets(cursor, topical_phrases=['Hillary', 'Clinton'], N=5):
    """ Retrieves the top N trending tweets containing one or more of the given topical phrases.
    Input:
        cursor (sqlite3.Cursor): Cursor object to query the database.
        topical_phrases (list of strings): A list of keywords identifying a topic.
        N: Number of trending tweets to retrieve
    Output:
        results (sqlite3.Cursor): Cursor object which can be used to iterate over the retrieved records/tuples.
    """
    # cannot pass test
    # s = "SELECT DISTINCT tweet, trending_score FROM (SELECT `text` AS tweet, retweet_count + favorite_count AS trending_score FROM tweets WHERE `text` LIKE "
    # prefix = ""
    # for phrase in topical_phrases:
    #     s += prefix + "'%{}%'".format(phrase)
    #     prefix = " OR `text` LIKE "
    # s += " ORDER BY trending_score DESC, tweet ASC) LIMIT {}".format(N)
    # query = s
    # results = cursor.execute(query)
    # return results
    query = "SELECT DISTINCT(text) AS tweet, (retweet_count + favorite_count) AS trending_score FROM tweets WHERE " + " OR ".join(("tweet LIKE '%" + phrase + "%'" for phrase in topical_phrases)) + " ORDER BY trending_score DESC LIMIT " + str(N) # your query here
    results = cursor.execute(query)
    return results

# test
results = trending_tweets(conn.cursor())
for row in results:
    print row


def num_tweets_in_feed(cursor):
    """ Retrieves the number of tweets STR recommends to each Twitter user.
    Input:
        cursor (sqlite3.Cursor): Cursor object to query the database.
    Output:
        results (sqlite3.Cursor): Cursor object which can be used to iterate over the retrieved records/tuples.
    """

    query = "SELECT a1.screen_name, a1.cnt + ifnull(a2.cnt, 0) as num_tweets FROM (SELECT screen_name, 0 as cnt FROM users) AS a1 LEFT JOIN (SELECT edges.screen_name, COUNT(*) AS cnt FROM edges, tweets WHERE edges.friend = tweets.screen_name GROUP BY edges.screen_name) AS a2 ON a1.screen_name == a2.screen_name"
    return cursor.execute(query)

t1 = time.time()
results = num_tweets_in_feed(conn.cursor())
count = 0
for row in results:
    count += 1
t2 = time.time()
print t2 - t1
# print results

# debug
# results = num_tweets_in_feed(conn.cursor())
# i = 0
# for row in results:
#     # if row[1] == 0:
#     i += 1
#     print row
#     # if i > 20:
#     #     break
#     # i += 1
#
# print i

# check edges
# slaves = []
# # masters = []
# for row in cursor.execute(query):
#     print row
#     slaves.append(row[0])
#     masters.append(row[1])
#
# original_slaves = []
# original_masters = []
# count = 0
# with open("edges.csv", 'r') as f:
#     for line in f:
#         if count == 0:
#             count += 1
#             continue
#         split_line = line.split(",")
#         original_slaves.append(split_line[0])
#         original_masters.append(split_line[1])
# with open("edges.csv", 'r') as f:
#         edges_file = f.read()
#
# for row in csv.reader(edges_file.splitlines(), delimiter=','):
#     if count == 0:
#             count += 1
#             continue
#     # split_line = row.split(",")
#     original_slaves.append(row[0])
#     original_masters.append(row[1])
#
# c1 = 0
# c2 = 0
# for i in range(len(original_slaves)):
#     if original_slaves[i] != slaves[i]:
#         c1 += 1
#         print "slave: ", slaves[i], "!=", original_slaves[i]
#
# for i in range(len(original_masters)):
#     if original_masters[i] != masters[i]:
#         c2 += 1
#         print masters[i][-1] + "!=" + original_masters[i][-1]
# print c1, c2


def load_twitter_data_pandas(users_filepath, edges_filepath, tweets_filepath):
    """ Loads the Twitter data from the csv files into Pandas dataframes
    Input:
        users_filepath (str) : absolute/relative path to users.csv file
        edges_filepath (str) : absolute/relative path to edges.csv file
        tweets_filepath (str) : absolute/relative path to tweets.csv file
    Output:
        (pd.DataFrame, pd.DataFrame, pd.DataFrame) : A tuple of three dataframes, the first one for users,
                                                    the second for edges and the third for tweets.
    """
    users = pd.read_csv(users_filepath, na_filter=False)
    edges = pd.read_csv(edges_filepath, na_filter=False)
    tweets = pd.read_csv(tweets_filepath, na_filter=False)
    return users, edges, tweets

# test
(users_df, edges_df, tweets_df) = load_twitter_data_pandas('users.csv', 'edges.csv', 'tweets.csv')
# make sure to change the path to csv files appropriately
print users_df.head()
print edges_df.head()
print tweets_df.head()


def plot_friends_vs_followers(users_df):
    """ Plots the friends_count (on y-axis) against the followers_count (on x-axis).
    Input:
        users_df (pd.DataFrame) : Dataframe containing Twitter user attributes,
                                    as returned by load_twitter_data_pandas()
    Output:
        (matplotlib.collections.PathCollection) : The object returned by the scatter plot function
    """
    y = users_df['friends_count']
    x = users_df['followers_count']
    return plt.scatter(x, y)


p = plot_friends_vs_followers(users_df)
plt.show()


def average(x):
    assert len(x) > 0
    return float(sum(x)) / len(x)


def pearson_def(x, y):
    assert len(x) == len(y)
    n = len(x)
    assert n > 0
    avg_x = average(x)
    avg_y = average(y)
    diffprod = 0
    xdiff2 = 0
    ydiff2 = 0
    for idx in range(n):
        xdiff = x[idx] - avg_x
        ydiff = y[idx] - avg_y
        diffprod += xdiff * ydiff
        xdiff2 += xdiff * xdiff
        ydiff2 += ydiff * ydiff

    return diffprod / math.sqrt(xdiff2 * ydiff2)


def correlation_coefficient(users_df):
    """ Plots the friends_count (on y-axis) against the followers_count (on x-axis).
    Input:
        users_df (pd.DataFrame) : Dataframe containing Twitter user attributes,
                                    as returned by load_twitter_data_pandas()
    Output:
        (double) : correlation coefficient between friends_count and followers_count
    """
    # n = len(users_df.index)
    # d1 = (users_df['friends_count'] * users_df['followers_count']).sum
    return pearson_def(users_df['friends_count'], users_df['followers_count'])

# test
print correlation_coefficient(users_df)


def degree_distribution(edges_df):
    """ Plots the distribution of .
    Input:
        edges_df (pd.DataFrame) : Dataframe containing Twitter edges,
                        as returned by load_twitter_data_pandas()
    Output:
        (array, array, list of Patch objects) : Tuple of the values of the histogram bins,
                        the edges of the bins and the silent list of individual patches used to create the histogram.
    """
    counts = edges_df['screen_name'].value_counts().to_dict()
    result = plt.hist(counts.values())
    return result

# test
degree_distribution(edges_df)