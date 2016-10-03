import sqlite3
from collections import deque

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import datetime
plt.style.use('ggplot')
matplotlib.use("svg")

# you should adjust this to fit your screen
matplotlib.rcParams['figure.figsize'] = (10.0, 5.0)
__author__ = 'jialingliu'


def load_data(fname):
    """ Read the given database into two pandas dataframes.

    Args:
        fname (string): filename of sqlite3 database to read

    Returns:
        (pd.DataFrame, pd.DataFrame): a tuple of two dataframes, the first for the vehicle data and the
                                      second for the prediction data.
    """
    conn = sqlite3.connect(fname)
    vdf = pd.read_sql(sql="SELECT * FROM vehicles WHERE `vid` IS NOT NULL AND `vid` != '' LIMIT 1000", con=conn, parse_dates=['tmstmp'])
    vdf['vid'] = vdf['vid'].astype(int)
    pdf = pd.read_sql(sql="SELECT * FROM predictions WHERE `vid` IS NOT NULL AND `vid` != '' LIMIT 1000", con=conn, parse_dates=['tmstmp', 'prdtm'])
    f = lambda x: len(str(x)) != 0
    pdf['dly'] = pdf['dly'].map(f)
    return vdf, pdf

# test
vdf, pdf = load_data('bus_aug23.db')
# Inspect the datatypes of the dataframe
# print vdf.dtypes
# print pdf.dtypes
#
# print len(vdf), len(pdf)
#
# # Inspect the first five entries of the dataframe
# print vdf.head()
# print pdf.head()


def split_trips(df):
    """ Splits the dataframe of vehicle data into a list of dataframes for each individual trip.

    Args:
        df (pd.DataFrame): A dataframe containing TrueTime bus data

    Returns:
        (list): A list of dataframes, where each dataFrame contains TrueTime bus data for a single bus running a
    """
    gb = df.groupby(by=['vid', 'rt', 'des', 'pid'])
    tmp = [gb.get_group(x).sort_values(by=['tmstmp', 'pdist']) for x in gb.groups]

    result = []
    for bus in tmp:
        length = len(bus.index)
        prev = 0
        for i in range(1, length):
            if (bus[i-1:i]['pdist'] > bus[i:i+1]['pdist']).real[0]:
                temp = bus[prev:i].set_index('tmstmp')
                result.append(temp)
                prev = i
        temp = bus[prev:length].set_index('tmstmp')
        result.append(temp)
    return result

# test
# all_trips = {rt: split_trips(vdf[vdf["rt"] == rt]) for rt in ["61A", "61B", "61C", "61D"]}
# print [(t, len(all_trips[t])) for t in all_trips]


# Sliding Averages
class SlidingAverage:

    def __init__(self, k):
        """ Initializes a sliding average calculator which keeps track of the average of the last k seen elements.

        Args:
            k (int): the number of elements to average (the half-width of the sliding average window)
        """
        self.k = k
        self.K = 2 * k + 1
        self.dq = deque()
        self.sum = 0
        self.num_cnt = 0
        self.avg_cnt = 0

    def update(self, x):
        """ Computes the sliding average after having seen element x

        Args:
            x (float): the next element in the stream to view

        Returns:
            (float): the new sliding average after having seen element x, if it can be calculated
        """
        # print "update", x, self.sum
        # print self.dq
        self.num_cnt += 1
        if len(self.dq) < self.K:
            self.sum += x
            self.dq.append(x)
            if len(self.dq) > self.k:
                self.avg_cnt += 1
                return self.sum / float(len(self.dq))
        else:
            first = self.dq.popleft()
            self.sum -= first
            self.sum += x
            self.dq.append(x)
            self.avg_cnt += 1
            return self.sum / float(self.K)

    def remain(self):
        # print "remain", self.sum, self.avg_cnt
        # print self.dq
        while self.avg_cnt < self.num_cnt:
            self.avg_cnt += 1
            if self.num_cnt - self.avg_cnt + 1 + self.k < len(self.dq):
                first = self.dq.popleft()
                self.sum -= first
            return self.sum / float(len(self.dq))
        return None


def compute_sliding_averages(s, k):
    """ Computes the sliding averages for a given Pandas series using the SlidingAverage class.

    Args:
        s (pd.Series): a Pandas series for which the sliding average needs to be calculated
        k (int): the half-width of the sliding average window

    Returns:
        (pd.Series): a Pandas series of the sliding averages

    """
    if k == 0:
        return s
    avg = []
    sa = SlidingAverage(k)
    for i in s.iteritems():
        new_average = sa.update(i[1])
        if new_average is not None:
            avg.append(new_average)
    new_average = sa.remain()
    while new_average is not None:
        avg.append(new_average)
        new_average = sa.remain()
    data = {'avg': avg}
    df = pd.DataFrame(data)
    return df['avg']

# test
# print compute_sliding_averages(pd.Series([1,2,3,4,5]),1)


def plot_trip(trips, k):
    """ Plots the sliding average speed as a function of time

    Args:
        trips (list): list of trip DataFrames to plot
        k (int): the half-width of the sliding average window
    """
    res = []
    for trip in trips:
        x = compute_sliding_averages(trip['spd'], k)
        y = trip.index.time
        res.extend(plt.plot(y, x))
    return plt

# test
# lines = plot_trip(all_trips['61A'][:20], 15)
# plt.show()


def plot_avg_spd(df, t):
    """ Plot the average speed of all recorded buses within t minute intervals
    Args:
        df (pd.DataFrame): dataframe of bus data
        t (int): the granularity of each time period (in minutes) for which an average is speed is calculated
    """
    df = df.set_index('tmstmp')
    gb = df.groupby(df.index.map(lambda tm: (tm.hour * 60 + tm.minute) - (tm.hour * 60 + tm.minute) % t))
    x = gb['spd'].mean()
    key = gb.groups.keys()
    key.sort()
    y = [datetime.time(k / 60, k % 60) for k in key]
    return plt.scatter(y, x)

# test
s = plot_avg_spd(vdf, 10)
plt.show()
