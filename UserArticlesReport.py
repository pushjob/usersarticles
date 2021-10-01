import pandas as pd
from logger import logger

date_conversion_format = '<134>%Y-%m-%dT%H:%M:%SZ'
# available columns for the report: user_id,a_first,c_first,a_last,c_last,is_same_article,is_same_wiki
report_columns = ["user_id", "is_same_article", "is_same_article"]
input_columns = ["user_id", "timestamp", "a", "c"]


class UserArticlesReport:
    def __init__(self, event_log_data_provider):
        self._event_log_data_provider = event_log_data_provider

    def report(self):
        df = self._event_log_data_provider.provide()

        # merge wiki id and article id to same column to make article id unique
        df = df[input_columns] # extra validation of the input interface, if columns upstream be renamed, please add mapper
        df["a"] = df["c"].map(str) + "-" + df["a"].map(str)

        # convert string timestamp to datetime
        df["timestamp"] = pd.to_datetime(df['timestamp'], format=date_conversion_format)

        # find same time hit to different articles, and put articles in a list
        df = df.groupby(["user_id", "timestamp"]).aggregate(lambda x: list(x)).reset_index()

        # count hits for every user at same time
        df["same_time_hits_count"] = df["a"].map(len)

        # leave only unique values for pages hit at same time
        df["a"] = df["a"].map(set)
        df["c"] = df["c"].map(set)

        # find first and last hit for every user, and count of hits at different time
        g = df.groupby(["user_id"])
        df["first_hit"] = g["timestamp"].transform(max)
        df["last_hit"] = g["timestamp"].transform(min)
        df["hits_count"] = g["user_id"].transform('count')

        # leave only users with more than 1 hit
        df = df[df["same_time_hits_count"] + df["hits_count"] - pd.Series([1 for x in range(len(df.index))]) > 1]
        df = df.drop(["same_time_hits_count", "hits_count"], axis=1)


        # split first and last hits to 2 df
        first_hits_df = df[df["first_hit"] == df["timestamp"]] \
            .drop(["timestamp", "last_hit", "first_hit"], axis=1) \
            .rename({"a": "a_first", "c": "c_first"}, axis=1)

        last_hits_df = df[df["last_hit"] == df["timestamp"]] \
            .drop(["timestamp", "first_hit", "last_hit"], axis=1) \
            .rename({"a": "a_last", "c": "c_last"}, axis=1)

        # validate counts
        if len(first_hits_df.index) != len(last_hits_df.index):
            logger.warn("something went wrong, count of first and last hits should be same")

        report_df = pd.merge(first_hits_df, last_hits_df, on=["user_id", ])

        # validate counts
        if len(first_hits_df.index) != len(report_df.index):
            logger.warn("something went wrong, count of first and report df should be same")

        def is_same(x, ac: str):
            x1 = x[ac + '_last'].copy()
            x2 = x[ac + '_first'].copy()

            if len(x2) == 1 and len(x1) == 1:
                return x1.pop() == x2.pop()
            else:
                ints = x1.intersection(x2)
                if len(ints) == 0:
                    return False
                else:
                    return None

        report_df["is_same_article"] = report_df.apply(lambda x: is_same(x, "a"), axis=1)
        report_df["is_same_wiki"] = report_df.apply(lambda x: is_same(x, "c"), axis=1)

        return report_df[report_columns]
