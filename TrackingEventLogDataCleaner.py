import pandas as pd
import re
from logger import logger
from DataCleaner import DataCleaner

# TODO pass all constants to function as arguments

csv_columns = ["url", "lang_code", "timestamp", "user_id", "request"]
csv_sep = "|"

params1_expected_domains = ['fandom.com', 'futhead.com', 'gamepedia.com', 'muthead.com', 'wikia-services.com',
                            'wikia.org']
request_required_params = ["a", "c"]
request_required_params_validation_patterns = ["^\d+$", "^\d+$"]

parser_error_unexpected_count_of_params1 = "parser_error_unexpected_count_of_params1"
parser_error_unexpected_url_pattern = "parser_error_unexpected_url_pattern"
parser_error_unexpected_url_domain = "parser_error_unexpected_url_domain"
parser_error_unexpected_date_format = "parser_error_unexpected_date_format"
parser_error_unexpected_user_id_format = "parser_error_unexpected_user_id_format"
parser_error_unexpected_request_pattern = "parser_error_unexpected_request_pattern"
parser_error_unexpected_request_param_value = "parser_error_unexpected_request_param_value"

date_validation_pattern = "^<134>\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ$"
mgd5_validation_pattern = "^[a-f0-9]{32}$"
url_validation_patter = "^http[s]?.*[^.]+\.[^.]+[/]?"
request_validation_pattern = "^/?__track/special/trackingevent/?\?[^?]+$"


class TrackingEventLogDataCleaner(DataCleaner):
    def _validate_and_filter(self, df: pd.DataFrame, v: pd.Series, err_collector: list, err_msg: str):
        if sum(v.values) > 0:
            failed_df = df.loc[v.values][['source_row', 'raw']]

            failed_df.loc[:, 'err_msg'] = err_msg
            err_collector.append(failed_df)
            df = df[~v.values]

        return df

    def _check_params_count(self, df: pd.DataFrame, err_collector: list, err_msg: str):
        if len(df.index) == 0:
            return df
        df.loc[:, 'params1_count'] = df.loc[:, 'split'].map(len)
        v = df.loc[:, 'params1_count'] != len(csv_columns)
        df = self._validate_and_filter(df, v, err_collector, err_msg)
        df = df.drop('params1_count', axis=1)

        return df

    def _check_url_pattern(self, df: pd.DataFrame, err_collector: list, err_msg: str):
        if len(df.index) == 0:
            return df
        df.loc[:, 't'] = None  # TODO this is stupid, but without this line _check_url_domain will yell warning:
        # "A value is trying to be set on a copy of a slice from a DataFrame"
        v = df.loc[:, 'url'].map(lambda x: not bool(re.findall(url_validation_patter, x.lower().strip())))
        df = self._validate_and_filter(df, v, err_collector, err_msg)
        df = df.drop('t', axis=1)

        return df

    def _check_url_domain(self, df: pd.DataFrame, err_collector: list, err_msg: str):
        if len(df.index) == 0:
            return df
        df.loc[:, 'url_split'] = df.loc[:, 'url'].map(lambda x: x.strip("/").split("/"))
        df.loc[:, 'url_address'] = df.loc[:, 'url_split'].map(lambda x: x[2])
        df.loc[:, 'url_domain'] = df.loc[:, 'url_address'].map(lambda x: '.'.join(x.split(".")[-2:]))
        v = df.loc[:, 'url_domain'].map(lambda x: x.lower().strip() not in params1_expected_domains)
        df = self._validate_and_filter(df, v, err_collector, err_msg)
        df = df.drop('url_domain', axis=1)
        df = df.drop('url_address', axis=1)
        df = df.drop('url_split', axis=1)

        return df

    def _check_url(self, df: pd.DataFrame, err_collector: list):
        if len(df.index) == 0:
            return df
        # validate url protocol
        df = self._check_url_pattern(df, err_collector, parser_error_unexpected_url_pattern)

        # validate domain in expected list
        df = self._check_url_domain(df, err_collector, parser_error_unexpected_url_domain)

        return df

    def _check_timestamp(self, df: pd.DataFrame, err_collector: list, err_msg: str):
        if len(df.index) == 0:
            return df
        v = df.loc[:, 'timestamp'].map(lambda x: not bool(re.findall(date_validation_pattern, x)))
        df = self._validate_and_filter(df, v, err_collector, err_msg)

        return df

    def _check_user_id(self, df: pd.DataFrame, err_collector: list, err_msg: str):
        if len(df.index) == 0:
            return df
        v = df.loc[:, 'user_id'].map(lambda x: not bool(re.findall(mgd5_validation_pattern, x)))
        df = self._validate_and_filter(df, v, err_collector, err_msg)

        return df

    def _check_request(self, df: pd.DataFrame, err_collector: list, err_msg: str):
        if len(df.index) == 0:
            return df
        v = df.loc[:, 'request'].map(lambda x: not bool(re.findall(request_validation_pattern, x.lower().strip())))
        df = self._validate_and_filter(df, v, err_collector, err_msg)

        return df

    def _get_required_params_set(self, x):
        d = dict.fromkeys(request_required_params)
        for kv in x:
            k, v = kv.split("=")
            if k in d:
                d[k] = v

        r = []
        for x in request_required_params:
            r.append(d[x])
        return r

    def _add_required_params_from_request(self, df: pd.DataFrame):
        if len(df.index) == 0:
            return df
        df.loc[:, 'request_params'] = df.loc[:, 'request'].map(lambda x: x.split("?")[1])
        df.loc[:, 'request_params'] = df.loc[:, 'request_params'].map(lambda x: x.split("&"))
        df.loc[:, 'request_params_tmp'] = df.loc[:, 'request_params'].map(self._get_required_params_set)
        df[request_required_params] = pd.DataFrame(df.loc[:, 'request_params_tmp'].tolist(), index=df.index)
        df = df.drop('request_params_tmp', axis=1)
        df = df.drop('request_params', axis=1)

        return df

    def _check_required_params_from_request(self, df: pd.DataFrame, err_collector: list, err_msg: str):
        if len(df.index) == 0:
            return df
        for i, c in enumerate(request_required_params):
            v = df.loc[:, c].map(
                lambda x: not x or not bool(re.findall(request_required_params_validation_patterns[i], x)))
            df = self._validate_and_filter(df, v, err_collector,
                                           err_msg + ", missing or unexpected value of parameter " + c)

        return df

    def clean(self):
        # source raw data in single column table
        log_df = self._raw_data_provider.provide()
        raw_count = len(log_df.index)
        logger.debug(f"Raw file records count: {raw_count}")
        log_df = log_df.set_axis(["raw"], axis=1)

        # store records row numbers from raw data in source_row column
        log_df.loc[:, 'source_row'] = log_df.index
        log_df_failed_to_parse = []

        # do the first split
        log_df.loc[:, 'split'] = log_df.loc[:, 'raw'].str.split(csv_sep)

        # validate number of parameters
        log_df = self._check_params_count(log_df, log_df_failed_to_parse, parser_error_unexpected_count_of_params1)

        # convert split to columns
        log_df[csv_columns] = pd.DataFrame(log_df.loc[:, 'split'].tolist(), index=log_df.index)
        log_df = log_df.drop("split", axis=1)

        # check url
        log_df = self._check_url(log_df, log_df_failed_to_parse)

        # check timestamp
        log_df = self._check_timestamp(log_df, log_df_failed_to_parse, parser_error_unexpected_date_format)

        # check user id
        log_df = self._check_user_id(log_df, log_df_failed_to_parse, parser_error_unexpected_user_id_format)

        # check request
        log_df = self._check_request(log_df, log_df_failed_to_parse, parser_error_unexpected_request_pattern)

        # add as columns values from request
        log_df = self._add_required_params_from_request(log_df)

        # validate new columns values
        log_df = self._check_required_params_from_request(log_df, log_df_failed_to_parse,
                                                          parser_error_unexpected_request_param_value)

        # drop raw data
        log_df = log_df.drop('raw', axis=1)

        # build and writing df with all errors
        log_df_errors = pd.DataFrame()
        if log_df_failed_to_parse:
            log_df_errors = pd.concat(log_df_failed_to_parse, ignore_index=True)

        cleaned_count = len(log_df.index)
        if cleaned_count == 0:
            logger.fatal(f"Something went wrong, there is no valid records after validation")
        elif cleaned_count != raw_count:
            logger.warning(
                f"Some of the records did not pass validation. Raw records:{raw_count}, out records: {cleaned_count}")

        return log_df[csv_columns + request_required_params + ["source_row"]], log_df_errors[["source_row", "err_msg", "raw"]]
