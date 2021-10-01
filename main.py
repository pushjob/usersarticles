from TrackingEventLogRawDataProvider import TrackingEventLogRawDataProvider
from TrackingEventLogCleanedDataWriter import TrackingEventLogCleanedDataWriter
from TrackingEventLogRawErrorsDataWriter import TrackingEventLogRawErrorsDataWriter
from TrackingEventLogDataCleaner import TrackingEventLogDataCleaner
from TrackingEventLogDataProvider import TrackingEventLogDataProvider
from UserArticlesReport import UserArticlesReport
from UserArticlesReportWriter import UserArticlesReportWriter


def main():
    # run data cleaner
    if True:
        raw_data_provider = TrackingEventLogRawDataProvider()
        clean_data_writer = TrackingEventLogCleanedDataWriter()
        errors_data_writer = TrackingEventLogRawErrorsDataWriter()
        data_cleaner = TrackingEventLogDataCleaner(raw_data_provider)
        df, df_errors = data_cleaner.clean()
        clean_data_writer.write(df)
        errors_data_writer.write(df_errors)

    # prepare the report
    event_log_data_provider = TrackingEventLogDataProvider()
    reporter = UserArticlesReport(event_log_data_provider)
    report_df = reporter.report()

    # write the report
    report_writer = UserArticlesReportWriter()
    report_writer.write(report_df)

main()