
# Assumptions:
1. request considered for the report is only: /__track/special/trackingevent
2. proper url (starting from http or https) is provided as url
3. user_id can be only 32 characters long md5 code
4. lang code is ignored and not part of the validation or report generation
5. all users, wiki and articles shared and same between all domains. Domain is not considered during report generation. 
6. there is trash domains in raw data, which should be filtered out. Only domains from provided list are in consideration
7. 'same time hits' problem:
   1. user hit few different articles at same time T1, for the same users no other hits except for T1:<br> 
      is_same_article = None, is_same_wiki = None since we can't tell which article/wiki was first or last
   2. user hit same article few times at same time T1, for the same users no other hits except for T1:<br>
      is_same_article = True, is_same_wiki = True, since we know for sure the users started and finished at same article
   3. user hit set of articles/wikis A1/W1 at same time T1, and same users hit set of articles/wikis A2/W2 at T2>T1:<br>
      1. A1 intersects A2: is_same_article = None, since we can't tell which exactly was firs/last
      2. A1 is not intersects with A2:  is_same_article = False, since we sure that user started and finished on different pages
      3. same for W1/W2 and is_same_wiki


# To run the process:
1. create resources directory in the project root
2. put data_sample.log at ./resources/data_sample.log 
3. run main.py
4. if completed ok, you should see following files:
   1. ./resources/log_clean_errors.csv: records from raw data which did not pass validation
   2. ./resources/log_cleaned.csv: cleaned record used for the report generation
   3. ./resources/user_articles_report.csv: the report output

#### If you want to rerun report generation based on log_cleaned.csv, update main.py accordingly to do not run cleaning process
#### If you want to see more columns in the report, change report_columns list in UserArticlesReport class 



# TODO:
1. move configuration variables from classes to configuration files and create configuration factory class 
2. create tests: unit, functional(cucumber)
3. report class also could return df with ignored records, like it done in cleaner 