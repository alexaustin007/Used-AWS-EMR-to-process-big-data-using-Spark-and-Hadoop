from pyspark.sql import SparkSession
from pyspark.sql.functions import col

S3_DATA_SOURCE_PATH = 's3://alex-emr-bucket/data-source/survey_results_public.csv'
S3_DATA_OUTPUT_PATH = 's3://alex-emr-bucket/data-output/output_results.csv'

def main():
    spark = SparkSession.builder.appName('alex_emr_project_xu').getOrCreate()
    all_data = spark.read.csv(S3_DATA_SOURCE_PATH, header = True)
    print("Total no of records in the source data: %s" % all_data.count())
    selected_data = all_data.where((col('Country') == 'United States') & (col('WorkWeekHrs') > 45) )
    print('the no of engineers who work more than 45 hours is: %s' % selected_data.count())
    selected_data.write.mode('overwrite').csv(S3_DATA_OUTPUT_PATH)
    print('Selected data was successfully saved to s3:%s' % S3_DATA_OUTPUT_PATH)


if __name__ == '__main__':
    main()
