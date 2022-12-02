import sys

from github_views import GitHubViews
from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
    Usage: curate-damons-datalake <output-s3-uri>
    Creates curated datasets from Damon's Datalake
    """
    output_s3_uri = ""
    if len(sys.argv) == 2:
        output_s3_uri = sys.argv[1]
    else:
        raise Exception.new("Output S3 URI required as 1st parameter")

    spark = (
        SparkSession.builder.appName("CurateDamonsDatalake")
        .getOrCreate()
    )

    # Read our data from the appropriate raw table
    df = spark.read.json("s3://ant309-prod-807427769151/sample-data/aws-samples/emr-serverless/data.json")

    # Step 1 - Get daily views and uniques per repo
    gh = GitHubViews()
    daily_metrics = gh.extract_latest_daily_metrics(df)

    # Write out our beautiful data
    daily_metrics.write.mode("overwrite").parquet(output_s3_uri)
