from github_views import GitHubViews
from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
    Usage: validate-damons-datalake
    Validation job to ensure everything is working well
    """

    spark = (
        SparkSession.builder.appName("CurateDamonsDatalake")
        .config("spark.sql.catalogImplementation", "hive")
        .config(
            "spark.hadoop.hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        )
        .getOrCreate()
    )

    # Read our data from the appropriate raw table
    df = spark.read.json("s3://ant309-test-807427769151/sample-data/aws-samples/emr-serverless/data.json")

    # Step 1 - Get daily views and uniques per repo
    gh = GitHubViews()
    daily_metrics = gh.extract_latest_daily_metrics(df)

    validation_df = daily_metrics.filter(daily_metrics.repo == 'aws-samples/emr-serverless')

    count = validation_df.count()
    min_views = validation_df.agg({"count": "min"}).collect()[0][0]
    max_views = validation_df.agg({"count": "max"}).collect()[0][0]
    assert count>=90, f"expected >=90 records, got: {count}. failing job."
    assert min_views<=max_views, f"expected min_views ({min_views}) <= max_views ({max_views}). failing job."
    assert min_views>0, f"expected min_views>0, got {min_views}. failing job."
    assert max_views>500, f"expected max_views>500, got {max_views}. failing job."
