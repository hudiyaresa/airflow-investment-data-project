from airflow.exceptions import AirflowException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import sys

# Define paths for transformed, valid, and invalid data
transformed_data_path = 's3a://transformed-data/'
valid_data_path = 's3a://valid-data/'
invalid_data_path = 's3a://invalid-data/'

# Define PostgreSQL connection properties
postgres_warehouse = "jdbc:postgresql://warehouse_db:5432/warehouse_db"
postgres_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

class ExtractTransform:

    @staticmethod
    def _dim_company(incremental, date):
        try:
            spark = SparkSession.builder.appName("Transform - dim_company").getOrCreate()
            query = "(SELECT * FROM staging.company) as data"
            df = spark.read.jdbc(url=postgres_warehouse, table=query, properties=postgres_properties)

            if df.isEmpty():
                spark.stop()
                print("dim_company has no data. Skipped...")
                return

            df = df.select('object_id', 'region', 'city', 'country_code', 'latitude', 'longitude') \
                   .withColumnRenamed('object_id', 'company_nk') \
                   .withColumn('company_id', F.monotonically_increasing_id())

            df.write.format("csv") \
                    .option("header", "true").option("delimiter", ";") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/dim_company")

            spark.stop()
        except Exception as e:
            raise AirflowException(f"Error transforming dim_company: {e}")

    @staticmethod
    def _dim_date(incremental, date):
        try:
            spark = SparkSession.builder.appName("Transform - dim_date").getOrCreate()
            query = "(SELECT * FROM dim_date) as data"
            df = spark.read.jdbc(url=postgres_warehouse, table=query, properties=postgres_properties)

            if df.isEmpty():
                spark.stop()
                print("dim_date has no data. Skipped...")
                return

            df = df \
                .withColumn("date_id", F.col("date_id").cast("int")) \
                .withColumn("date_actual", F.to_date("date_actual", "yyyy-MM-dd")) \
                .withColumn("day_of_year", F.col("day_of_year").cast("int")) \
                .withColumn("week_of_month", F.col("week_of_month").cast("int")) \
                .withColumn("week_of_year", F.col("week_of_year").cast("int")) \
                .withColumn("month_actual", F.col("month_actual").cast("int")) \
                .withColumn("quarter_actual", F.col("quarter_actual").cast("int")) \
                .withColumn("year_actual", F.col("year_actual").cast("int")) \
                .withColumn("first_day_of_week", F.to_date("first_day_of_week", "yyyy-MM-dd")) \
                .withColumn("last_day_of_week", F.to_date("last_day_of_week", "yyyy-MM-dd")) \
                .withColumn("first_day_of_month", F.to_date("first_day_of_month", "yyyy-MM-dd")) \
                .withColumn("last_day_of_month", F.to_date("last_day_of_month", "yyyy-MM-dd")) \
                .withColumn("first_day_of_quarter", F.to_date("first_day_of_quarter", "yyyy-MM-dd")) \
                .withColumn("last_day_of_quarter", F.to_date("last_day_of_quarter", "yyyy-MM-dd")) \
                .withColumn("first_day_of_year", F.to_date("first_day_of_year", "yyyy-MM-dd")) \
                .withColumn("last_day_of_year", F.to_date("last_day_of_year", "yyyy-MM-dd"))

            df.write.format("csv") \
                    .option("header", "true").option("delimiter", ";") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/dim_date")

            spark.stop()
        except Exception as e:
            raise AirflowException(f"Error transforming dim_date: {e}")

    @staticmethod
    def _dim_funding_round(incremental, date):
        try:
            spark = SparkSession.builder.appName("Transform - dim_funding_round").getOrCreate()
            query = "(SELECT * FROM staging.funding_rounds) as data"
            df = spark.read.jdbc(url=postgres_warehouse, table=query, properties=postgres_properties)

            if df.isEmpty():
                spark.stop()
                print("dim_funding_round has no data. Skipped...")
                return

            df = df.select('funding_round_code', 'funding_round_type', 'is_first_round', 'is_last_round') \
                   .withColumnRenamed('funding_round_code', 'funding_code') \
                   .withColumn('funding_round_id', F.monotonically_increasing_id()) \
                   .withColumn('is_first_round', F.col('is_first_round').cast('boolean')) \
                   .withColumn('is_last_round', F.col('is_last_round').cast('boolean'))

            df.write.format("csv") \
                    .option("header", "true").option("delimiter", ";") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/dim_funding_round")

            spark.stop()
        except Exception as e:
            raise AirflowException(f"Error transforming dim_funding_round: {e}")

    @staticmethod
    def _dim_investor(incremental, date):
        try:
            spark = SparkSession.builder.appName("Transform - dim_investor").getOrCreate()
            query = "(SELECT * FROM staging.investments) as data"
            df = spark.read.jdbc(url=postgres_warehouse, table=query, properties=postgres_properties)

            if df.isEmpty():
                spark.stop()
                print("dim_investor has no data. Skipped...")
                return

            df = df.select('investor_object_id').distinct() \
                   .withColumnRenamed('investor_object_id', 'investor_nk') \
                   .withColumn('investor_id', F.monotonically_increasing_id())

            df.write.format("csv") \
                    .option("header", "true").option("delimiter", ";") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/dim_investor")

            spark.stop()
        except Exception as e:
            raise AirflowException(f"Error transforming dim_investor: {e}")

    @staticmethod
    def _fact_company_growth(incremental, date):
        try:
            spark = SparkSession.builder.appName("Transform - fact_company_growth").getOrCreate()
            acq = spark.read.jdbc(url=postgres_warehouse, table="(SELECT * FROM staging.acquisition) as data", properties=postgres_properties)
            fr = spark.read.jdbc(url=postgres_warehouse, table="(SELECT * FROM staging.funding_rounds) as data", properties=postgres_properties)
            ip = spark.read.jdbc(url=postgres_warehouse, table="(SELECT * FROM staging.ipos) as data", properties=postgres_properties)
            comp = spark.read.jdbc(url=postgres_warehouse, table="(SELECT * FROM staging.company) as data", properties=postgres_properties)
            dd = spark.read.jdbc(url=postgres_warehouse, table="(SELECT * FROM dim_date) as data", properties=postgres_properties)

            if acq.isEmpty() and fr.isEmpty() and ip.isEmpty():
                spark.stop()
                print("fact_company_growth has no data. Skipped...")
                return

            acq_t = acq.withColumn('company_nk', F.col('acquired_object_id')) \
                       .withColumn('date_actual', F.to_date('acquired_at', 'yyyy-MM-dd')) \
                       .join(dd, acq.date_actual == dd.date_actual, 'left') \
                       .drop('date_actual') \
                       .groupBy('company_nk', 'date_id') \
                       .agg(F.count('acquisition_id').alias('acquisition_count'))

            fr_t = fr.withColumn('company_nk', F.col('object_id')) \
                     .groupBy('company_nk') \
                     .agg(F.sum('raised_amount_usd').alias('total_funding_usd'))

            ip_t = ip.withColumn('company_nk', F.col('object_id')) \
                     .withColumn('ipo_valuation_usd', F.col('valuation_amount').cast('float')) \
                     .withColumn('ipo_raised_amount_usd', F.col('raised_amount').cast('float'))

            df = acq_t.join(fr_t, 'company_nk', 'left') \
                     .join(ip_t, 'company_nk', 'left') \
                     .join(comp, 'company_nk', 'left') \
                     .select('company_nk', 'date_id', 'acquisition_count', 'total_funding_usd', 'ipo_valuation_usd', 'ipo_raised_amount_usd')

            df.write.format("csv") \
                    .option("header", "true").option("delimiter", ";") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/fact_company_growth")

            spark.stop()
        except Exception as e:
            raise AirflowException(f"Error transforming fact_company_growth: {e}")

    @staticmethod
    def _fact_investments(incremental, date):
        try:
            spark = SparkSession.builder.appName("Transform - fact_investments").getOrCreate()
            inv = spark.read.jdbc(url=postgres_warehouse, table="(SELECT * FROM staging.investments) as data", properties=postgres_properties)
            fr = spark.read.jdbc(url=postgres_warehouse, table="(SELECT funding_round_id, raised_amount_usd FROM staging.funding_rounds) as data", properties=postgres_properties)
            dd = spark.read.jdbc(url=postgres_warehouse, table="(SELECT * FROM dim_date) as data", properties=postgres_properties)

            if inv.isEmpty():
                spark.stop()
                print("fact_investments has no data. Skipped...")
                return

            inv_t = inv.withColumn('investment_nk', F.col('investment_id')) \
                       .withColumn('date_actual', F.to_date('created_at', 'yyyy-MM-dd'))
            inv_t = inv_t.join(dd, inv_t.date_actual == dd.date_actual, 'left') \
                         .drop('date_actual') \
                         .withColumnRenamed('date_id', 'date_id')

            df = inv_t.join(fr, inv_t.funding_round_id == fr.funding_round_id, 'left') \
                      .select('investment_nk', 'investor_object_id', 'funded_object_id',
                              'funding_round_id', 'date_id', 'raised_amount_usd')

            df.write.format("csv") \
                    .option("header", "true").option("delimiter", ";") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/fact_investments")

            spark.stop()
        except Exception as e:
            raise AirflowException(f"Error transforming fact_investments: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: extract_transform.py <function_name> <incremental> <date>")
        sys.exit(-1)

    function_name = sys.argv[1]
    incremental = sys.argv[2].lower() == 'true'
    date = sys.argv[3]

    # Dictionary mapping function names to their corresponding methods
    function_map = {
        "dim_company": ExtractTransform._dim_company,
        "dim_date": ExtractTransform._dim_date,
        "dim_funding_round": ExtractTransform._dim_funding_round,
        "dim_investor": ExtractTransform._dim_investor,
        "fact_company_growth": ExtractTransform._fact_company_growth,
        "fact_investments": ExtractTransform._fact_investments
    }

    if function_name in function_map:
        function_map[function_name](incremental, date)
    else:
        print(f"Unknown function name: {function_name}")
        sys.exit(-1)