from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.models import Variable, TaskInstance

from etl_pipeline.tasks.warehouse.components.validations import Validation, ValidationType
from helper.minio import CustomMinio

import re
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql import functions as F
import sys
from io import BytesIO
import tempfile
import os


transformed_data_path = 's3a://transformed-data/'
valid_data_path = 's3a://valid-data/'
invalid_data_path = 's3a://invalid-data/'


postgres_staging = "jdbc:postgresql://staging_db:5432/staging_db"
postgres_warehouse = "jdbc:postgresql://warehouse_db:5432/warehouse_db"
postgres_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

class ExtractTransform:
    @staticmethod
    def _categories(incremental, date):
        try:
            spark = SparkSession.builder \
                .appName("ETL Pipeline - Categories") \
                .getOrCreate()

            query = "(SELECT * FROM staging.categories) as data"
            if incremental:
                query = f"(SELECT * FROM staging.categories WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY') as data"

            df = spark.read.jdbc(url = postgres_staging, 
                                 table = query, 
                                 properties = postgres_properties)
            
            if not df.isEmpty():
                spark.stop()
                print("categories doesn't have new data. Skipped...")
                return
            else:
                df.show(5)
                df = df.withColumnRenamed('category', 'category_nk') \
                       .withColumnRenamed('categoryname', 'category_name') \
                       .dropDuplicates(['category_nk']) \
                       .drop('created_at')
                
                df.write \
                    .format("csv") \
                    .option("header", "true") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/categories")
            
            spark.stop()

        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from categories: {e}")
        

    @staticmethod
    def _customers(incremental, date):
        try:
            spark = SparkSession.builder \
                .appName("ETL Pipeline - Customers") \
                .getOrCreate()

            query = "(SELECT * FROM staging.customers) as data" 
            if incremental:
                query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

            df = spark.read.jdbc(url = postgres_staging, 
                                 table = query, 
                                 properties = postgres_properties)
            
            if not df.isEmpty():
                spark.stop()
                print("customers doesn't have new data. Skipped...")
                return            
            else:                
                # Rename columns
                df = df.withColumnRenamed('customerid', 'customer_nk') \
                       .withColumnRenamed('firstname', 'first_name') \
                       .withColumnRenamed('lastname', 'last_name') \
                       .withColumnRenamed('creditcardtype', 'credit_card_type') \
                       .withColumnRenamed('creditcard', 'credit_card') \
                       .withColumnRenamed('creditcardexpiration', 'credit_card_expiration')

                # deduplication based on customer_nk
                df = df.dropDuplicates(['customer_nk'])

                # Masking credit card number
                df = df.withColumn('credit_card', 
                    F.concat(
                        F.regexp_replace(F.substring(F.col('credit_card'), 1, -4), r'\d', 'X'),
                        F.substring(F.col('credit_card'), -4, 4)
                    )
                )

                # drop column created_at
                df = df.drop('created_at')

                df.write \
                    .format("csv") \
                    .option("header", "true") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/customers")

            spark.stop()
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from customers: {e}")

    @staticmethod
    def _products(incremental, date):
        try:
            spark = SparkSession.builder \
                .appName("ETL Pipeline - Products") \
                .getOrCreate()

            query = "(SELECT * FROM staging.products) as data" 
            if incremental:
                query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

            df = spark.read.jdbc(url = postgres_staging, 
                                 table = query, 
                                 properties = postgres_properties)
            
            if not df.isEmpty():
                spark.stop()
                print("products doesn't have new data. Skipped...")
                return
            
            else:
                # Rename columns
                df = df.withColumnRenamed('prod_id', 'product_nk') \
                       .withColumnRenamed('category', 'category_nk')

                # deduplication based on product_nk
                df = df.dropDuplicates(['product_nk'])

                # Extract data from the `categories` table
                categories_df = spark.read.jdbc(url = postgres_warehouse, 
                                 table = "(SELECT * FROM categories) as data", 
                                 properties = postgres_properties)
                
                # Lookup `category_id` from `categories` table based on `category`
                df = df.join(categories_df, df.category_nk == categories_df.category_nk, "left") \
                       .select(df['*'], categories_df.category_id)

                # drop columns
                df = df.drop('created_at', 'category_nk')

                df.write \
                    .format("csv") \
                    .option("header", "true") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/products")

            spark.stop()
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from products: {e}")

    @staticmethod
    def _inventory(incremental, date):
        try:
            spark = SparkSession.builder \
                .appName("ETL Pipeline - Inventory") \
                .getOrCreate()
            
            query = "(SELECT * FROM staging.inventory) as data" 
            if incremental:
                query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

            df = spark.read.jdbc(
                url = postgres_staging, 
                table = query, 
                properties = postgres_properties
            )
            
            if not df.isEmpty():
                spark.stop()
                print("inventory doesn't have new data. Skipped...")
                return
            
            else:
                # Rename columns
                df = df.withColumnRenamed('prod_id', 'product_nk') \
                       .withColumnRenamed('quan_in_stock', 'quantity_stock')

                # deduplication based on product_nk
                df = df.dropDuplicates(['product_nk'])

                # Extract data from the `products` table
                products_df = spark.read.jdbc(url = postgres_warehouse, 
                                 table = "(SELECT * FROM products) as data", 
                                 properties = postgres_properties)

                # Lookup `product_id` from `products` table based on `product_nk`
                df = df.join(products_df, df.product_nk == products_df.product_nk, "left") \
                       .select(df['*'], products_df.product_id)

                # drop column created_at
                df = df.drop('created_at')

                df.write \
                    .format("csv") \
                    .option("header", "true") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/inventory")

            spark.stop()
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from inventory: {e}")
    
    @staticmethod
    def _orders(incremental, date):
        try:
            spark = SparkSession.builder \
                .appName("ETL Pipeline - Orders") \
                .getOrCreate()

            query = "(SELECT * FROM staging.orders) as data"    
            if incremental:
                query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

            df = spark.read.jdbc(
                url = postgres_staging, 
                table = query, 
                properties = postgres_properties
            )
            
            if not df.isEmpty():
                spark.stop()
                print("orders doesn't have new data. Skipped...")
                return
            
            else:
                # Rename columns
                df = df.withColumnRenamed('orderid', 'order_nk') \
                       .withColumnRenamed('customerid', 'customer_nk') \
                       .withColumnRenamed('orderdate', 'order_date') \
                       .withColumnRenamed('netamount', 'net_amount') \
                       .withColumnRenamed('totalamount', 'total_amount')

                # Extract data from the `customer` table
                customers_df = spark.read.jdbc(
                    url = postgres_warehouse, 
                    table = "(SELECT * FROM customers) as data", 
                    properties = postgres_properties
                )

                # Lookup `customer_id` from `customer` table based on `customer_nk`
                df = df.join(customers_df, df.customer_nk == customers_df.customer_nk, "left") \
                       .select(df['*'], customers_df.customer_id)

                # drop columns
                df = df.drop('created_at', 'customer_nk')

                df.write \
                    .format("csv") \
                    .option("header", "true") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/orders")

            spark.stop()

        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from orders: {e}")
    
    @staticmethod
    def _orderlines(incremental, date):
        try:
            spark = SparkSession.builder \
                .appName("ETL Pipeline - Orderlines") \
                .getOrCreate()

            query = "(SELECT * FROM staging.orderlines) as data" 
            if incremental:
                query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

            df = spark.read.jdbc(url = postgres_staging, 
                                 table = query, 
                                 properties = postgres_properties)
            
            if not df.isEmpty():
                spark.stop()
                print("orderlines doesn't have new data. Skipped...")
                return
            
            else:
                # Rename columns
                df = df.withColumnRenamed('orderlineid', 'orderline_nk') \
                       .withColumnRenamed('orderid', 'order_nk') \
                       .withColumnRenamed('prod_id', 'product_nk') \
                       .withColumnRenamed('orderdate', 'order_date')

                # Extract data from the `orders` table
                orders_df = spark.read.jdbc(
                    url = postgres_warehouse, 
                    table = "(SELECT * FROM orders) as data", 
                    properties = postgres_properties
                )

                # Lookup `order_id` from `orders` table based on `orderid`
                df = df.join(orders_df, df.order_nk == orders_df.order_nk, "left") \
                       .select(df['*'], orders_df.order_id)

                # Extract data from the `product` table
                products_df = spark.read.jdbc(
                    url = postgres_warehouse, 
                    table = "(SELECT * FROM products) as data", 
                    properties = postgres_properties
                )

                # Lookup `product_id` from `product` table based on `prod_id`
                df = df.join(products_df, df.product_nk == products_df.product_nk, "left") \
                       .select(df['*'], products_df.product_id)

                # drop unnecessary columns
                df = df.drop('created_at', 'order_nk', 'product_nk')

                df.write \
                    .format("csv") \
                    .option("header", "true") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/orderlines")

            spark.stop()
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from orderlines: {e}")
            
    @staticmethod
    def _cust_hist(incremental, date):
        try:
            spark = SparkSession.builder \
                .appName("ETL Pipeline - Customer History") \
                .getOrCreate()

            query = "(SELECT * FROM staging.cust_hist) as data"     
            if incremental:
                query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

            df = spark.read.jdbc(url = postgres_staging, 
                                 table = query, 
                                 properties = postgres_properties)
            
            if not df.isEmpty():
                spark.stop()
                print("cust_hist doesn't have new data. Skipped...")
                return
            
            else:
                # Rename columns
                df = df.withColumnRenamed('customerid', 'customer_nk') \
                       .withColumnRenamed('prod_id', 'product_nk') \
                       .withColumnRenamed('orderid', 'order_nk')

                # Extract data from the `customers` table
                customers_df = spark.read.jdbc(
                    url = postgres_warehouse, 
                    table = "(SELECT * FROM customers) as data", 
                    properties = postgres_properties
                )

                # Lookup `customer_id` from `customers` table based on `customerid`
                df = df.join(customers_df, df.customer_nk == customers_df.customer_nk, "left") \
                       .select(df['*'], customers_df.customer_id)

                # Extract data from the `orders` table
                orders_df = spark.read.jdbc(
                    url = postgres_warehouse, 
                    table = "(SELECT * FROM orders) as data", 
                    properties = postgres_properties
                )

                # Lookup `order_id` from `orders` table based on `orderid`
                df = df.join(orders_df, df.order_nk == orders_df.order_nk, "inner")

                # Extract data from the `product` table
                products_df = spark.read.jdbc(
                    url = postgres_warehouse, 
                    table = "(SELECT * FROM products) as data", 
                    properties = postgres_properties
                )

                # Lookup `product_id` from `product` table based on `prod_id`
                df = df.join(products_df, df.product_nk == products_df.product_nk, "left") \
                       .select(df['*'], products_df.product_id)

                # get necessary columns
                df = df.select('customer_id', 'order_id', 'product_id')

                df.write \
                    .format("csv") \
                    .option("header", "true") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/cust_hist")

            spark.stop()

        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from cust_hist: {e}")
    
    @staticmethod
    def _order_status_analytic(incremental, date):
        try:
            spark = SparkSession.builder \
                .appName("ETL Pipeline - Order Status Analytic") \
                .getOrCreate()

            query = "(SELECT * FROM staging.order_status_analytic) as data" 
            if incremental:
                query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

            df = spark.read.jdbc(url = postgres_staging, 
                                 table = query, 
                                 properties = postgres_properties)
            
            if not df.isEmpty():
                spark.stop()
                print("order_status_analytic doesn't have new data. Skipped...")
                return
            
            else:
                # Rename columns
                df = df.withColumnRenamed('orderid', 'order_nk')

                # Extract data from the `orders` table
                orders_df = spark.read.jdbc(
                    url = postgres_warehouse, 
                    table = "(SELECT * FROM orders) as data", 
                    properties = postgres_properties
                )

                # Lookup `order_id` from `orders` table based on `orderid`
                df = df.join(orders_df, df.order_nk == orders_df.order_nk, "inner")

                # get necessary columns
                df = df.select('order_id', 'order_nk', 'sum_stock', 'status')

                df.write \
                    .format("csv") \
                    .option("header", "true") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/order_status_analytic")

            spark.stop()
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from order_status_analytic: {e}")
    
    #============================================================================================================================================================================
    @staticmethod
    def _customers_history(incremental, date):
        try:
            spark = SparkSession.builder \
                .appName("ETL Pipeline - Customers History") \
                .getOrCreate()

            query = "(SELECT * FROM staging.customer_orders_history) as data" 
            if incremental:
                query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

            df = spark.read.jdbc(url = postgres_staging, 
                                 table = query, 
                                 properties = postgres_properties)
            
            if not df.isEmpty():
                spark.stop()
                print("customers_history doesn't have new data. Skipped...")
                return
            else:
                # Rename columns
                df = df.withColumnRenamed('customer_id', 'customer_nk') \
                       .withColumnRenamed('customer_firstname', 'first_name') \
                       .withColumnRenamed('customer_lastname', 'last_name') \
                       .withColumnRenamed('customer_address1', 'address1') \
                       .withColumnRenamed('customer_address2', 'address2') \
                       .withColumnRenamed('customer_city', 'city') \
                       .withColumnRenamed('customer_state', 'state') \
                       .withColumnRenamed('customer_zip', 'zip') \
                       .withColumnRenamed('customer_country', 'country') \
                       .withColumnRenamed('customer_region', 'region') \
                       .withColumnRenamed('customer_email', 'email') \
                       .withColumnRenamed('customer_phone', 'phone') \
                       .withColumnRenamed('customer_creditcardtype', 'credit_card_type') \
                       .withColumnRenamed('customer_creditcard', 'credit_card') \
                       .withColumnRenamed('customer_creditcardexpiration', 'credit_card_expiration') \
                       .withColumnRenamed('customer_username', 'username') \
                       .withColumnRenamed('customer_password', 'password') \
                       .withColumnRenamed('customer_age', 'age') \
                       .withColumnRenamed('customer_income', 'income') \
                       .withColumnRenamed('customer_gender', 'gender')

                columns_to_keep = [
                    'customer_nk', 'customer_id', 'first_name', 'last_name', 
                    'address1', 'address2', 'city', 'state', 'zip', 
                    'country', 'region', 'email', 'phone', 
                    'credit_card_type', 'credit_card', 'credit_card_expiration', 
                    'username', 'password', 'age', 'income', 'gender'
                ]

                # Drop unnecessary columns
                df = df.select(*columns_to_keep)

                # Deduplication based on customer_nk
                df = df.dropDuplicates(['customer_nk'])

                df.write \
                    .format("csv") \
                    .option("header", "true") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/customers_history")

            spark.stop()

        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from _customers_history: {e}")

    @staticmethod
    def _products_history(incremental, date):
        try:
            spark = SparkSession.builder \
                .appName("ETL Pipeline - Products History") \
                .getOrCreate()

            query = "(SELECT * FROM staging.customer_orders_history) as data" 
            if incremental:
                query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

            df = spark.read.jdbc(url = postgres_staging, 
                                 table = query, 
                                 properties = postgres_properties)
            
            if not df.isEmpty():
                spark.stop()
                print("products_history doesn't have new data. Skipped...") 
                return
            
            else:                
                # Rename columns
                df = df.withColumnRenamed('product_id', 'product_nk') \
                       .withColumnRenamed('product_category', 'category_nk') \
                       .withColumnRenamed('product_title', 'title') \
                       .withColumnRenamed('product_actor', 'actor') \
                       .withColumnRenamed('product_price', 'price') \
                       .withColumnRenamed('product_special', 'special') \
                       .withColumnRenamed('product_common_prod_id', 'common_prod_id')

                # Deduplication based on product_nk
                df = df.dropDuplicates(['product_nk'])

                # Extract data from the `categories` table
                categories_df = spark.read.jdbc(
                    url = postgres_warehouse, 
                    table = "(SELECT * FROM categories) as data", 
                    properties = postgres_properties
                )

                # Lookup `category_id` from `categories` table based on `category`
                df = df.join(categories_df, df.category_nk == categories_df.category_nk, "left") \
                       .select(df['*'], categories_df.category_id)
                
                # Get relevant columns
                df = df.select('product_nk', 'category_id', 'title', 'actor', 'price', 'special', 'common_prod_id')

                df.write \
                    .format("csv") \
                    .option("header", "true") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/products_history")

            spark.stop()

        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from _products_history: {e}")

    @staticmethod
    def _orders_history(incremental, date):
        try:
            spark = SparkSession.builder \
                .appName("ETL Pipeline - Orders History") \
                .getOrCreate()

            query = "(SELECT * FROM staging.customer_orders_history) as data" 
            if incremental:
                query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

            df = spark.read.jdbc(url = postgres_staging, 
                                 table = query, 
                                 properties = postgres_properties)
            
            if not df.isEmpty():
                spark.stop()
                print("orders_history doesn't have new data. Skipped...")
                return
            
            else:
                # Rename columns
                df = df.withColumnRenamed('order_id', 'order_nk') \
                       .withColumnRenamed('order_customerid', 'customer_nk') \
                       .withColumnRenamed('order_date', 'order_date') \
                       .withColumnRenamed('order_netamount', 'net_amount') \
                       .withColumnRenamed('order_tax', 'tax') \
                       .withColumnRenamed('order_totalamount', 'total_amount')

                # Deduplication based on order_nk
                df = df.dropDuplicates(['order_nk'])

                # Extract data from the `customers` table
                customer_column_list, customer_data = _extract(connection_id='warehouse_db', table_name='customers', incremental=False)
                customer_df = spark.createDataFrame(customer_data, schema=customer_column_list)

                # Lookup `customer_id` from `customers` table based on `customer_nk`
                df = df.join(customer_df, df.customer_nk == customer_df.customer_nk, "left") \
                       .select(df['*'], customer_df.customer_id)
                
                # Get relevant columns
                df = df.select('order_nk', 'customer_id', 'order_date', 'net_amount', 'tax', 'total_amount')

                df.write \
                    .format("csv") \
                    .option("header", "true") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/orders_history")

            spark.stop()

        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from _orders_history: {e}")
    
    @staticmethod
    def _orderlines_history(incremental, date):
        try:
            spark = SparkSession.builder \
                .appName("ETL Pipeline - Orderlines History") \
                .getOrCreate()

            query = "(SELECT * FROM staging.customer_orders_history) as data" 
            if incremental:
                query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

            df = spark.read.jdbc(url = postgres_staging, 
                                 table = query, 
                                 properties = postgres_properties)
            
            if not df.isEmpty():
                spark.stop()
                print("orderlines_history doesn't have new data. Skipped...")
                return
            
            else:
                # Rename columns
                df = df.withColumnRenamed('orderline_id', 'orderline_nk') \
                       .withColumnRenamed('order_id', 'order_nk') \
                       .withColumnRenamed('product_id', 'product_nk') \
                       .withColumnRenamed('orderline_quantity', 'quantity') \
                       .withColumnRenamed('orderline_orderdate', 'order_date')

                # Deduplication based on orderline_nk, order_nk, product_nk, quantity
                df = df.dropDuplicates(['orderline_nk', 'order_nk', 'product_nk', 'quantity'])

                # Extract data from the `orders` table
                orders_df = spark.read.jdbc(
                    url = postgres_warehouse, 
                    table = "(SELECT * FROM orders) as data", 
                    properties = postgres_properties
                )

                # Lookup `order_id` from `orders` table based on `order_nk`
                df = df.join(orders_df, df.order_nk == orders_df.order_nk, "left") \
                       .select(df['*'], orders_df.order_id)

                # Extract data from the `products` table
                products_df = spark.read.jdbc(
                    url = postgres_warehouse, 
                    table = "(SELECT * FROM products) as data", 
                    properties = postgres_properties
                )

                # Lookup `product_id` from `products` table based on `product_nk`
                df = df.join(products_df, df.product_nk == products_df.product_nk, "left") \
                       .select(df['*'], products_df.product_id)

                # Get relevant columns
                df = df.select('orderline_nk', 'order_id', 'product_id', 'quantity', 'order_date')

                df.write \
                    .format("csv") \
                    .option("header", "true") \
                    .mode("overwrite") \
                    .save(f"{transformed_data_path}/orderlines_history")

            spark.stop()
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from _orderlines_history: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: extract_transform.py <function_name> <incremental> <date>")
        sys.exit(-1)

    function_name = sys.argv[1]
    incremental = sys.argv[2].lower() == 'true'
    date = sys.argv[3]

    # Dictionary mapping function names to their corresponding methods
    function_map = {
        "categories": ExtractTransform._categories,
        "customers": ExtractTransform._customers,
        "products": ExtractTransform._products,
        "inventory": ExtractTransform._inventory,
        "orders": ExtractTransform._orders,
        "orderlines": ExtractTransform._orderlines,
        "cust_hist": ExtractTransform._cust_hist,
        "order_status_analytic": ExtractTransform._order_status_analytic,
        "customers_history": ExtractTransform._customers_history,
        "products_history": ExtractTransform._products_history,
        "orders_history": ExtractTransform._orders_history,
        "orderlines_history": ExtractTransform._orderlines_history
    }

    if function_name in function_map:
        function_map[function_name](incremental, date)
    else:
        print(f"Unknown function name: {function_name}")
        sys.exit(-1)