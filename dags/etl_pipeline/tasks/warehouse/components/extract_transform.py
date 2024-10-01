from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException

from etl_pipeline.tasks.warehouse.components.extract import _extract
from etl_pipeline.tasks.warehouse.components.validations import Validation, ValidationType
from helper.minio import CustomMinio

import re
import pandas as pd

BASE_PATH = "/opt/airflow/dags"
        
class ExtractTransform:
    def _categories(incremental, date):
        try:
            if incremental:
                data = _extract(connection_id = 'staging_db', table_name='staging.categories', incremental = incremental, date = date)
                object_name = f'categories-{date}.csv'

            else:
                data = _extract(connection_id = 'staging_db', table_name='staging.categories', incremental = incremental)
                object_name = f'categories.csv'

            if data.empty:
                raise AirflowSkipException("categories doesn't have new data. Skipped...")
            
            else:
                # Rename columns
                data = data.rename(columns={'category':'category_nk', 'categoryname':'category_name'})

                # deduplication based on category_nk and category name
                data = data.drop_duplicates(subset='category_nk')

                # drop column created_at
                data = data.drop(columns=['created_at'])

                CustomMinio._put_csv(data, 'transformed-data', object_name)

        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from categories: {e}")

    def _customers(incremental, date):
        try:
            if incremental: 
                data = _extract(connection_id = 'staging_db', table_name = 'staging.customers', incremental = incremental, date = date)
                object_name = f'customers-{date}.csv'   

            else:
                data = _extract(connection_id = 'staging_db', table_name='staging.customers', incremental = incremental)
                object_name = f'customers.csv'

            if data.empty:
                raise AirflowSkipException("customers doesn't have new data. Skipped...")
            
            else:   
                # Rename columns
                data = data.rename(columns = {'customerid':'customer_nk', 'firstname':'first_name', 
                                            'lastname':'last_name', 'address':'address', 'city':'city', 'state':'state',
                                            'zip':'zip', 'email':'email', 'creditcardtype':'credit_card_type', 
                                            'creditcard':'credit_card', 'creditcardexpiration':'credit_card_expiration', 
                                            'username':'username', 'password':'password'})

                # deduplication based on customer_nk
                data = data.drop_duplicates(subset='customer_nk')

                # Masking credit card number
                data['credit_card'] = data['credit_card'].apply(lambda x: re.sub(r'\d', 'X', x[:-4]) + x[-4:])

                # drop column created_at
                data = data.drop(columns=['created_at'])

                CustomMinio._put_csv(data, 'transformed-data', object_name)

        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from customers: {e}")

    def _products(incremental, date):
        try:
            if incremental:
                data = _extract(connection_id = 'staging_db', table_name='staging.products', incremental = incremental, date = date)
                object_name = f'products-{date}.csv'
            else:
                data = _extract(connection_id = 'staging_db', table_name='staging.products', incremental = incremental)
                object_name = 'products.csv'


            if data.empty:
                raise AirflowSkipException("products doesn't have new data. Skipped...")
            
            else:
                # Rename columns
                data = data.rename(columns={'prod_id':'product_nk', 'category':'category_nk'})
                
                # deduplication based on product_nk
                data = data.drop_duplicates(subset='product_nk')

                # Extract data from the `categories` table
                categories = _extract(connection_id = 'warehouse_db', table_name='categories', incremental = False)

                #Lookup `category_id` from `categories` table based on `category`   
                data['category_id'] = data['category_nk'].apply(lambda x: categories.loc[categories['category_nk'] == x, 'category_id'].values[0])
                
                # drop column created_at
                data = data.drop(columns=['created_at','category_nk'])

                CustomMinio._put_csv(data, 'transformed-data', object_name)
        
        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from products: {e}")

    def _inventory(incremental, date):
        try:
            if incremental:
                data = _extract(connection_id = 'staging_db', table_name='staging.inventory', incremental = incremental, date = date)
                object_name = f'inventory-{date}.csv'
            else:
                data = _extract(connection_id = 'staging_db', table_name='staging.inventory', incremental = incremental)
                object_name = 'inventory.csv'
            
            if data.empty:
                raise AirflowSkipException("inventory doesn't have new data. Skipped...")
            
            else:
                # Rename columns
                data = data.rename(columns={'prod_id':'product_nk', 'quan_in_stock':'quantity_stock'})
                
                # deduplication based on product_nk
                data = data.drop_duplicates(subset='product_nk')

                # Extract data from the `products` table
                products = _extract(connection_id = 'warehouse_db', table_name='products', incremental = False)

                #Lookup `product_id` from `products` table based on `product_nk`   
                data['product_id'] = data['product_nk'].apply(lambda x: products.loc[products['product_nk'] == x, 'product_id'].values[0])
                
                # drop column created_at
                data = data.drop(columns=['created_at'])

                CustomMinio._put_csv(data, 'transformed-data', object_name)
        
        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from inventory: {e}")
    
    def _orders(incremental, date):
        try:
            if incremental:
                data = _extract(connection_id = 'staging_db', table_name='staging.orders', incremental = incremental, date = date)
                object_name = f'orders-{date}.csv'
            else:
                data = _extract(connection_id = 'staging_db', table_name='staging.orders', incremental = incremental)
                object_name = 'orders.csv'
            
            if data.empty:
                raise AirflowSkipException("orders doesn't have new data. Skipped...")
            
            else:
                # Rename columns
                data = data.rename(columns={'orderid':'order_nk', 'customerid':'customer_nk', 'orderdate':'order_date', 
                                            'netamount':'net_amount', 'tax':'tax', 'totalamount':'total_amount'})
                
                # Extract data from the `customer` table
                customers = _extract(connection_id = 'warehouse_db', table_name='customers', incremental = False)

                # Lookup `customer_id` from `customer` table based on `customer_nk`   
                data['customer_id'] = data['customer_nk'].apply(lambda x: customers.loc[customers['customer_nk'] == x, 'customer_id'].values[0])
                
                # drop column created_at
                data = data.drop(columns=['created_at','customer_nk'])

                CustomMinio._put_csv(data, 'transformed-data', object_name)
        
        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from orders: {e}")
    
    def _orderlines(incremental, date):
        try:
            if incremental:
                data = _extract(connection_id = 'staging_db', table_name='staging.orderlines', incremental = incremental, date = date)
                object_name = f'orderlines-{date}.csv'
            else:
                data = _extract(connection_id = 'staging_db', table_name='staging.orderlines', incremental = incremental)
                object_name = 'orderlines.csv'
            
            if data.empty:      
                raise AirflowSkipException("orderlines doesn't have new data. Skipped...")
            
            else:
                # Rename columns
                data = data.rename(columns={'orderlineid':'orderline_nk', 'orderid':'order_nk', 'prod_id':'product_nk', 
                                        'quantity':'quantity', 'orderdate':'order_date'})
            
                # Extract data from the `orders` table
                orders = _extract(connection_id = 'warehouse_db', table_name = 'orders', incremental = False)

                # Lookup `order_id` from `orders` table based on `orderid`   
                data['order_id'] = data['order_nk'].apply(lambda x: orders.loc[orders['order_nk'] == x, 'order_id'].values[0])
                
                # Extract data from the `product` table
                products = _extract(connection_id = 'warehouse_db', table_name='products', incremental = False)

                # Lookup `product_id` from `product` table based on `prod_id`   
                data['product_id'] = data['product_nk'].apply(lambda x: products.loc[products['product_nk'] == x, 'product_id'].values[0])
                
                # drop unnecessary columns
                data = data.drop(columns=['created_at','order_nk','product_nk'])

                CustomMinio._put_csv(data, 'transformed-data', object_name)
        
        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from orderlines: {e}")
            
    def _cust_hist(incremental, date):
        try:
            if incremental:
                data = _extract(connection_id = 'staging_db', table_name='staging.cust_hist', incremental = incremental, date = date)
                object_name = f'cust_hist-{date}.csv'   
            else:
                data = _extract(connection_id = 'staging_db', table_name='staging.cust_hist', incremental = incremental)
                object_name = 'cust_hist.csv'

            if data.empty:  
                raise AirflowSkipException("cust_hist doesn't have new data. Skipped...")
            
            else:
                # Rename columns
                data = data.rename(columns={'customerid':'customer_nk', 'prod_id':'product_nk', 'orderid':'order_nk'})
            
                # Extract data from the `customers` table
                customers = _extract(connection_id = 'warehouse_db', table_name='customers', incremental = False)

                # Lookup `customer_id` from `customers` table based on `customerid`   
                data['customer_id'] = data['customer_nk'].apply(lambda x: customers.loc[customers['customer_nk'] == x, 'customer_id'].values[0])
                
                # Extract data from the `orders` table
                orders = _extract(connection_id = 'warehouse_db', table_name='orders', incremental = False)
                orders = orders[['order_nk','order_id']]

                # Lookup `order_id` from `orders` table based on `orderid`   
                data = pd.merge(data, orders, on='order_nk', suffixes=('', '_orders'), how='inner')

                # Extract data from the `product` table
                products = _extract(connection_id = 'warehouse_db', table_name = 'products', incremental = False)

                # Lookup `product_id` from `product` table based on `prod_id`   
                data['product_id'] = data['product_nk'].apply(lambda x: products.loc[products['product_nk'] == x, 'product_id'].values[0])
                
                # get ecessary columns
                data = data[['customer_id','order_id','product_id']]

                CustomMinio._put_csv(data, 'transformed-data', object_name)

        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from cust_hist: {e}")
    
    def _order_status_analytic(incremental, date):
        try:
            if incremental:
                data = _extract(connection_id = 'staging_db', table_name='staging.order_status_analytic', incremental = incremental, date = date)
                object_name = f'order_status_analytic-{date}.csv'
            else:
                data = _extract(connection_id = 'staging_db', table_name='staging.order_status_analytic', incremental = incremental)
                object_name = 'order_status_analytic.csv'
            
            if data.empty:
                raise AirflowSkipException("order_status_analytic doesn't have new data. Skipped...")
            
            else:
                # Rename columns
                data = data.rename(columns={'orderid':'order_nk'})

                # Extract data from the `orders` table
                orders = _extract(connection_id = 'warehouse_db', table_name='orders', incremental = False)

                # Lookup `order_id` from `orders` table based on `orderid`   
                data = pd.merge(data, orders, on = 'order_nk', suffixes = ('', '_orders'), how = 'inner')

                # get necessary columns
                data = data[['order_id', 'order_nk', 'sum_stock', 'status']]

                CustomMinio._put_csv(data, 'transformed-data', object_name)
        
        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from order_status_analytic: {e}")
    
    #============================================================================================================================================================================
    def _customers_history(incremental, date):
        try:
            if incremental:
                data = _extract(connection_id = 'staging_db', table_name='staging.customer_orders_history', incremental = incremental, date = date)
                object_name = f'customers_history-{date}.csv'
            else:
                data = _extract(connection_id = 'staging_db', table_name='staging.customer_orders_history', incremental = incremental)
                object_name = 'customers_history.csv'

            if data.empty:
                raise AirflowSkipException("customers_history doesn't have new data. Skipped...")
            
            else:
                # Rename columns
                data = data.rename(columns={
                                        'customer_id': 'customer_nk',
                                        'customer_firstname': 'first_name',
                                        'customer_lastname': 'last_name',
                                        'customer_address1': 'address1',
                                        'customer_address2': 'address2',
                                        'customer_city': 'city',
                                        'customer_state': 'state',
                                        'customer_zip': 'zip',
                                        'customer_country': 'country',
                                        'customer_region': 'region',
                                        'customer_email': 'email',
                                        'customer_phone': 'phone',
                                        'customer_creditcardtype': 'credit_card_type',
                                        'customer_creditcard': 'credit_card',
                                        'customer_creditcardexpiration': 'credit_card_expiration',
                                        'customer_username': 'username',
                                        'customer_password': 'password',
                                        'customer_age': 'age',
                                        'customer_income': 'income',
                                        'customer_gender': 'gender'
                                    }) 
                
                columns_to_keep = [
                    'customer_nk', 'customer_id', 'first_name', 'last_name', 
                    'address1', 'address2', 'city', 'state', 'zip', 
                    'country', 'region', 'email', 'phone', 
                    'credit_card_type', 'credit_card', 'credit_card_expiration', 
                    'username', 'password', 'age', 'income', 'gender'
                ]

                # Drop unnecessary columns
                data = data.drop(columns=[col for col in data.columns if col not in columns_to_keep])

                # Deduplication based on customer_nk
                data = data.drop_duplicates(subset='customer_nk')

                CustomMinio._put_csv(data, 'transformed-data', object_name)
        
        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from _customers_history: {e}")

    def _products_history(incremental, date):
        try:
            if incremental:
                data = _extract(connection_id = 'staging_db', table_name='staging.customer_orders_history', incremental = incremental, date = date)
                object_name = f'products_history-{date}.csv'
            else:
                data = _extract(connection_id = 'staging_db', table_name='staging.customer_orders_history', incremental = incremental)
                object_name = 'products_history.csv'

            if data.empty:
                raise AirflowSkipException("products_history doesn't have new data. Skipped...")
            
            else:
                # Rename columns
                data = data.rename(columns={
                    'product_id': 'product_nk', 
                    'product_category': 'category_nk', 
                    'product_title': 'title', 
                    'product_actor': 'actor', 
                    'product_price': 'price', 
                    'product_special': 'special', 
                    'product_common_prod_id': 'common_prod_id'
                })

                # Deduplication based on product_nk
                data = data.drop_duplicates(subset='product_nk')

                # Extract data from the `categories` table
                categories = _extract(connection_id = 'warehouse_db', table_name = 'categories', incremental = incremental)

                #Lookup `category_id` from `categories` table based on `category`   
                data['category_id'] = data['category_nk'].apply(lambda x: categories.loc[categories['category_nk'] == x, 'category_id'].values[0])
                
                # Get relevant columns
                data = data[['product_nk', 'category_id', 'title', 'actor', 'price', 'special', 'common_prod_id']]

                CustomMinio._put_csv(data, 'transformed-data', object_name)

        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from _products_history: {e}")

    def _orders_history(incremental, date):
        try:
            if incremental:
                data = _extract(connection_id = 'staging_db', table_name='staging.customer_orders_history', incremental = incremental, date = date)
                object_name = f'orders_history-{date}.csv'
            else:
                data = _extract(connection_id = 'staging_db', table_name='staging.customer_orders_history', incremental = incremental)
                object_name = 'orders_history.csv'

            if data.empty:
                raise AirflowSkipException("orders_history doesn't have new data. Skipped...")
            
            else:
                # Rename columns
                data = data.rename(columns={
                            'order_id': 'order_nk', 
                            'order_customerid': 'customer_nk', 
                            'order_date': 'order_date', 
                            'order_netamount': 'net_amount', 
                            'order_tax': 'tax', 
                            'order_totalamount': 'total_amount'
                        })


                # Deduplication based on order_nk
                data = data.drop_duplicates(subset='order_nk')

                # Extract data from the `customers` table
                customer = _extract(connection_id = 'warehouse_db', table_name = 'customers', incremental = incremental)

                #Lookup `customer_id` from `customers` table based on `customer_nk`   
                data['customer_id'] = data['customer_nk'].apply(lambda x: customer.loc[customer['customer_nk'] == x, 'customer_id'].values[0])
                
                # Get relevant columns
                data = data[['order_nk', 'customer_id', 'order_date', 'net_amount', 'tax', 'total_amount']]

                CustomMinio._put_csv(data, 'transformed-data', object_name)

        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from _orders_history: {e}")
    
    def _orderlines_history(incremental, date):
        try:
            if incremental:
                data = _extract(connection_id = 'staging_db', table_name='staging.customer_orders_history', incremental = incremental, date = date)
                object_name = f'orderlines_history-{date}.csv'  
            else:
                data = _extract(connection_id = 'staging_db', table_name='staging.customer_orders_history', incremental = incremental)
                object_name = 'orderlines_history.csv'

            if data.empty:
                raise AirflowSkipException("orderlines_history doesn't have new data. Skipped...")
            
            else:
                # rename column for orders
                data = data.rename(columns={
                    'orderline_id': 'orderline_nk', 
                    'order_id': 'order_nk', 
                    'product_id': 'product_nk', 
                    'orderline_quantity': 'quantity', 
                    'orderline_orderdate': 'order_date'
                })

                # Deduplication based on order_nk
                data = data.drop_duplicates(subset=['orderline_nk','order_nk','product_nk','quantity'])

                # Extract data from the `orders` table
                orders = _extract(connection_id = 'warehouse_db', table_name = 'orders', incremental = False)

                # Lookup `order_id` from `orders` table based on `orderid`   
                data['order_id'] = data['order_nk'].apply(lambda x: orders.loc[orders['order_nk'] == x, 'order_id'].values[0])
                
                # Extract data from the `product` table
                products = _extract(connection_id = 'warehouse_db', table_name = 'products', incremental = False)

                # Lookup `product_id` from `product` table based on `prod_id`   
                data['product_id'] = data['product_nk'].apply(lambda x: products.loc[products['product_nk'] == x, 'product_id'].values[0])
                
                
                # Get relevant columns
                data = data[['orderline_nk', 'order_id', 'product_id', 'quantity', 'order_date']]

                CustomMinio._put_csv(data, 'transformed-data', object_name)

        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting and transforming data from _orderlines_history: {e}")