from airflow.decorators import task_group
from airflow.operators.python import PythonOperator

from etl_pipeline.tasks.warehouse.components.extract_transform import ExtractTransform 
from etl_pipeline.tasks.warehouse.components.validations import Validation, ValidationType
from etl_pipeline.tasks.warehouse.components.load import Load


@task_group
def warehouse(incremental):
    @task_group
    def step_1():
        @task_group
        def extract_transform():
            categories = PythonOperator(
                task_id = 'categories',
                python_callable = ExtractTransform._categories,
                op_kwargs = {
                    'incremental': incremental,
                    'date': '{{ ds }}'
                }
            )

            customers = PythonOperator(
                task_id = 'customers',
                python_callable = ExtractTransform._customers,
                op_kwargs = {
                    'incremental': incremental,
                    'date': '{{ ds }}'
                }
            )

            products = PythonOperator(
                task_id = 'products',
                python_callable = ExtractTransform._products,
                op_kwargs = {
                    'incremental': incremental,
                    'date': '{{ ds }}'
                }
            )

            orders = PythonOperator(
                task_id = 'orders',
                python_callable = ExtractTransform._orders,
                op_kwargs = {
                    'incremental': incremental,
                    'date': '{{ ds }}'
                }
            )

            customers_history = PythonOperator(
                task_id = 'customers_history',
                python_callable = ExtractTransform._customers_history,
                op_kwargs = {
                    'incremental': incremental,
                    'date': '{{ ds }}'
                }
            )

            products_history = PythonOperator(
                task_id = 'products_history',
                python_callable = ExtractTransform._products_history,
                op_kwargs = {
                    'incremental': incremental,
                    'date': '{{ ds }}'
                }
            )

            orders_history = PythonOperator(
                task_id = 'orders_history',
                python_callable = ExtractTransform._orders_history,
                op_kwargs = {
                    'incremental': incremental,
                    'date': '{{ ds }}'
                }
            )

            categories
            customers
            products
            orders
            customers_history
            products_history
            orders_history

        @task_group
        def validation():
            categories = PythonOperator(
                task_id = 'categories',
                python_callable = Validation._validation_data,
                op_kwargs = {
                    'need_validation': False,
                    'data': 'categories.csv',
                    'valid_bucket': 'valid-data',
                    'dest_object': 'categories.csv',
                }
            )

            customers = PythonOperator(
                task_id = 'customers',
                python_callable = Validation._validation_data,
                op_kwargs = {
                    'need_validation': True,
                    'data': 'customers.csv',
                    'valid_bucket': 'valid-data',
                    'dest_object': 'customers.csv',
                    'invalid_bucket': 'invalid-data',
                    'validation_functions': {
                        "email": ValidationType.validate_email_format, 
                        "phone": ValidationType.validate_phone_format, 
                        "credit_card_expiration": ValidationType.validate_credit_card_expiration_format
                    }
                }
            )

            products = PythonOperator(
                task_id = 'products',
                python_callable = Validation._validation_data,
                op_kwargs = {
                    'need_validation': True,
                    'data': 'products.csv',
                    'valid_bucket': 'valid-data',
                    'dest_object': 'products.csv',
                    'invalid_bucket': 'invalid-data',
                    'validation_functions': {
                        "price": ValidationType.validate_price_range
                    }
                }
            )

            orders = PythonOperator(
                task_id = 'orders',
                python_callable = Validation._validation_data,
                op_kwargs = {
                    'need_validation': True,
                    'data': 'orders.csv',
                    'valid_bucket': 'valid-data',
                    'dest_object': 'orders.csv',
                    'invalid_bucket': 'invalid-data',
                    'validation_functions': {
                        "net_amount": ValidationType.validate_positive_value,
                        "tax": ValidationType.validate_positive_value,
                        "total_amount": ValidationType.validate_positive_value
                    }
                }
            )

            customers_history = PythonOperator(
                task_id = 'customers_history',
                python_callable = Validation._validation_data,
                op_kwargs = {
                    'need_validation': True,
                    'data': 'customers_history.csv',
                    'valid_bucket': 'valid-data',
                    'dest_object': 'customers_history.csv',
                    'invalid_bucket': 'invalid-data',
                    'validation_functions': {
                        "email": ValidationType.validate_email_format, 
                        "phone": ValidationType.validate_phone_format, 
                        "credit_card_expiration": ValidationType.validate_credit_card_expiration_format
                    }
                }
            )

            products_history = PythonOperator(
                task_id = 'products_history',
                python_callable = Validation._validation_data,
                op_kwargs = {
                    'need_validation': True,
                    'data': 'products_history.csv',
                    'valid_bucket': 'valid-data',
                    'dest_object': 'products_history.csv',
                    'invalid_bucket': 'invalid-data',
                    'validation_functions': {
                        "price": ValidationType.validate_price_range
                    }
                }
            )

            orders_history = PythonOperator(
                task_id = 'orders_history',
                python_callable = Validation._validation_data,
                op_kwargs = {
                    'need_validation': True,
                    'data': 'orders_history.csv',
                    'valid_bucket': 'valid-data',
                    'dest_object': 'orders_history.csv',
                    'invalid_bucket': 'invalid-data',
                    'validation_functions': {
                        "net_amount": ValidationType.validate_positive_value,
                        "tax": ValidationType.validate_positive_value,
                        "total_amount": ValidationType.validate_positive_value
                    }
                }
            )

            customers
            categories
            customers
            products
            orders
            customers_history
            products_history
            orders_history

        @task_group
        def load():
            categories = PythonOperator(
                task_id = 'categories',
                python_callable = Load._warehouse,
                trigger_rule = 'none_failed',
                op_kwargs = {
                    'table_name': 'categories',
                    'incremental': incremental,
                    'table_pkey': 'category_nk',
                    'date': '{{ ds }}'
                }
            )

            customers = PythonOperator(
                task_id = 'customers',
                python_callable = Load._warehouse,
                trigger_rule = 'none_failed',
                op_kwargs = {
                    'table_name': 'customers',
                    'incremental': incremental,
                    'table_pkey': 'customer_nk',
                    'date': '{{ ds }}'
                }   
            )

            products = PythonOperator(
                task_id = 'products',
                python_callable = Load._warehouse,
                trigger_rule = 'none_failed',
                op_kwargs = {
                    'table_name': 'products',
                    'incremental': incremental,
                    'table_pkey': 'product_nk',
                    'date': '{{ ds }}'
                }
            )

            orders = PythonOperator(
                task_id = 'orders',
                python_callable = Load._warehouse,
                trigger_rule = 'none_failed',
                op_kwargs = {
                    'table_name': 'orders',
                    'incremental': incremental,
                    'table_pkey': 'order_nk',
                    'date': '{{ ds }}'
                }
            )

            customers_history = PythonOperator(
                task_id = 'customers_history',
                python_callable = Load._warehouse,
                trigger_rule = 'none_failed',
                op_kwargs = {
                    'table_name': 'customers',
                    'incremental': incremental,
                    'table_pkey': 'customer_nk',
                    'date': '{{ ds }}'
                }
            )

            products_history = PythonOperator(
                task_id = 'products_history',
                python_callable = Load._warehouse,
                trigger_rule = 'none_failed',
                op_kwargs = {
                    'table_name': 'products',
                    'incremental': incremental,
                    'table_pkey': 'product_nk',
                    'date': '{{ ds }}'
                }
            )

            orders_history = PythonOperator(
                task_id = 'orders_history',
                python_callable = Load._warehouse,
                trigger_rule = 'none_failed',
                op_kwargs = {
                    'table_name': 'orders',
                    'incremental': incremental,
                    'table_pkey': 'order_nk',
                    'date': '{{ ds }}'
                }
            )

            categories >> products_history >> products
            customers_history >> customers >> orders_history >>orders
            

        extract_transform() >> validation() >> load()


    @task_group
    def step_2():
        @task_group
        def extract_transform():
            inventory = PythonOperator(
                task_id = 'inventory',
                python_callable = ExtractTransform._inventory,
                op_kwargs = {
                    'incremental': incremental,
                    'date': '{{ ds }}'
                }
            )

            orderlines = PythonOperator(
                task_id = 'orderlines',
                python_callable = ExtractTransform._orderlines,
                op_kwargs = {
                    'incremental': incremental,
                    'date': '{{ ds }}'
                }
            )
            
            cust_hist = PythonOperator(
                task_id = 'cust_hist',
                python_callable = ExtractTransform._cust_hist,
                op_kwargs = {
                    'incremental': incremental,
                    'date': '{{ ds }}'
                }
            )
            
            order_status_analytic = PythonOperator(
                task_id = 'order_status_analytic',
                python_callable = ExtractTransform._order_status_analytic,
                op_kwargs = {
                    'incremental': incremental,
                    'date': '{{ ds }}'
                }
            )

            inventory
            orderlines
            cust_hist
            order_status_analytic
            

        @task_group
        def validation():
            inventory = PythonOperator(
                task_id = 'inventory',
                python_callable = Validation._validation_data,
                op_kwargs = {
                    'need_validation': False,
                    'data': 'inventory.csv',
                    'valid_bucket': 'valid-data',
                    'dest_object': 'inventory.csv'
                }
            )

            orderlines = PythonOperator(
                task_id = 'orderlines',
                python_callable = Validation._validation_data,
                op_kwargs = {
                    'need_validation': True,
                    'data': 'orderlines.csv',
                    'valid_bucket': 'valid-data',
                    'dest_object': 'orderlines.csv',
                    'invalid_bucket': 'invalid-data',
                    'validation_functions': {
                        "quantity": ValidationType.validate_positive_value
                    }
                }
            )
            
            cust_hist = PythonOperator(
                task_id = 'cust_hist',
                python_callable = Validation._validation_data,
                op_kwargs = {
                    'need_validation': False,
                    'data': 'cust_hist.csv',
                    'valid_bucket': 'valid-data',
                    'dest_object': 'cust_hist.csv'
                }
            )
            
            order_status_analytic = PythonOperator(
                task_id = 'order_status_analytic',
                python_callable = Validation._validation_data,
                op_kwargs = {
                    'need_validation': True,
                    'data': 'order_status_analytic.csv',
                    'valid_bucket': 'valid-data',
                    'dest_object': 'order_status_analytic.csv',
                    'invalid_bucket': 'invalid-data',
                    'validation_functions': {
                        "status": ValidationType.validate_order_status
                    }
                }
            )

            inventory
            orderlines
            cust_hist
            order_status_analytic

        @task_group
        def load():
            inventory = PythonOperator(
                task_id = 'inventory',
                python_callable = Load._warehouse,
                trigger_rule = 'none_failed',
                op_kwargs = {
                    'table_name': 'inventory',
                    'incremental': incremental,
                    'table_pkey': 'product_nk',
                    'date': '{{ ds }}'  
                }
            )

            orderlines = PythonOperator(
                task_id = 'orderlines',
                python_callable = Load._warehouse,
                trigger_rule = 'none_failed',
                op_kwargs = {  
                    'table_name': 'orderlines',
                    'incremental': incremental,
                    'table_pkey': ["orderline_nk","order_id","product_id","quantity"],
                    'date': '{{ ds }}'
                }
            )
            
            cust_hist = PythonOperator(
                task_id = 'cust_hist',
                python_callable = Load._warehouse,
                trigger_rule = 'none_failed',
                op_kwargs = {
                    'table_name': 'cust_hist',
                    'incremental': incremental,
                    'table_pkey': ["customer_id","order_id","product_id"],
                    'date': '{{ ds }}'
                }
            )
            
            order_status_analytic = PythonOperator(
                task_id = 'order_status_analytic',
                python_callable = Load._warehouse,
                trigger_rule = 'none_failed',
                op_kwargs = {
                    'table_name': 'order_status_analytic',
                    'incremental': incremental,
                    'table_pkey': 'order_id',
                    'date': '{{ ds }}'
                }
            )

            inventory
            orderlines
            cust_hist
            order_status_analytic
            

        extract_transform() >> validation() >> load()

    step_1() >> step_2()