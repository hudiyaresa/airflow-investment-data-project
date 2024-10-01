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
            tasks = [
                ('categories', ExtractTransform._categories),
                ('customers', ExtractTransform._customers),
                ('customers_history', ExtractTransform._customers_history)
            ]

            for task_id, python_callable in tasks:
                PythonOperator(
                    task_id=task_id,
                    python_callable=python_callable,
                    trigger_rule = 'none_failed',
                    op_kwargs={
                        'incremental': incremental,
                        'date': '{{ ds }}'
                    }
                )

        @task_group
        def validation():
            validation_tasks = [
                ('categories', False, {}),
                ('customers', True, {
                    "email": ValidationType.validate_email_format, 
                    "phone": ValidationType.validate_phone_format, 
                    "credit_card_expiration": ValidationType.validate_credit_card_expiration_format
                }),
                ('customers_history', True, {
                    "email": ValidationType.validate_email_format, 
                    "phone": ValidationType.validate_phone_format, 
                    "credit_card_expiration": ValidationType.validate_credit_card_expiration_format
                })
            ]

            for task_id, need_validation, validation_functions in validation_tasks:
                PythonOperator(
                    task_id=task_id,
                    python_callable=Validation._data_validations,
                    trigger_rule = 'none_failed',
                    op_kwargs={
                        'table_name': task_id,
                        'need_validation': need_validation,
                        'valid_bucket': 'valid-data',
                        'incremental': incremental,
                        'date': '{{ ds }}' if incremental else None,
                        'invalid_bucket': 'invalid-data' if need_validation else None,
                        'validation_functions': validation_functions if need_validation else None
                    }
                )

        @task_group
        def load():
            load_tasks = [
                ('categories', 'category_nk'),
                ('customers', 'customer_nk'),
                ('customers_history', 'customer_nk')
            ]

            for task_id, table_pkey in load_tasks:
                PythonOperator(
                    task_id=task_id,
                    python_callable=Load._warehouse,
                    trigger_rule='none_failed',
                    op_kwargs={
                        'table_name': task_id,
                        'incremental': incremental,
                        'table_pkey': table_pkey,
                        'date': '{{ ds }}'
                    }
                )

        extract_transform() >> validation() >> load()


    @task_group
    def step_2():
        @task_group
        def extract_transform():
            tasks = [
                ('products_history', ExtractTransform._products_history),
                ('orders_history', ExtractTransform._orders_history),
                ('products', ExtractTransform._products),
                ('orders', ExtractTransform._orders),
            ]

            for task_id, python_callable in tasks:
                PythonOperator(
                    task_id=task_id,
                    python_callable=python_callable,
                    trigger_rule = 'none_failed',
                    op_kwargs={
                        'incremental': incremental,
                        'date': '{{ ds }}'
                    }
                )
            

        @task_group
        def validation():
            validation_tasks = [
                ('products_history', True, {
                    "price": ValidationType.validate_price_range
                }),
                ('orders_history', True, {
                    "net_amount": ValidationType.validate_positive_value,
                    "tax": ValidationType.validate_positive_value,
                    "total_amount": ValidationType.validate_positive_value
                }),
                ('products', True, {
                    "price": ValidationType.validate_price_range
                }),
                ('orders', True, {
                    "net_amount": ValidationType.validate_positive_value,
                    "tax": ValidationType.validate_positive_value,
                    "total_amount": ValidationType.validate_positive_value
                }),
            ]

            for task_id, need_validation, validation_functions in validation_tasks:
                PythonOperator(
                    task_id=task_id,
                    python_callable=Validation._data_validations,
                    trigger_rule = 'none_failed',
                    op_kwargs={
                        'table_name': task_id,
                        'need_validation': need_validation,
                        'valid_bucket': 'valid-data',
                        'incremental': incremental,
                        'date': '{{ ds }}' if incremental else None,
                        'invalid_bucket': 'invalid-data' if need_validation else None,
                        'validation_functions': validation_functions if need_validation else None
                    }
                )

        @task_group
        def load():
            load_tasks = [
                ('products_history', 'product_nk'),
                ('orders_history', 'order_nk'),
                ('products', 'product_nk'),
                ('orders', 'order_nk')
            ]

            for task_id, table_pkey in load_tasks:
                PythonOperator(
                    task_id=task_id,
                    python_callable=Load._warehouse,
                    trigger_rule='none_failed',
                    op_kwargs={
                        'table_name': task_id,
                        'incremental': incremental,
                        'table_pkey': table_pkey,
                        'date': '{{ ds }}'
                    }
                )
            

        extract_transform() >> validation() >> load()

    @task_group
    def step_3():
        @task_group
        def extract_transform():
            tasks = [
                ('inventory', ExtractTransform._inventory),
                ('orderlines', ExtractTransform._orderlines),
                ('cust_hist', ExtractTransform._cust_hist),
                ('order_status_analytic', ExtractTransform._order_status_analytic)
            ]

            for task_id, python_callable in tasks:
                PythonOperator(
                    task_id=task_id,
                    python_callable=python_callable,
                    trigger_rule = 'none_failed',
                    op_kwargs={
                        'incremental': incremental,
                        'date': '{{ ds }}'
                    }
                )
            

        @task_group
        def validation():
            validation_tasks = [
                ('inventory', False, {}),
                ('orderlines', True, {
                    "quantity": ValidationType.validate_positive_value
                }),
                ('cust_hist', False, {}),
                ('order_status_analytic', True, {
                    "status": ValidationType.validate_order_status
                })
            ]

            for task_id, need_validation, validation_functions in validation_tasks:
                PythonOperator(
                    task_id=task_id,
                    python_callable=Validation._data_validations,
                    trigger_rule = 'none_failed',
                    op_kwargs={
                        'table_name': task_id,
                        'need_validation': need_validation,
                        'valid_bucket': 'valid-data',
                        'incremental': incremental,
                        'date': '{{ ds }}' if incremental else None,
                        'invalid_bucket': 'invalid-data' if need_validation else None,
                        'validation_functions': validation_functions if need_validation else None
                    }
                )

        @task_group
        def load():
            load_tasks = [
                ('inventory', 'product_nk'),
                ('orderlines', ["orderline_nk", "order_id", "product_id", "quantity"]),
                ('cust_hist', ["customer_id", "order_id", "product_id"]),
                ('order_status_analytic', 'order_id')
            ]

            for task_id, table_pkey in load_tasks:
                PythonOperator(
                    task_id=task_id,
                    python_callable=Load._warehouse,
                    trigger_rule='none_failed',
                    op_kwargs={
                        'table_name': task_id,
                        'incremental': incremental,
                        'table_pkey': table_pkey,
                        'date': '{{ ds }}'
                    }
                )
            

        extract_transform() >> validation() >> load()

    step_1() >> step_2() >> step_3()