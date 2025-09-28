from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, redshift_conn_id="", tables=[], *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            self.log.info(f"Running data quality check on table: {table}")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")

            if len(records) < 1 or len(records[0]) < 1:
                error_msg = f"Data quality check failed: {table} returned no results."
                self.log.error(error_msg)
                raise AirflowException(error_msg)

            num_records = records[0][0]
            if num_records == 0:
                error_msg = f"Data quality check failed: {table} contains 0 rows."
                self.log.error(error_msg)
                raise AirflowException(error_msg)

            self.log.info(
                f"Data quality check passed for table {table} with {num_records} records."
            )
