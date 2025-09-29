from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, redshift_conn_id="", tests=[], *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test in self.tests:
            sql = test.get("sql")
            expected = test.get("expected")

            if not sql or expected is None:
                raise AirflowException("Each test must include 'sql' and 'expected' keys.")

            self.log.info(f"Running data quality check: {sql}")
            records = redshift.get_records(sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise AirflowException(f"Data quality check failed: '{sql}' returned no results.")

            actual = records[0][0]
            if actual != expected:
                raise AirflowException(
                    f"Data quality check failed: '{sql}' returned {actual}, expected {expected}."
                )

            self.log.info(f"Data quality check passed: '{sql}' returned {actual} as expected.")