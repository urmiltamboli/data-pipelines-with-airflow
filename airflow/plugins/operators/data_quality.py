from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, redshift_conn_id="", checks=[], *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        failing_tests = []

        for check in self.checks:
            sql = check.get("sql")
            op = check.get("op")
            val = check.get("val")
            msg = check.get("msg", "Data quality check failed")

            if not sql or not op or val is None:
                raise AirflowException("Each check must include 'sql', 'op', and 'val'.")

            records = redshift.get_records(sql)
            if len(records) < 1 or len(records[0]) < 1:
                failing_tests.append(f"{msg}: Query returned no results.")
                continue

            result = records[0][0]
            if not eval(f"{result} {op} {val}"):
                failing_tests.append(f"{msg}: Got {result}, expected {op} {val}.")

        if failing_tests:
            raise AirflowException("Data quality checks failed:\n" + "\n".join(failing_tests))

        self.log.info("All data quality checks passed.")