from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self, redshift_conn_id="", table="", sql="", append_only=False, *args, **kwargs
    ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_only:
            self.log.info(f"Truncating table {self.table} before insert.")
            redshift.run(f"TRUNCATE TABLE {self.table};")

        self.log.info(
            "Insert data from staging table into {} dimension table".format(self.table)
        )
        # formatted_sql = getattr(SqlQueries,self.sql).format(self.table)
        # redshift.run(formatted_sql)

        insert_statement = f"INSERT INTO {self.table} \n{self.sql}"
        self.log.info(f"Running sql: \n{insert_statement}")
        redshift.run(insert_statement)
        self.log.info(f"Successfully completed insert into {self.table}")
