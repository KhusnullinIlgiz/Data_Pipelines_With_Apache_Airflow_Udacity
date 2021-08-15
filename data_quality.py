from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        for table in self.tables:
            count_rows = redshift_hook.run("SELECT * FROM {}".format(table))
            if count_rows == None or count_rows == 0:
                raise ValueError("{} is empty".format(table))
            else:
                self.log.info("Data Quality Check is successful: {} has {} rows".format(table, count_rows))
                                     
                                     