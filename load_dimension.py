from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 query = "",
                 delete_mode = False,
                 table = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.delete_mode = delete_mode
        self.table = table

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.delete_mode:
            redshift_hook.run("DELETE FROM {table}".format(table = self.table))
            
        redshift_hook.run(self.query)
