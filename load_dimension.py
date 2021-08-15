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
        self.table = table
        self.query = "INSERT INTO " + self.table + " " + query
        self.delete_mode = delete_mode
        

    def execute(self, context):
        """
        Executing sql statement in redshift for dimention table to insert values
        """
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.delete_mode:
            redshift_hook.run("DELETE FROM {table}".format(table = self.table))
            
        redshift_hook.run(self.query)
