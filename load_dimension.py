from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    run_query = """
    INSERT INTO public.{table} 
    {query}
    """

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
        self.query = query
        self.delete_mode = delete_mode
        

    def execute(self, context):
        """
        Executing sql statement in redshift for dimention table to insert values
        """
        self.log.info("Getting credentials for Redshift: {}".format(self.redshift_conn_id))
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.delete_mode:
            self.log.info("Deleting content of {}".format(self.table))
            redshift_hook.run("DELETE FROM {table}".format(table = self.table))
        
        self.log.info("Running INSERT INTO {} query".format(self.table))
        redshift_hook.run(self.run_query.format(table= self.table,query= self.query))
        self.log.info("INSERT INTO FINISHED")
