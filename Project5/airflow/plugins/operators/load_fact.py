from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here                
                 conn_id = "",
                 table = "",
                 sql_query = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.conn_id = conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        #self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        self.log.info("Deleting Redshift fact table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info(f"Insert data into fact table {self.table}")
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql_query
        )
        redshift.run(formatted_sql)
