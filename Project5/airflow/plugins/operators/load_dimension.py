from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
        TRUNCATE TABLE {};
        INSERT INTO {}
        {};
        COMMIT;
    """
    
    delete_sql = """
        DELETE FROM {};
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here              
                 conn_id = "",
                 table = "",
                 sql_query = "",
                 with_delete = false, 
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.conn_id = conn_id
        self.table = table
        self.sql_query = sql_query
        self.with_delete = with_delete
        

    def execute(self, context):
       # self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        if self.with_delete:
            self.log.info(f"Deleting table {self.table} from Redshift")
            deleteting_sql = LoadDimensionOperator.delete_sql.format(
                self.table
            ) 
            redshift.run(deleteting_sql)
          
        self.log.info(f"Insert data from fact table into dimension table {self.table}")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.table,
            self.sql_query
        )
        redshift.run(formatted_sql)
        
        
       
       
