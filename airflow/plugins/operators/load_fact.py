from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#FAB06B'
    
    #Insert statement
    insert_into_stmt = """
        INSERT INTO {table} 
        {select_query}
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 select_query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_query = select_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        #Insert the data into the fact table(s)
        redshift.run(LoadFactOperator.insert_into_stmt.format(
            table=self.table,
            select_query=self.select_query
        ))
