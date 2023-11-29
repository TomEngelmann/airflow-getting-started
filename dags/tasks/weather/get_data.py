from airflow.hooks.postgres_hook import PostgresHook  # Import hinzugefügt

def get_data_from_postgres(): 
    postgres_conn_id = 'postgres_localhost'

    # Select Statement
    query_sql = """
        SELECT * FROM weather_data
    """

    # Postgres Hook verbinden
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    return pg_hook.run(sql=query_sql)