def drop_data(database_name: str, tables: list):
    '''database_name - name of database including tables to drop from
       tables - list of tables to drop, ex. tables= spark.catalog.listTables(db)
    '''

    if not database_name or not tables:
        raise ValueError

    for table in tables:
        spark.sql(f"DROP table {database_name}.{table}")
