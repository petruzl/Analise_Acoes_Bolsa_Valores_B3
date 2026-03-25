from sqlalchemy import create_engine

def get_engine():
    server = 'localhost'  
    database = 'AnaliseAcoes'
    
    connection_string = f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    
    engine = create_engine(connection_string)
    
    return engine