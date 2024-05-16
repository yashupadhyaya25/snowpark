from snowflake.snowpark import *

class snowflake():
    def __init__(self,
                 account_name,
                 user_name,
                 password,
                 role='ACCOUNTADMIN',
                 warehouse='COMPUTE_WH',
                 database='DATA_FLICKS',
                 schema='PUBLIC'):
        
        self.connection_parameters = {
        "account": account_name,
        "user": user_name,
        "password": password,
        "role": role,  # optional
        "warehouse": warehouse,  # optional
        "database": database,  # optional
        "schema": schema  # optional
        }  

    def session(self) :
        snowpark_session = Session.builder.configs(self.connection_parameters).create()
        return snowpark_session
