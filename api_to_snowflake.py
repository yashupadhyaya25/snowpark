import requests as rq
from configparser import ConfigParser
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
from modules.SNOWFLAKE import snowflake
import pandas as pd

## Evnironment Variable ##
ENV = 'development'
config = ConfigParser()
config.read('config.ini')
SF_ACCOUNT_NAME = config.get(ENV,'account_name')
SF_ACCOUNT_USER_NAME = config.get(ENV,'user_name')
SF_ACCOUNT_PASSWORD = config.get(ENV,'password')
## Evnironment Variable ##

class main():
    def __init__(self,database) -> None:
        self.snowflake_obj = snowflake(
            account_name=SF_ACCOUNT_NAME,
            user_name=SF_ACCOUNT_USER_NAME,
            password=SF_ACCOUNT_PASSWORD,
            database=database
        )

    def api_call(self):
        snow_session = self.snowflake_obj.session()
        user_url = "https://api.escuelajs.co/api/v1/users"
        user_res = rq.get(url=user_url)
        user_json_data = user_res.json()
        user_df = snow_session.create_dataframe(
            data=user_json_data
        )
        user_df.write.mode('overwrite').save_as_table('NETFLIX.API_USER',table_type='transient')
        user_df.show()
        snow_session.close()

if __name__ == '__main__' :
    # main_obj = main(database='SNOWPARK')
    # main_obj.api_call()
    pass