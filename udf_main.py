from modules.SNOWFLAKE import snowflake
from configparser import ConfigParser
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField

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
    
    def create_df(self):
        snow_session = self.snowflake_obj.session()

        concat_name_udf = snow_session.udf.register_from_file(
            file_path='./udfs.py',
            func_name='fullname_udf',
            is_permanent=False,
            return_type=StringType(),
            input_types=[StringType(),StringType()]
        )

        sf_df = snow_session.create_dataframe(
            data=[[1,'Jon','Snow'],[2,'Lucifer','Morning Star']],
            schema=['ID','FIRST_NAME','LAST_NAME']
        )
        
        sf_df.select('ID',concat_name_udf(["FIRST_NAME","LAST_NAME"]).alias('FULL_NAME'))

        sf_df.show()

if __name__ == '__main__' :
    main_obj = main(database='SNOWPARK')
    main_obj.create_df()
    # pass