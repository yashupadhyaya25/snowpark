from modules.SNOWFLAKE import snowflake
from configparser import ConfigParser
from snowflake.snowpark.functions import sproc,sum
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

    def anonymous_sproc(self):
        snow_session = self.snowflake_obj.session()
        sproc(lambda snow_session,x : snow_session.sql(f"select {x} + 1").collect()[0][0],
                        return_type=IntegerType(), 
                        input_types=[IntegerType()], 
                        name="my_sproc", 
                        replace=True,
                        packages=["snowflake-snowpark-python"])
        print(snow_session.call("my_sproc",1))
        snow_session.close()
    
    def _sproc(self) :
        snow_session = self.snowflake_obj.session()
        @sproc(name='sp_total_sales_by_day',
               is_permanent=True,
               stage_location='@snowflake_stage',
               replace=True,
               return_type=StringType(),
               packages=["snowflake-snowpark-python"]
               )
        def sp_total_sales_by_day(snow_session):
            sales_df = snow_session.table('NETFLIX.SALES')
            df = sales_df.group_by('ORDERDATE').agg(sum('UNITPRICE').alias('AMOUNT'))
            df.write.mode("overwrite").save_as_table("NETFLIX.SALES_BY_DATE", table_type="transient")
            return "Success"

        print(snow_session.call("sp_total_sales_by_day"))
        snow_session.close()

if __name__ == '__main__' :
    main_obj = main(database='SNOWPARK')
    # main_obj.anonymous_sproc()
    # main_obj._sproc()
    pass