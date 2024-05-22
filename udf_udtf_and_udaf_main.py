from modules.SNOWFLAKE import snowflake
from configparser import ConfigParser
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
from snowflake.snowpark.functions import udtf,udaf
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
    
    ## Create User Define Function
    def create_udf(self):
        snow_session = self.snowflake_obj.session()

        concat_name_udf = snow_session.udf.register_from_file(
            file_path='./udfs.py',
            func_name='fullname_udf',
            is_permanent=True,
            return_type=StringType(),
            input_types=[StringType(),StringType()],
            name='fullname_udf',
            stage_location='@snowflake_stage',
            packages=['pandas'],
            replace=True
        )

        sf_df = snow_session.create_dataframe(
            data=[[1,'Jon','Snow'],[2,'Lucifer','Morning Star']],
            schema=['ID','FIRST_NAME','LAST_NAME']
        )
        
        sf_df = sf_df.select('ID',concat_name_udf(["FIRST_NAME","LAST_NAME"]).alias('FULL_NAME'))

        sf_df.show()
        snow_session.close()

    ## Create User Define Table Function
    def create_udtfs(self) :
        snow_session = self.snowflake_obj.session()
        sf_df = snow_session.create_dataframe(
            data=[[1,'Jon','Snow'],[2,'Lucifer','Morning Star']],
            schema=['ID','FIRST_NAME','LAST_NAME']
        )

        full_name_udft  = snow_session.udtf.register_from_file(
            file_path='./udfts.py',
            stage_location="@snowflake_stage",
            handler_name="full_name",
            output_schema=StructType([StructField("FULL_NAME", StringType())]),
            input_types=[StringType(),StringType()]
        )

        sf_df = sf_df.select('ID',full_name_udft("FIRST_NAME","LAST_NAME"))
        sf_df.show()

        snow_session.close()

    ## Create User Define Aggregate Function
    def create_udaf(self) :
        snow_session = self.snowflake_obj.session()
        sales_df = snow_session.table('NETFLIX.SALES')
        sales_by_date = snow_session.udaf.register_from_file(
            file_path='./udafs.py',
            stage_location="@snowflake_stage",
            handler_name="sales_by_date",
            return_type=IntegerType(),
            input_types=[IntegerType()]
        )
        sales_df = sales_df.agg(sales_by_date("UNITPRICE").alias('TOTAL_AMOUNT'))
        sales_df.show()
        snow_session.close()
        
if __name__ == '__main__' :
    main_obj = main(database='SNOWPARK')
    # main_obj.create_udf()
    # main_obj.create_udfts()
    main_obj.create_udaf()
    # pass