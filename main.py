from modules.SNOWFLAKE import snowflake
from snowflake.snowpark.functions import col

class main():
    def __init__(self) -> None:
        self.snowflake_obj = snowflake(
            account_name='hhsouhq-mk47637',
            user_name='Yash',
            password='Voda@1234',
            schema='DBT_RAW'
        )
        
    def query(self) :
        query = 'Select * from TITLES'    
        snow_session = self.snowflake_obj.session()
        title_table = snow_session.sql(query)
        movie_df = title_table.filter(col("TYPE")=='MOVIE')
        show_df = title_table.filter(col("TYPE")=='SHOW')
        movie_df.write.mode("overwrite").save_as_table("PUBLIC.snow_park_movie", table_type="transient")
        show_df.write.mode("overwrite").save_as_table("PUBLIC.snow_park_show", table_type="transient")

if __name__ == '__main__' :
    main_obj = main()
    main_obj.query()
