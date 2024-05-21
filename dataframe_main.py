from modules.SNOWFLAKE import snowflake
from snowflake.snowpark.functions import col
from snowflake.snowpark import Row
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

    ## Method to create DataFrame ##
    def create_df(self):
        snow_session = self.snowflake_obj.session()

        ## Method - 1
        sf_df = snow_session.create_dataframe(
            data=[[1,'Jon','Snow'],[2,'Lucifer','Morning Star']],
            schema=['ID','FIRST_NAME','LAST_NAME']
            )
        print('### Method - 1 ###')
        # sf_df.show()
        print('### Method - 1 ###')

        ## Method - 2 using Row Function
        sf_df = snow_session.create_dataframe(data=[
            Row(ID=1,FIRST_NAME='Jon',LAST_NAME='Snow'),
            Row(ID=2,FIRST_NAME='Lucifer',LAST_NAME='Morning Star')
            ])
        print('### Method - 2 ###')
        # sf_df.show()
        print('### Method - 2 ###')

        ## Method - 3 Defining Schema
        schema = StructType([StructField('ID',IntegerType()),
                             StructField('FIRST_NAME',StringType(25)),
                             StructField('LAST_NAME',StringType(25))
                             ])
        sf_df = snow_session.create_dataframe(data=[
            Row(ID=1,FIRST_NAME='Jon',LAST_NAME='Snow'),
            Row(ID=2,FIRST_NAME='Lucifer',LAST_NAME='Morning Star'),
            ],
            schema=schema
            )
        print('### Method - 3 ###')
        # df = sf_df.to_pandas()
        print(sf_df.schema)
        print('### Method - 3 ###')
        snow_session.close()

    ## Query and using filter transformation and selecting only columns which we need
    def query(self) :
        query = 'Select * from NETFLIX.TITLES'    
        snow_session = self.snowflake_obj.session()
        title_table = snow_session.sql(query)
        ## You can also select column like df1 = df.select[df['col1'],df['col2']] or df1 = df.select(df.col1,df.col2) or df1 = df.select("col1","col2")
        movie_df = title_table.filter(col("TYPE")=='MOVIE').select(col('ID'),col('TITLE'))
        show_df = title_table.filter(col("TYPE")=='SHOW')
        # movie_df.write.mode("overwrite").save_as_table("NETFLIX.snow_park_movie", table_type="transient")
        # show_df.write.mode("overwrite").save_as_table("NETFLIX.snow_park_show", table_type="transient")
        snow_session.close()

    ## Joining operation between two dataframe
    def join(self) :
        snow_session = self.snowflake_obj.session()
        title_df = snow_session.table('NETFLIX.TITLES')
        credits_df = snow_session.table('NETFLIX.CREDITS')
        join_df = credits_df.join(title_df,credits_df.col('"id"')==title_df.ID,how='inner')\
        .select('ID','"person_id"','"name"',title_df['TITLE'].alias('Movie_Title'),credits_df['"role"'].as_("Person_Role"))
        join_df.show(100)
        snow_session.close()
    
    ## Create a View
    def view(self):
        snow_session = self.snowflake_obj.session()
        credits_df = snow_session.table('NETFLIX.CREDITS')
        actor_view = credits_df.filter(credits_df['"role"'] == 'ACTOR').create_or_replace_view('NETFLIX.actor_v')
        director_view = credits_df.filter(credits_df['"role"'] == 'DIRECTOR').create_or_replace_view('NETFLIX.director_v')
        snow_session.close()

    def read_file_from_stage(self):
        snow_session = self.snowflake_obj.session()
        ## CSV Read
        stage_df = snow_session.read\
        .options({"field_delimiter" : ";",
                  "PARSE_HEADER" : True, 
                  "infer_schema" : True ## This will incur schema from stage
                  })\
        .csv("@snowflake_stage/company_user.csv")
        # .schema(
        #     StructType([
        #         StructField('Login_email',StringType(30)),
        #         StructField('Identifier',IntegerType()),
        #         StructField('First_name',StringType(25)),
        #         StructField('Last_name',StringType(25))
        #     ])
        #         )\
        # .csv("@snowflake_stage/company_user.csv")
        stage_df.show()

        ## JSON Read
        stage_json_df = snow_session.read\
        .options({'strip_outer_array':True})\
        .json('@snowflake_stage/nested_json.json')
        stage_json_df = stage_json_df.select(
            stage_json_df["$1"]["id"].alias('ID'),
            stage_json_df["$1"]["batters"]["batter"].alias('batter')
            )\
            .join_table_function('flatten','batter')\
            .select('ID','"VALUE"')\
            .select(col('ID').cast(IntegerType()).alias('ID'),col("VALUE")['id'].cast(IntegerType()).alias('batter_id'),col("VALUE")['type'].cast(StringType(25)).alias('batter_type'))
        stage_json_df.show()
        snow_session.close()

if __name__ == '__main__' :
    # pass
    main_obj = main(database='SNOWPARK')
    # main_obj.create_df()
    # main_obj.query()
    # main_obj.join()
    # main_obj.view()
    # main_obj.read_file_from_stage()