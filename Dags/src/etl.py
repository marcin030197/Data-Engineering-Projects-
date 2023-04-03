import airflow
import pandas as pd
from sqlalchemy import create_engine
import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag
from airflow.models import Variable

database=Variable.get("database")

def etl(ti):  
    '''
        This function runs etl process ( data transformation)

        Returns: table for BI - same amount of countries in every year
    ''' 

    dt = ti.xcom_pull(task_ids=['validation'])
    if dt[0]==True:
        print( 'validation - ok')
    else :
        raise Exception ('Error - validation')

    try:
        hook = PostgresHook(postgres_conn_id="postgres")
        countries = hook.get_pandas_df('''
                select 
                    id,
                    country,
                    "year" ,
                    healthy_life_expectancy_at_birth ,
                    freedom_to_make_life_choices ,
                    generosity 
                from project.countries
                where year> 2010
                ;
            ''')
    except:
        raise Exception ('Error loading data from database')
    
    countries_agg=countries.groupby('country').agg({"id":"count"})
    countries_agg=countries_agg.reset_index()
    countries_agg=countries_agg.rename(columns={"id": "number_of_countries"})

    max_numbers_of_years = max(countries_agg['number_of_countries'])
    countries_agg=countries_agg[(countries_agg['number_of_countries']>=max_numbers_of_years)]

    countries = pd.merge(countries,countries_agg,on='country',how='inner')
    countries= countries.drop(columns=['number_of_countries'])

    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('''
                        drop table if exists project.f_countries;  
                        CREATE TABLE IF NOT EXISTS project.f_countries(
                                id integer,
                                country varchar,
                                "year" integer,
                                healthy_life_expectancy_at_birth float,
                                freedom_to_make_life_choices float,
                                generosity float)
                        '''
                    )
        cursor.close()
        conn.commit()

        engine = create_engine(database)
        countries.to_sql('f_countries',engine,schema='project', if_exists='append', index = False)
    except:
        raise Exception ('Error loading data to database')
    

if __name__ == "__main__":
   etl()