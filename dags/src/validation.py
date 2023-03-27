import airflow
import pandas as pd
from sqlalchemy import create_engine
import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag

def validation():
    try:
        hook = PostgresHook(postgres_conn_id="postgres")
        countries = hook.get_pandas_df('''
                select 
                    * 
                from project.countries;
            ''')
    except:
        raise Exception ('Error loading data from database')

    if (countries['id']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is not unique")
    
    if countries.isnull().values.any():
        raise Exception("Null values found")

    column_4 = countries[countries.columns[4]].to_list()
    for i,x in enumerate(column_4):
        if column_4[i] <0 :
            raise Exception ('values in "freedom_to_make_life_choices" less than 0')
        if column_4[i] >1:
            raise Exception ('values in "freedom_to_make_life_choices" greater than 1')
        else: 
            pass

    column_5 = countries[countries.columns[5]].to_list()
    for i,x in enumerate(column_5):
        if column_5[i] <-1 :
            raise Exception ('values in "Generosity" less than -1')
        if column_5[i] >1:
            raise Exception ('values in "Generosity" greater than 1')
        else: 
            pass

if __name__ == "__main__":
    validation()