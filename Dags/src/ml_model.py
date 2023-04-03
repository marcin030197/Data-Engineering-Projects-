import airflow
import pandas as pd
from sqlalchemy import create_engine
import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag
from airflow.models import Variable

database=Variable.get("database")

def ml_model(ti):
    '''
        Function for ML pipeline

        Returns: prediciton of life_expectancy (above or below benchmark) depends on freedom_to_make_life_choices and generosity

    '''
    import sklearn
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestClassifier

    dt = ti.xcom_pull(task_ids=['validation'])
    if dt[0]==True:
        print( 'validation - ok')
    else :
        raise Exception ('Error - validation')

    try:
        hook = PostgresHook(postgres_conn_id="postgres")
        model = hook.get_pandas_df('''
            select 
                freedom_to_make_life_choices,
                generosity,
                project.life_expectancy_bench(c.healthy_life_expectancy_at_birth)
		    from project.countries c ;
            ''')
    except:
        raise Exception ('Error loading data from database')
    
    X=model.drop("life_expectancy_bench", axis=1)
    y=model["life_expectancy_bench"].values.ravel()
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.33, random_state = 1)
    rfc = RandomForestClassifier(random_state=0, max_depth=2, n_estimators=100)
    rfc.fit(X_train, y_train)
    results = model.loc[X_test.index].copy()
    results['prediction'] = rfc.predict(X_test)

    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('''
                        drop table if exists project.countries_test_prediction;  
                        CREATE TABLE IF NOT EXISTS project.countries_test_prediction(
                                freedom_to_make_life_choices float,
                                generosity float,
                                life_expectancy_bench integer,
                                prediction integer
                                )
                        '''
                    )
        cursor.close()
        conn.commit()

        engine = create_engine(database)
        results.to_sql('countries_test_prediction',engine,schema='project', if_exists='append', index = False)
    except:
        raise Exception ('Error loading data to database')

if __name__ == "__main__":
    ml_model()