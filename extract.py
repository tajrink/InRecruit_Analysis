from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from config import connection
import pandas as pd

#create engine to connect to AWS DB
engine = create_engine(connection)

Base = automap_base()

Base.prepare(engine, reflect=True)

#save table as python object
jobs = Base.classes.jobs
salaries = Base.classes.salaries
skills = Base.classes.skills

#create temporary connection to DB
session = Session(engine)

#constructing sql query to pull data from table
jobs_stmt=session.query(jobs)
skills_stmt=session.query(skills)
salaries_stmt=session.query(salaries)

#load in pandas from queries
jobs_df=pd.read_sql(jobs_stmt.statement,con=engine.connect())
skills_df=pd.read_sql(skills_stmt.statement,con=engine.connect())
salaries_df=pd.read_sql(salaries_stmt.statement,con=engine.connect())

#merge dataframe
result1=pd.merge(jobs_df,salaries_df,on='id')
result2=pd.merge(result1,skills_df,on='id')

#
result2.to_csv("data/joined_data.csv",index=False)








