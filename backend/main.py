from flask import Flask
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np

POSTGRES_USER="unicorn_user"
POSTGRES_PASSWORD="magical_password"
POSTGRES_DB="rainbow_database"


app = Flask(__name__)

def metric():
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@172.21.0.3:5432/{POSTGRES_DB}')
    df = pd.read_sql_query("SELECT * FROM vacations", engine)

    key_skills = df['key_skill'].drop_duplicates().to_list()[:-1]
    mrot = 16242

    metrics = {skill: 0 for skill in key_skills}

    for skill in key_skills:
        created_at = df[df['key_skill'] == skill]['created_at'].to_list()[:-1]
        # print(datetime.strptime(created_at[5], '%Y-%m-%d %H:%M:%S'))
        for date in created_at:
            d = datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S')
            metrics[skill] += (1.5**-((datetime.today() - d).total_seconds() / (24 * 60 * 60))) 
        
        salary_l = df[df['key_skill'] == skill]['vacancy_compensation_low'].dropna().to_list()
        salary = []
        for s in salary_l:
            if s is not np.nan and s is not None:
                salary.append(float(s) if float(s) > 10000 else float(s) * 70)
        metrics[skill] *= np.exp(np.mean(salary) / mrot)

    max_ = max(metrics.values())
    min_ = min(metrics.values())
    for k in metrics.keys():
        metrics[k] = (metrics[k] - min_) /(max_ - min_)
    return str(metrics)

def metric2():
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@172.21.0.3:5432/{POSTGRES_DB}')
    df = pd.read_sql_query("SELECT key_skill, COUNT(*) FROM vacations GROUP BY key_skill", engine)
    return str(df.to_dict())

@app.route('/')
def hello_world():
    return metric()

@app.route('/key_skills')
def bye_world():
    return metric2()

if __name__ == '__main__':
   app.run(host='0.0.0.0')
