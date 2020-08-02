import pandas as pd
import numpy as np
from twitter import *
import pymysql.cursors
from sqlalchemy import create_engine
from sqlalchemy.types import CHAR,INT
import datetime
import t_search
import t_tok

tok=t_tok.t_tok()
twitter = Twitter(auth = OAuth(tok[0],tok[1],tok[2],tok[3]))

statuses = twitter.statuses.user_timeline(screen_name = "@realDonaldTrump",count=20,tweet_mode='extended')

created_at=[]
for data in statuses:
    dt_time=datetime.datetime.strptime(data['created_at'].replace('+0000',''),'%a %b %d %H:%M:%S %Y')
    created_at.append(dt_time)
print(created_at)
retweet=[data['retweet_count'] for data in statuses]
favorite_count=[data['favorite_count'] for data in statuses]
text=[data["full_text"].replace("“","_").replace("’","_") for data in statuses]

df = pd.DataFrame({"create_time": created_at,
                    'retweet_count': retweet,
                    'favorite_count': favorite_count,
                    'text': text})


try:
    db_data = 'mysql+pymysql://root:123456@localhost:3306/twitter_1?charset=utf8mb4'
    engine = create_engine(db_data)
    print("成功連結")

    df.to_sql(name = 'twitter_1',  
            con = engine,
            if_exists = 'replace',
            index = False,
    )

except Exception as ex:
    print("問題發生:",ex)


print(df)