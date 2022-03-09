#Función para levantar tuits usando el endpoint de search tuits.
#Reemplazar el endpoint por el search_full_tuits. Acá lo uso porque se precisa
#Tweet Fields: https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-recent

import tweepy as tp
import pandas as pd
import boto3
from io import StringIO
import logging
from datetime import datetime
import os
import pdb
import time



def upload_s3(bucket, prefix, df):
  time.sleep(1)
  now = datetime.now()

  year = now.strftime("%Y")
  month = now.strftime("%m")
  day = now.strftime("%d")
  actual_time = now.strftime("%H%M%s")

  date_fn = f"{year}-{month}-{day}-{actual_time}"

  key = os.path.join(prefix + date_fn + ".csv")

  csv_buf = StringIO()
  df.to_csv(csv_buf, header=True, index=False)
  csv_buf.seek(0)
  s3_client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=key)

  ###Comprimir archivo!
  logger.info(f"Tweets Guardados. Nombre del archivo: {key}")

def get_tweets(user, max_results,limit):
  #Max results: número máximo de tuits por request. Buscar en documentación máximo y fijar allí!!!
  #Limit: cantidad máxima de tuits que quiero recuperar para el usuario.
  #En el futuro debería cambiarse el endpoint contra el que funciona para adaptar a API académica. 
  query = f'from:{user}' #Query para recuperar tuits. En el futuro debe agregarse el from/to
  user_tweets = [] #Lista para acumular tuits
  for tweet in tp.Paginator(tp_client.search_recent_tweets, query=query,tweet_fields=['id','created_at', 'text'], max_results=max_results).flatten(limit=limit):
    #Realiza consulta y devuelve resultados paginados. Itera sobre cada tweet recuperado
    data = [user]+list(tweet.data.values())
    user_tweets.append(data)

  logger.info(f"Consulta user: {user}. Agregando resultados a tabla temporal parcial. Tweets descargados para este user: {str(len(user_tweets))}")
  
  return pd.DataFrame(user_tweets) 

if __name__=="__main__":
  
  logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
        filename="/home/ec2-user/logs/get-tweet.log",
    )
  logger = logging.getLogger("get-tweet.log")

  logger.info("Lanzando aplicación.")
  
  ###########Parametros#########

  #Esto se va a usar con la cuenta academica para limitar el rango de fechas
  #from = 
  #to = 
  user_list = user_list = ['biabionica','GonzaloGarces5','infobae', 'eldestapeweb', 'LNV22', 'U24noticias', 'clarincom', 'JMilei', 'ZetaOrlando', 'diegocabot', 'laderechamedios', 'nanothompson', 'edufeiok', 'alferdez', 'infobaeeconomia','segregustavo', 'FerIglesias', 'Ambitocom','BancoCentral_AR', 'A24COM','madorni','LANACION','todonoticias'] # Esto debe levantarse de archivo. Agregar aleatoreidad!
  max_results = 100 #Maximo de la API
  limit = 5000

  ###Bucket de destino de la data
  bucket = "twitter-project-daromi"
  prefix = "data/p.expectativas/get_tweets/"

  ###Despues levantar de parameter store
  bearer_token = "AAAAAAAAAAAAAAAAAAAAAENM8wAAAAAAUtlf0HA8rHCGmQuAzAI%2BIRET1Os%3Ds4HJZP6SMHiungfwqgwHdTVRmcv7eCTnDCM2PYyxf6s9FM3B1u"
  ##############################

  tp_client = tp.Client(bearer_token=bearer_token,  wait_on_rate_limit=True)
  s3_client = boto3.client("s3")

  tuits_df = pd.DataFrame()

  for user in user_list:
    logger.info(f"Recuperando Tweets de: {user}.")
  
    if len(tuits_df)>15: #Guardo cada n tuits recuperados. Reinicio DataFrame vacío.

      upload_s3(bucket, prefix, tuits_df)

      tuits_df = pd.DataFrame()

    try:    
      tuits_df = pd.concat([tuits_df, get_tweets(user, max_results,limit)])
    except Exception as e:
      logger.error(f"Error consultando la cuenta del user {user}. Datos de la excepción {e}.")
    
    upload_s3(bucket, prefix, tuits_df)