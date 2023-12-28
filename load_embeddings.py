from astrapy.db import AstraDB, AstraDBCollection

import openai, os, uuid, time, requests, traceback, math
import pandas as pd

from traceloop.sdk import Traceloop
from traceloop.sdk.tracing import tracing as Tracer
from traceloop.sdk.decorators import workflow, task, agent
from dotenv import load_dotenv, find_dotenv

# Load the .env file
if not load_dotenv(find_dotenv(),override=True):
    raise Exception("Couldn't load .env file")

#Add Telemetry
TRACELOOP_API_KEY=os.getenv('TRACELOOP_API_KEY')
Traceloop.init(app_name="Bike Recommendation App", disable_batch=True)
# Generate a UUID
uuid_obj = str(uuid.uuid4())
Tracer.set_correlation_id(uuid_obj)

#declare constant
ASTRA_DB_APPLICATION_TOKEN=os.getenv('ASTRA_DB_APPLICATION_TOKEN')
ASTRA_DB_API_ENDPOINT=os.getenv('ASTRA_DB_API_ENDPOINT')
ASTRA_COLLECTION=os.getenv('ASTRA_COLLECTION')

openai.api_key = os.getenv('OPENAI_API_KEY')
model_id = "text-embedding-ada-002"

@task(name="Establish Astra DB Connection")
def create_connection():
    #Establish Connectivity
    astra_db = AstraDB(token=ASTRA_DB_APPLICATION_TOKEN, api_endpoint=ASTRA_DB_API_ENDPOINT)
    return astra_db

@task(name="Refresh Collection")
def refresh_collection(astra_db):
    # Check whether the collection exists
    collection_list = astra_db.get_collections()
    if ASTRA_COLLECTION not in collection_list.values():
        # Create the collection
        astra_db.create_collection(ASTRA_COLLECTION)
        print("Collection Created")
    else:
        # Truncate the collection
        astra_db.truncte_collection(ASTRA_COLLECTION)
        print("Collection Truncated")


@task(name="Download Raw json data file from source")
def load_data_file():
    #load data from sample json file
    url = "https://raw.githubusercontent.com/ykimoto/HybridWithAstrapy/main/data/bikes.json"
    response = requests.get(url)
    bikes = response.json()
    print("Bike file loaded")
    bikes = pd.DataFrame(bikes)
    return bikes

@task(name="Create and load Embeddings")    
def create_load_embeddings(bikes, session):
   for id in bikes.index:
      description = bikes['description'][id].replace(',', '\,')
      description = description.replace('"', '\"')
      image = bikes['image'][id]

      try:
         if float(image):
             image ="https://img1.cgtrader.com/items/3587445/dcfbb2669c/large/road-bike-generic-rigged-3d-model-max.jpg"
      except ValueError:
          # do nothing
          pass

      # Create Embedding for each bike row, save them to the database
      full_chunk = bikes['description'][id]
      embedding = openai.Embedding.create(input=full_chunk, model=model_id)['data'][0]['embedding']
      #print("Embeddings Created - Count " + str(id))
      query = SimpleStatement(f"""INSERT INTO bike_rec.bikes(model, brand, price, image, type, description, description_embedding) VALUES (%s, %s, %s, %s, %s, %s, %s)""")
     
      # Create a try-catch block
      try:
         session.execute(query, (bikes['model'][id], bikes['brand'][id], bikes['price'][id], image, bikes['type'][id], description, embedding), trace=True)
         print("Record Inserted: " + str(id))
      except Exception as e:
          # Log the exception
          traceback.print_exc()
          print(e)
          break

@workflow(name="Load Bike Recommendation Data")
def run_loading_data():
    #Create Connection
    astra_db = create_connection()
    
    #Create or Truncate the Bike Catalog Collection
    refresh_collection(astra_db)

    bikes = load_data_file()
    
    #call embedding function and load data
    create_load_embeddings(bikes, session)

run_loading_data()