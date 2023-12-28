from astrapy.db import AstraDBCollection

import openai, os, uuid, requests
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

@task(name="Establish Astra DB Connection and get Collection")
def create_connection():
    #Establish Connectivity and get the collection
    collection = AstraDBCollection(
        collection_name=ASTRA_COLLECTION, token=ASTRA_DB_APPLICATION_TOKEN, api_endpoint=ASTRA_DB_API_ENDPOINT
    )
    return collection

@task(name="Embed Input Query")
def embed_query(customer_input):
    # Create embedding based on same model
    embedding = openai.embeddings.create(input=customer_input, model=model_id).data[0].embedding
    return embedding

@task(name="Build top k simple query")
def build_simple_query(customer_input, k):
    params = {}
    params['embedding'] = embed_query(customer_input)
    params['k'] = k
    return params

@task(name="Build top k hybrid query")
def build_hybrid_query(customer_input, filter, k):
    params = {}
    params['embedding'] = embed_query(customer_input)
    params['k'] = k
    params['filter'] = filter
    return params

@task(name="Perform ANN search on Astra DB")
def query_astra_db(collection, params):
    if 'filter' in params:
        results = collection.vector_find(
            vector=params['embedding'],
            limit=params['k'],
            filter={"type": params['filter']},
            fields=["type", "brand", "model", "price", "description"],
        )
    else:
        results = collection.vector_find(
            vector=params['embedding'],
            limit=params['k'],
            fields=["type", "brand", "model", "price", "description"],
        )
    bikes_results = pd.DataFrame(results)
    return bikes_results

@task(name="Ask user for Query Mode")
def ask_user_query_mode(option1, option2):
   print("Please choose between the following options:")
   print(f"1. {option1}")
   print(f"2. {option2}")
   
   user_choice = input("Enter your choice (1 or 2): ")
   # Check the user's input and execute the appropriate code.
   if user_choice == "1":
       return option1
   elif user_choice == "2":
       return option2
   else:
    print("Invalid choice. Please enter 1 or 2.")
    return ask_user_query_mode(option1, option2)

@workflow(name="Execute Bike Recommendation Query")
def execute_demo():
    collection = create_connection()
    query = input("Please enter a question about Bikes in the catalog: ")
    k = input("Please Enter the number of results to show: ")
    query_mode = ask_user_query_mode("simple", "hybrid")

    if(query_mode == "hybrid"):
        filter = input("Please enter the type of Bike you're looking for (e.g. Kids Bike, eBikes, mountain bike, commuter bike, etc): ")
        params = build_hybrid_query(query, filter, k)
    else:
        params = build_simple_query(query, k)

    bikes_results = query_astra_db(collection, params)
    print(bikes_results)

execute_demo()