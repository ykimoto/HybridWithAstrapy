from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory, SimpleStatement
import openai, os, uuid, time, requests, traceback
from traceloop.sdk import Traceloop
from traceloop.sdk.tracing import tracing as Tracer
from traceloop.sdk.decorators import workflow, task, agent
from dotenv import load_dotenv, find_dotenv
import pandas as pd

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
ASTRA_DB_SECURE_BUNDLE_PATH=os.getenv('ASTRA_SECUREBUNDLE_PATH')
ASTRA_DB_APPLICATION_TOKEN=os.getenv('ASTRA_DB_TOKEN')
ASTRA_DB_KEYSPACE=os.getenv('ASTRA_KEYSPACE')
openai.api_key = os.getenv('OPENAI_API_KEY')
model_id = "text-embedding-ada-002"

@task(name="Create Cassandra Connection")
def create_connection():
    #Establish Connectivity
    cluster = Cluster(
    cloud={
        "secure_connect_bundle": ASTRA_DB_SECURE_BUNDLE_PATH,
    },
    auth_provider=PlainTextAuthProvider(
        "token",
        ASTRA_DB_APPLICATION_TOKEN,
    ),
    )
    session = cluster.connect()
    keyspace = ASTRA_DB_KEYSPACE
    return session, keyspace

@task(name="Embed Input Query")
def embed_query(customer_input):
    # Create embedding based on same model
    embedding = openai.Embedding.create(input=customer_input, model=model_id)['data'][0]['embedding']
    return embedding

@task(name="Build top k simple query")
def build_simple_query(customer_input, keyspace, k):
    embedding = embed_query(customer_input)
    query = SimpleStatement(
    f"""
    SELECT *
    FROM {keyspace}.bikes
    ORDER BY description_embedding ANN OF {embedding} LIMIT {k};
    """
    )
    return query

@task(name="Build top k hybrid query")
def build_hybrid_query(customer_input, keyspace, filter, k):
    embedding = embed_query(customer_input)
    hybrid_query = SimpleStatement(
    f"""
    SELECT *
    FROM {keyspace}.bikes
    WHERE type : '{filter}'
    ORDER BY description_embedding ANN OF {embedding} LIMIT {k};
    """
    )
    return hybrid_query

@task(name="Perform ANN search on Astra DB")
def query_astra_db(session, query):
    results = session.execute(query)
    top_results = results._current_rows
    bikes_results = pd.DataFrame(top_results)
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
    session, keyspace = create_connection()
    query = input("Please Enter your Bike Question: ")
    k = input("Please Enter Number of Results: ")
    query_mode = ask_user_query_mode("simple", "hybrid")

    if(query_mode == "hybrid"):
        filter = input("Please Enter Bike Type: (e.g. Kids Bike or eBikes)")
        db_query = build_hybrid_query(query, keyspace, filter, k)
    else:
        db_query = build_simple_query(query, keyspace, k)

    bikes_results = query_astra_db(session, db_query)
    print(bikes_results)

execute_demo()