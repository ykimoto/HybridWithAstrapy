# Bike Recommendation Engine
Uses a small set of bike review/description to generate bike recommendations.

## Setup
- Create .env file with following values
```sh
  OPENAI_API_KEY=""
  ASTRA_SECUREBUNDLE_PATH=""
  ASTRA_DB_TOKEN=""
  ASTRA_KEYSPACE=""
  ASTRA_TABLE=""
  TRACELOOP_API_KEY=""
  LIMIT_TOP_K="3"
```
Provide TRACELOOP_API_KEY only if you want telemetry to be enabled

## Load data
```sh
python load_embeddings.py
```
## Run demo from command line
```sh
python demo.py
```
## Launch UI
This app uses streamlit to run UI
```sh
streamlit run demo-ui.py
```
## Open Telemetry/Traceloop view
![Open AI Chat Trace](https://github.com/mangatrai/vector-db-examples/assets/13439074/78ac563e-1187-4baf-b64c-0d7b45a2fe95)

![Open AI Completion Trace](https://github.com/mangatrai/vector-db-examples/assets/13439074/f1b85aa0-46a9-4ccb-856e-9fb0aa1c3537)