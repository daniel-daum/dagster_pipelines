
# Data Pipelines

This repository contains my personal data pipelines. 
I am using Python, Dagster, and DBT,  to orchestratate and transform relevant data.

 I am using a Postgres instance as the 'warehouse' that transformations occur in.


 I have 3 'environments:
 
 - A 'dev' envrionment, running from source (pdm start), with env vars set from a .env file that ovverrides the os env vars.
 - A 'stage' envrionment, running in containers (docker compose up), which pulls env vars from the host
 - A 'prod' envrionment, running in docker compose on a virtual machine, updated via github actions cicd pipeline. 

### Pipelines
- **USAF Docket Pipeline**: A data pipeline that pulls, stores, and transforms Air Force judicial punishment data from the UAF Docket site. I am creating a dashboard that analyzes active cases and historical case outcomes. Source Data: https://legalassistance.law.af.mil/AMJAMS/PublicDocket/docket.html 

### Requirements
This project will requrire a S3 bucket (as an io manager), and postgres database. Additionally, it will require the following env variables. If in dev envrionment, you can set these using a .env file, which is set to ovveride the host os env variables:

- WAREHOUSE:  A SQLALCHEMY postgres db connection string
- TARGET: dev, stage, or prod [choose target env]
- AWS_SECRET_ACCESS_KEY: Secret key to access s3 bucket io manager
- AWS_ACCESS_KEY_ID: Access key to access s3 bucket io manager
- DBT_ENV_SECRET_HOST: postgres warehouse db host url
- DBT_ENV_SECRET_USER: postgres warehouse username
- DBT_ENV_SECRET_PASS: postgres warehouse password
### Development
This project uses Python 3.11. PDM

**Development Pre-requisites:**
- Python 3.11
- PDM (Package Manager)
- Docker (If you plan to run this in a container)

To set up a development environment:

- Globally install PDM
```bash
python -m pip install --upgrade pip

pip install pdm
```

- Clone the repository
```bash
git clone https://github.com/daniel-daum/data_pipelines.git
```

- Change directories into the repo
```bash
cd data_pipelines
```

- Create a virtual environment (I call mine venv)
```bash
python -m venv venv
```
- Install dependencies with PDM (run at project root directory)
```bash
pdm install
```

- Run Dagster via pdm 
```bash
pdm start
```

***NOTE:** Running locally vs running with Docker you will have to change some configs. This repo is configured to run as docker compose by default. to run the dagster server from source you will have to uncomment code in two files, one related to dagster and one related to dbt.* 

When running locally you will have to change two files:

- workspace.yaml -> uncomment and use "load from: data_pipelines"
- ./dbt_pipelines/config/profiles -> uncomment and switch "target" to "dev"

### Deployment
This repository is containerized and deployed using docker compose. 

To Run with Docker:

- Clone the repo
```bash
git clone https://github.com/daniel-daum/data_pipelines.git
```


- Change directories into the repo
```bash
cd data_pipelines
```

- Run container via docker compose
```bash
docker compose up
```

- The dagit webserver will startup on http://localhost:3000
- The Metabase instance will startup on http://localhost:5000

