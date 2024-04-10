# Project Setup

To get started with our Instacart Data Engineering Project, follow these setup instructions:

1. **Environment Configuration:**

   - Ensure Python 3.8+, Docker, and Docker Compose are installed on your system.
  
2. **Set up a Google Cloud Platform (GCP)** 

	- Set up your GCP account
	- Create your GCP project
	- Create a service account with the following permission role: **Storage Admin** + **Storage Object Admin** + **BigQuery Admin**
	- Download the `service-account-keys.json` file, rename it to `dezc-credentials.json` and store at in `~/.google/credentials/dezc-credentials.json`

3. **Set up Environment Variable**

	- copy the `.env.sample` to `.env`
	- you can keep all the values the same

4. **Repository Clone**

   - Clone the project repository from GitHub to your local machine.

5. **Provision Cloud Infrastructure**

   - Use Terraform to create and manage GCP resources, including Google Cloud Storage and BigQuery by running the following comand
	```sh
	# run the following command inside `terraform` directory

	# Initializes a Terraform configuration directory
	terraform init
	
	# Generates an execution plan
	terraform plan

	# Applies the changes defined in the Terraform plan
	terraform apply
	```

6. **Run applications inside containers**

	```sh
	# run the following command inside `workflow` directory

	# initialize Airflow environment with necessary setup and configurations.
	docker compose up airflow-init

	# launch all services defined in the Docker Compose file in detached mode for background running.
	docker compose up -d
	```

7. **Download Dataset**
	- visit [Instacart Market Basket Analysis](
https://www.kaggle.com/c/instacart-market-basket-analysis), download the dataset, and place it inside `workflow/data` directory

1. **Workflow Orchestration**
	- visit `http://localhost:8080` and login to Airflow with the following credential
    	- username: airflow
		- password: airflow
	- run the following DAGs with the following orders
    	- Run the s1_ingest_to_gcs DAG
    	- Run the s2_ingest_to_bigquery DAG
		- Run the s3_dbt_build DAG

2. **Dashboard Setup**
	- visit `http://localhost:3000/` to access Metabase
	- connect it to BigQuery using the `dezc-credentials.json` to access and visualize the data
	

