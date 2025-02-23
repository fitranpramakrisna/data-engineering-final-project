# Indonesian Tech Job Market Analysis
This is an end-to-end data engineering project that builds a data pipeline for the Indonesian tech job market. The processed data is used to create a dashboard in Looker Studio. The ETL process runs using batch processing, scheduled daily at 10:00 AM.
![image](https://github.com/user-attachments/assets/4b1c3259-fdbf-44d5-a4a4-ceb94061ab06)

# Tools:
1. Docker
2. Airflow
3. Python
4. Google BigQuery
5. Looker Studio

### How to set up docker
1. Clone this repo
2. Go to folder /airflow-docker
3. Execute `docker-compose build`
4. After it's done, also execute `docker-compose up -d`


### Folder Structure
`/airflow-docker/dags`

This folder contains the main function to run and display the dag for the airflow, name of the dag file is `etl_job_market.py`

`/airflow-docker/resources/csv`
This folder contains csv file as the temp file to later upload and move to the bigquery data warehouse

`/airflow-docker/resources/scripts`

This folder contains 4 folders that work together to tailor the function of dag file.
- `extract_dealls.py`, python script to scrap / extract the data from Dealls Web
- `extract_kalibrr.py`, python script to scrap / extract the data from Kalibrr Web
- `mapping_industry.py`, python script to map/match industry name for standarizing the name of the industry
- `mapping_job_title.py`, python script to map/match job title name for standarizing the name of the job title
- `transform_dealls.py`, python script to transform the extracted data from Dealls
- `transform_kalibrr.py`, python script to transform the extracted data from Kalibrr

To visit the end product (the dashboad) you can visit this link [Dashboard](https://lookerstudio.google.com/reporting/3a3cade2-f0c1-4613-b937-1221ece8b816)
