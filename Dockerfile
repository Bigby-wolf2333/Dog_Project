FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py
CMD ["--user", "root", "--password", "root", "--host", "pgdatabase", "--port", "5432", "--db", "dog_adoption", "--table_name", "description", "--url", "https://raw.githubusercontent.com/Bigby-wolf2333/Dog_Project/main/Datasets/description.csv"]

ENTRYPOINT [ "python", "ingest_data.py" ]