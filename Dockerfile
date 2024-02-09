FROM jupyter/pyspark-notebook
LABEL authors="santiago"

ENV HOST=192.168.1.8\
    PORT=5432 \
    DATABASE=voting \
    USER=postgres \
    PASSWORD=postgres

# Install pyspark and other dependencies
COPY requirements.txt /tmp/requirements.txt
COPY packages/postgresql-42.7.1.jar /opt/packages/postgresql-42.7.1.jar

# Install dependencies
RUN pip install -r /tmp/requirements.txt