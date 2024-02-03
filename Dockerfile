FROM jupyter/pyspark-notebook
LABEL authors="santiago"
# Install pyspark and other dependencies
COPY requirements.txt /tmp/requirements.txt

# Install dependencies
RUN pip install -r /tmp/requirements.txt