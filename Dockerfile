FROM jupyter/pyspark-notebook:latest

ADD NY.py .

CMD [ "spark-submit", "./NY.py" ]