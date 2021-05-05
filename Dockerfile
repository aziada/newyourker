FROM jupyter/pyspark-notebook

ADD NY.py .

RUN pip install socket requests json
CMD [ "spark-submit", "--num-executors 2" , "--driver-memory 4g" , " --executor-memory 6g",  "./NY.py" ]