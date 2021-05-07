FROM jupyter/pyspark-notebook:latest

ADD NY.py .

VOLUME ["/data/yelp_biz.json"]
VOLUME ["/data/yelp_tip.json"]
VOLUME ["/data/yelp_review.json"]
VOLUME ["/data/yelp_checkin.json"]

CMD [ "spark-submit", "./NY.py" ]