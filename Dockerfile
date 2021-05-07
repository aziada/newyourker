FROM jupyter/pyspark-notebook:latest

ADD NY.py .

VOLUME ["/data/"]

COPY yelp_biz.json yelp_tip.json yelp_review.json yelp_checkin.json yelp_user.json /data/

CMD [ "spark-submit", "./NY.py" ]




