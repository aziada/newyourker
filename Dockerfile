FROM jupyter/pyspark-notebook:latest

ADD NY.py .
volumes:
          - ./data/yelp_biz.json
          - ./data/yelp_tip.json
          - ./data/yelp_review.json
          - ./data/yelp_checkin.json
CMD [ "spark-submit", "./NY.py" ]