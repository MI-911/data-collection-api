FROM python:3.7
ADD . /app
WORKDIR /app
RUN pip install -r requirements.txt
RUN pip install gevent
EXPOSE 8000
CMD ["gunicorn", "--worker-class", "gevent", "--worker-connections", "1000", "-b", "0.0.0.0:8000", "mindreader"]
