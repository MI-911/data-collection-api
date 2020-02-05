FROM python:3.7
# Adding requirements first allows Docker to cache the install of requirements
ADD requirements.txt /app/
WORKDIR /app
RUN pip install -r requirements.txt
RUN pip install gevent
ADD . /app/
EXPOSE 8000
CMD ["gunicorn", "--worker-class", "gevent", "--worker-connections", "1000", "-b", "0.0.0.0:8000", "mindreader"]