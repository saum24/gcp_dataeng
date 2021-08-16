FROM python:3.7.3-stretch
LABEL maintainer="SAUMYA PALLAV<saumyapallav@gmail.com>"
WORKDIR /usr/src/app

COPY PublishToPubsub.py .
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "./PublishToPubsub.py"]
