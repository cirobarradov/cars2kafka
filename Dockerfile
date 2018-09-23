FROM python:3.6

COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD ["python","cars2kafka.py","config/config.json"]

#docker run -v $(pwd)/config/:/app/config/ zity2kafka
