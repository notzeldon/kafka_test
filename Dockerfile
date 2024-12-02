FROM python:3.11

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY main.py .
CMD python3 main.py consume --topic hello_topic --kafka kafka:9021
