FROM python:3.9-slim
RUN apt-get update -y && \
    apt-get install -y python3-pip
COPY . /
RUN python3 -m unittest tests/test_data_processor.py

#CMD ["python3","-m","unittest","tests/test_data_processor.py"]
CMD ["python3","entrypoint.py"]