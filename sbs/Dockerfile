FROM python:2.7.13
RUN pip install boto3
ADD credentials /root/.aws/credentials
ADD config /root/.aws/config
ADD sbs.py .
CMD python sbs.py