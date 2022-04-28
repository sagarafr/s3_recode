FROM python:3.10.4

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

RUN mkdir -p /cfg
COPY settings.cfg /cfg/settings.cfg

WORKDIR /opt
EXPOSE 4005
