FROM python:3.10.4

COPY requirments.txt /tmp/requirments.txt
RUN pip install -r /tmp/requirments.txt

RUN mkdir -p /cfg
COPY settings.cfg /cfg/settings.cfg

WORKDIR /opt
EXPOSE 4000
