FROM amazon/aws-cli:2.5.6

RUN mkdir -p /root/.aws
RUN yum install -y python-pip iputils vim
RUN pip install awscli-plugin-endpoint
COPY conf/config /root/.aws/config
COPY conf/credentials /root/.aws/credentials
