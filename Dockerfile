### 1. Get Linux
FROM alpine:3.10.9

### 2. Get Java via the package manager
RUN apk update \
&& apk upgrade \
&& apk add --no-cache bash \
&& apk add --no-cache --virtual=build-dependencies unzip \
&& apk add --no-cache curl \
&& apk add --no-cache openjdk11-jre

### 3. Get Python, PIP

RUN apk add make automake gcc g++ subversion libxml2-dev libxslt-dev python3-dev \
&& python3 -m ensurepip \
&& pip3 install --upgrade pip setuptools \
&& rm -r /usr/lib/python*/ensurepip && \
if [ ! -e /usr/bin/pip ]; then ln -s pip3 /usr/bin/pip ; fi && \
if [[ ! -e /usr/bin/python ]]; then ln -sf /usr/bin/python3 /usr/bin/python; fi && \
rm -r /root/.cache

WORKDIR /app
COPY . /app

# Fetch the ontopilot-master 
RUN apk add git
RUN git clone https://github.com/biocodellc/elk_pipeline.git

# Add generally larger-size jars from external repository to the lib directory
ADD https://github.com/biocodellc/query_fetcher/releases/download/0.0.1/query_fetcher-0.0.1.jar /app/lib/

RUN pip install --trusted-host pypi.python.org -r requirements.txt
CMD [ "python", "./pipeline.py" ]

