### 1. Get Linux
FROM alpine:3.7

### 2. Get Java via the package manager
RUN apk update \
&& apk upgrade \
&& apk add --no-cache bash \
&& apk add --no-cache --virtual=build-dependencies unzip \
&& apk add --no-cache curl \
&& apk add --no-cache openjdk8-jre

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
#Add generally larger-size jars from external repository to the lib directory
ADD https://repo.biocodellc.com/repository/3rd-party/org/biocode/ontopilot/2017-08-04/ontopilot-2017-08-04.jar /app/lib/
ADD https://repo.biocodellc.com/repository/3rd-party/org/biocode/jaxb-api/2.2.3/jaxb-api-2.2.3.jar /app/lib/
ADD https://repo.biocodellc.com/repository/3rd-party/org/biocode/query_fetcher/0.0.1/query_fetcher-0.0.1.jar /app/lib/

RUN pip install --trusted-host pypi.python.org -r requirements.txt
CMD [ "python", "./pipeline.py" ]

