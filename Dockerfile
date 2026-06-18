FROM python:3.12-slim
RUN apt-get update && apt-get install -qq git iputils-ping
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir --upgrade \
	asyncclick==8.3.0.7 \
	uvloop==0.22.1 \
	wakeonlan==3.3.0 \
	icmplib==3.0.4 \
	requests==2.34.2 \
	aiomqtt==2.5.1 \
	paho-mqtt==2.1.0 \
	pyyaml==6.0.3 \
	aiosnmp==0.7.2 \
	git+https://github.com/worosom/aiopjlink@e9383cee5510aaaa9f23f1299643b02c424c2448
RUN pip install --no-cache-dir PyWebOSTV==0.8.9 ws4py==0.6.0 wsaccel==0.6.7
RUN echo "{}" > /opt/weboscreds.json
WORKDIR /app
