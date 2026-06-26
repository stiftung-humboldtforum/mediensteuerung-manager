# Unified on python:3.12-slim with the rest of the stack (api/calendar/knx/fac).
# SNMP runs on pysnmp (pure-Python, same lib as fac) and LG webOS TVs on
# aiowebostv (maintained, native asyncio) — these replace the former native /
# unmaintained aiosnmp + PyWebOSTV/ws4py/wsaccel stack that pinned this image
# to 3.11. No native build step is required anymore.
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
	pysnmp==7.1.27 \
	aiowebostv==0.7.5 \
	git+https://github.com/worosom/aiopjlink@e9383cee5510aaaa9f23f1299643b02c424c2448
RUN echo "{}" > /opt/weboscreds.json
WORKDIR /app
