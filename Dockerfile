# Unified on python:3.12-slim with the rest of the stack (api/calendar/knx/fac).
# SNMP runs on pysnmp (pure-Python, same lib as fac) and LG webOS TVs on
# aiowebostv (maintained, native asyncio) — these replaced the former native /
# unmaintained aiosnmp + PyWebOSTV/ws4py/wsaccel stack. No native build step.
FROM python:3.12-slim
RUN apt-get update && apt-get install -qq git iputils-ping
RUN pip install --no-cache-dir --upgrade pip

# Fully-pinned + hashed deps (generated from requirements.in via `uv pip compile
# --universal --generate-hashes`); --require-hashes makes the build fail on any
# drift or tampering.
COPY requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt
# aiopjlink has no PyPI release; it is pinned by an immutable commit SHA, so
# install it separately with --no-deps (it has no dependencies) to keep the
# hash-checked layer above intact.
RUN pip install --no-cache-dir --no-deps git+https://github.com/worosom/aiopjlink@e9383cee5510aaaa9f23f1299643b02c424c2448
RUN echo "{}" > /opt/weboscreds.json
WORKDIR /app
