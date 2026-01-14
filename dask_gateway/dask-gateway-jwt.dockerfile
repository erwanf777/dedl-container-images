FROM ghcr.io/dask/dask-gateway-server:2025.4.0
ARG DASK_EXT_DIR=/home/dask/dask-server-extensions
RUN pip install --no-cache-dir pyjwt && \
    mkdir $DASK_EXT_DIR
COPY dask_gateway_setup.py $DASK_EXT_DIR/dask_jwt_authenticator.py
COPY jwks.json jwks.json
ENV PYTHONPATH="$DASK_EXT_DIR"

