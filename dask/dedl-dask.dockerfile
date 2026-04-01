FROM mambaorg/micromamba:2.4.0-debian13
COPY --chown=$MAMBA_USER:$MAMBA_USER ../envs/dedl-core.yaml /tmp/dedl-core.yaml
RUN micromamba install --yes -n base --use-uv -f /tmp/dedl-core.yaml && \
    micromamba clean --all --yes

# Install DEFAIR wheels (pre-built and placed in dask/wheels/)
COPY --chown=$MAMBA_USER:$MAMBA_USER /wheels/*.whl /tmp/wheels/
RUN micromamba run -n base pip install --no-deps /tmp/wheels/*.whl && \
    rm -rf /tmp/wheels
