FROM vemlp-cn-beijing.cr.volces.com/preset-images/code-sandbox:server-20241204

# Install Python dependencies for RCA and Data Analysis into the sandbox-runtime environment
RUN /root/miniconda3/envs/sandbox-runtime/bin/pip install --no-cache-dir \
    duckdb \
    pandas \
    numpy \
    pyarrow \
    fastparquet \
    scikit-learn \
    matplotlib \
    seaborn \
    requests \
    sqlalchemy
