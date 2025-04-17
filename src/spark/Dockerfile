# src/spark/Dockerfile
FROM jupyter/pyspark-notebook

USER root
RUN pip install --no-cache-dir \
    pandas \
    scikit-learn \
    matplotlib \
    seaborn
USER $NB_UID