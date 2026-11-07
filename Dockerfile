# ==============================================================================
# DOCKERFILE CHO MAGE.AI CONTAINER (TASK T01 - TTU)
# Cài đặt dbt-core, dbt-postgres, PyArrow (xử lý Parquet lớn) và GeoPandas (GIS)
# ==============================================================================
FROM mageai/mageai:latest

# Khóa phiên bản các thư viện chính xác để đảm bảo tính tái lập (Reproducibility)
RUN pip3 install --no-cache-dir \
    dbt-core==1.8.0 \
    dbt-postgres==1.8.0 \
    psycopg2-binary==2.9.9 \
    pyarrow==16.1.0 \
    geopandas==0.14.4 \
    shapely==2.0.4
