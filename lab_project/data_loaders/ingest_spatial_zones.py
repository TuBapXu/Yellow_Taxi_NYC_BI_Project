import os
import hashlib
from pathlib import Path
import pandas as pd
import geopandas as gpd
import psycopg2
from psycopg2.extras import execute_values

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        dbname=os.getenv('POSTGRES_DB', 'DW_DB'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'admin123')
    )


def compute_sha256(file_path: Path) -> str:
    h = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


@data_loader
def load_and_ingest_zones(*args, **kwargs):
    """
    Task S01 (stinh): Nạp Zone Lookup CSV (265 mã) và Shapefile (263 đa giác) vào tầng staging.
    Hỗ trợ tự động tìm file trong /home/src/external_data.
    """
    data_dir = Path(kwargs.get('data_dir', '/home/src/external_data'))

    # 1. Tìm file CSV Lookup
    csv_candidates = list(data_dir.glob('*zone*lookup*.csv'))
    if not csv_candidates:
        raise FileNotFoundError(f"Không tìm thấy file taxi_zone_lookup.csv trong {data_dir}")
    csv_path = csv_candidates[0]
    csv_sha256 = compute_sha256(csv_path)

    df_lookup = pd.read_csv(csv_path)
    # Chuẩn hóa tên cột
    df_lookup.columns = [c.strip() for c in df_lookup.columns]
    df_lookup = df_lookup.rename(columns={
        'LocationID': 'location_id',
        'Borough': 'borough',
        'Zone': 'zone_name',
        'service_zone': 'service_zone'
    })

    # 2. Tìm file Shapefile (.shp hoặc .zip)
    shp_candidates = list(data_dir.glob('**/taxi_zones.shp')) + list(data_dir.glob('*taxi_zones*.zip'))
    if not shp_candidates:
        raise FileNotFoundError(f"Không tìm thấy taxi_zones.shp hoặc taxi_zones.zip trong {data_dir}")
    shp_path = shp_candidates[0]
    shp_sha256 = compute_sha256(shp_path)

    gdf = gpd.read_file(shp_path)
    if gdf.crs is None:
        # Hệ tọa độ chuẩn của NYC TLC Shapefile là EPSG:2263
        gdf = gdf.set_crs(epsg=2263)

    source_srid = gdf.crs.to_epsg() or 2263
    # Chuyển đổi sang chuẩn WGS84 (EPSG:4326) và ép về MultiPolygon đồng nhất
    gdf_4326 = gdf.to_crs(epsg=4326)

    # Gộp các mảnh cùng LocationID (nếu 1 LocationID bị tách làm nhiều dòng trong shapefile, vd: 56, 103)
    gdf_dissolved = gdf_4326.dissolve(by='LocationID', as_index=False)

    # 3. Lưu vào PostgreSQL schema `staging`
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            # Bảng 1: staging.raw_taxi_zone_lookup
            cur.execute("""
                CREATE TABLE IF NOT EXISTS staging.raw_taxi_zone_lookup (
                    location_id INT PRIMARY KEY,
                    borough VARCHAR(50),
                    zone_name VARCHAR(100),
                    service_zone VARCHAR(50),
                    source_file_name VARCHAR(255),
                    source_file_sha256 CHAR(64),
                    loaded_at_utc TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC')
                );
                TRUNCATE TABLE staging.raw_taxi_zone_lookup;
            """)

            lookup_records = [
                (
                    int(row['location_id']),
                    None if pd.isna(row['borough']) else str(row['borough']),
                    None if pd.isna(row['zone_name']) else str(row['zone_name']),
                    None if pd.isna(row['service_zone']) else str(row['service_zone']),
                    csv_path.name,
                    csv_sha256
                )
                for _, row in df_lookup.iterrows()
            ]
            execute_values(cur, """
                INSERT INTO staging.raw_taxi_zone_lookup
                (location_id, borough, zone_name, service_zone, source_file_name, source_file_sha256)
                VALUES %s
            """, lookup_records)

            # Bảng 2: staging.raw_taxi_zones_shp (Lưu geometry chuẩn PostGIS MultiPolygon 4326)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS staging.raw_taxi_zones_shp (
                    location_id INT PRIMARY KEY,
                    borough VARCHAR(50),
                    zone_name VARCHAR(100),
                    source_srid INT,
                    geom_4326 geometry(MultiPolygon, 4326),
                    is_valid_geom BOOLEAN,
                    source_file_name VARCHAR(255),
                    source_file_sha256 CHAR(64),
                    loaded_at_utc TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC')
                );
                TRUNCATE TABLE staging.raw_taxi_zones_shp;
            """)

            for _, row in gdf_dissolved.iterrows():
                loc_id = int(row['LocationID'])
                borough = None if pd.isna(row.get('borough')) else str(row.get('borough'))
                zone_str = None if pd.isna(row.get('zone')) else str(row.get('zone'))
                wkt_geom = row['geometry'].wkt if row['geometry'] is not None else None

                cur.execute("""
                    INSERT INTO staging.raw_taxi_zones_shp
                    (location_id, borough, zone_name, source_srid, geom_4326, is_valid_geom, source_file_name, source_file_sha256)
                    VALUES (
                        %s, %s, %s, %s,
                        ST_Multi(ST_MakeValid(ST_GeomFromText(%s, 4326))),
                        ST_IsValid(ST_GeomFromText(%s, 4326)),
                        %s, %s
                    )
                """, (loc_id, borough, zone_str, source_srid, wkt_geom, wkt_geom, shp_path.name, shp_sha256))

        conn.commit()
    finally:
        conn.close()

    summary_df = pd.DataFrame([{
        'lookup_csv_file': csv_path.name,
        'lookup_rows': len(df_lookup),
        'shapefile_name': shp_path.name,
        'raw_shp_features': len(gdf),
        'unique_location_polygons': len(gdf_dissolved),
        'source_srid': source_srid
    }])
    print("==> KẾT QUẢ NẠP ZONES & SHAPEFILE:")
    print(summary_df.to_string(index=False))
    return summary_df


@test
def test_zone_counts(output, *args) -> None:
    assert output is not None, 'Output is undefined'
    assert int(output['lookup_rows'].iloc[0]) == 265, f"Kỳ vọng 265 dòng CSV lookup, thực tế: {output['lookup_rows'].iloc[0]}"
    assert int(output['unique_location_polygons'].iloc[0]) >= 260, "Số lượng đa giác khu vực hợp lệ quá ít!"