# Collect & Transform Data


# Import Modules

``` python
from pathlib import Path
import json
import polars as pl
# The command below builds the polars_geo plugin in Rust locally
# This only needs to be run once
#! cd polars_geo && uv run maturin develop --release
import polars_geo
```

# Initialise Parameters

``` python
data_dir = Path("data")
trips_url = "https://s3.amazonaws.com/tripdata/202403-citibike-tripdata.csv.zip"
trips_filename = "202403-citibike-tripdata.csv.zip"
geojson_url = "https://raw.githubusercontent.com/HodgesWardElliott/custom-nyc-neighborhoods/refs/heads/master/custom-pedia-cities-nyc-Mar2018.geojson"
geojson_filename = "nyc-neighbourhoods.geojson"
geojson_path = data_dir / geojson_filename
csv_trips_filename = trips_filename.replace('.csv.zip', '.csv')
csv_path = data_dir / csv_trips_filename
zip_path = data_dir / trips_filename
```

# Trips

## Download Trips Data

``` python
# Download and unzip trips data if CSV does not exist
if not csv_path.exists():
    # Download the zip file if not already present
    if not zip_path.exists():
        !curl -L -o {zip_path} {trips_url}
    # Unzip the file
    !unzip -d {data_dir} {zip_path}
    # Remove the zip file
    zip_path.unlink(missing_ok=True)
```

## View Trips Data

``` python
# Get the CSV filename (remove .zip extension)
csv_trips_filename = trips_filename.replace('.csv.zip', '.csv')
!wc -l {data_dir}/{csv_trips_filename}
!head -n 6 {data_dir}/{csv_trips_filename}
```

    2663296 data/202403-citibike-tripdata.csv
    "ride_id","rideable_type","started_at","ended_at","start_station_name","start_station_id","end_station_name","end_station_id","start_lat","start_lng","end_lat","end_lng","member_casual"
    "62021B31AF42943E","electric_bike","2024-03-13 15:57:41.800","2024-03-13 16:07:09.853","Forsyth St & Grand St","5382.07","Front St & Jay St","4895.03",40.717763305,-73.993166089,40.702461,-73.986842,"member"
    "EC7BE9D296FFD072","electric_bike","2024-03-16 10:25:46.114","2024-03-16 10:30:21.554","E 12 St & 3 Ave","5788.12","Mott St & Prince St","5561.04",40.73245585,-73.988553643,40.72317958,-73.99480012,"member"
    "EC85C0EEC95157BB","classic_bike","2024-03-20 19:20:49.818","2024-03-20 19:28:00.165","E 12 St & 3 Ave","5788.12","Mott St & Prince St","5561.04",40.73223272,-73.98889957,40.72317958,-73.99480012,"member"
    "9DDE9AF5606B4E0F","classic_bike","2024-03-13 20:31:12.599","2024-03-13 20:40:31.209","6 Ave & W 34 St","6364.10","E 25 St & 1 Ave","6004.07",40.74964,-73.98805,40.7381765,-73.97738662,"member"
    "E4446F457328C5FE","electric_bike","2024-03-16 10:50:11.535","2024-03-16 10:53:02.451","Cleveland Pl & Spring St","5492.05","Mott St & Prince St","5561.04",40.721995115,-73.997344375,40.72317958,-73.99480012,"member"

## Load Trips Data as Polars DataFrame

``` python
trips = pl.read_csv(
    f"{data_dir}/{csv_trips_filename}", 
    try_parse_dates=True,
    schema_overrides={
        "start_station_id": pl.String,
        "end_station_id": pl.String
    },
).sort(
    "started_at"
)
```

## Inspect Trips Data

``` python
print(type(trips))
print(trips.describe())
```

    <class 'polars.dataframe.frame.DataFrame'>
    shape: (9, 14)
    ┌───────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬──────────┐
    │ statistic ┆ ride_id   ┆ rideable_ ┆ started_a ┆ … ┆ start_lng ┆ end_lat   ┆ end_lng   ┆ member_c │
    │ ---       ┆ ---       ┆ type      ┆ t         ┆   ┆ ---       ┆ ---       ┆ ---       ┆ asual    │
    │ str       ┆ str       ┆ ---       ┆ ---       ┆   ┆ f64       ┆ f64       ┆ f64       ┆ ---      │
    │           ┆           ┆ str       ┆ str       ┆   ┆           ┆           ┆           ┆ str      │
    ╞═══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪══════════╡
    │ count     ┆ 2663295   ┆ 2663295   ┆ 2663295   ┆ … ┆ 2.663295e ┆ 2.662525e ┆ 2.662525e ┆ 2663295  │
    │           ┆           ┆           ┆           ┆   ┆ 6         ┆ 6         ┆ 6         ┆          │
    │ null_coun ┆ 0         ┆ 0         ┆ 0         ┆ … ┆ 0.0       ┆ 770.0     ┆ 770.0     ┆ 0        │
    │ t         ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
    │ mean      ┆ null      ┆ null      ┆ 2024-03-1 ┆ … ┆ -73.97160 ┆ 40.739102 ┆ -73.97172 ┆ null     │
    │           ┆           ┆           ┆ 6 20:47:1 ┆   ┆ 9         ┆           ┆ 6         ┆          │
    │           ┆           ┆           ┆ 8.462635  ┆   ┆           ┆           ┆           ┆          │
    │ std       ┆ null      ┆ null      ┆ null      ┆ … ┆ 0.028498  ┆ 0.040858  ┆ 0.028511  ┆ null     │
    │ min       ┆ 000000804 ┆ classic_b ┆ 2024-02-2 ┆ … ┆ -74.03    ┆ 40.57     ┆ -74.08670 ┆ casual   │
    │           ┆ FF55270   ┆ ike       ┆ 9 00:20:2 ┆   ┆           ┆           ┆ 1         ┆          │
    │           ┆           ┆           ┆ 7.570000  ┆   ┆           ┆           ┆           ┆          │
    │ 25%       ┆ null      ┆ null      ┆ 2024-03-1 ┆ … ┆ -73.99175 ┆ 40.714275 ┆ -73.99177 ┆ null     │
    │           ┆           ┆           ┆ 0 03:57:0 ┆   ┆ 3         ┆           ┆           ┆          │
    │           ┆           ┆           ┆ 3.443000  ┆   ┆           ┆           ┆           ┆          │
    │ 50%       ┆ null      ┆ null      ┆ 2024-03-1 ┆ … ┆ -73.97940 ┆ 40.738661 ┆ -73.97950 ┆ null     │
    │           ┆           ┆           ┆ 6 12:39:2 ┆   ┆ 5         ┆           ┆ 4         ┆          │
    │           ┆           ┆           ┆ 2.172000  ┆   ┆           ┆           ┆           ┆          │
    │ 75%       ┆ null      ┆ null      ┆ 2024-03-2 ┆ … ┆ -73.95532 ┆ 40.762288 ┆ -73.95561 ┆ null     │
    │           ┆           ┆           ┆ 4 13:58:2 ┆   ┆ 7         ┆           ┆ 3         ┆          │
    │           ┆           ┆           ┆ 2.583000  ┆   ┆           ┆           ┆           ┆          │
    │ max       ┆ FFFFFBD89 ┆ electric_ ┆ 2024-03-3 ┆ … ┆ -73.83406 ┆ 41.18     ┆ -73.16    ┆ member   │
    │           ┆ 8A57A06   ┆ bike      ┆ 1 23:57:1 ┆   ┆ 3         ┆           ┆           ┆          │
    │           ┆           ┆           ┆ 6.025000  ┆   ┆           ┆           ┆           ┆          │
    └───────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴──────────┘

``` python
print(trips[:, :4])
print(trips[:, 4:8])
print(trips[:, 8:])
```

    shape: (2_663_295, 4)
    ┌──────────────────┬───────────────┬─────────────────────────┬─────────────────────────┐
    │ ride_id          ┆ rideable_type ┆ started_at              ┆ ended_at                │
    │ ---              ┆ ---           ┆ ---                     ┆ ---                     │
    │ str              ┆ str           ┆ datetime[μs]            ┆ datetime[μs]            │
    ╞══════════════════╪═══════════════╪═════════════════════════╪═════════════════════════╡
    │ 9EC2AD5F3F8C8B57 ┆ classic_bike  ┆ 2024-02-29 00:20:27.570 ┆ 2024-03-01 01:20:22.196 │
    │ C76D82D96516BDC2 ┆ classic_bike  ┆ 2024-02-29 07:54:34.223 ┆ 2024-03-01 08:54:12.611 │
    │ B4C73C958C65FEA6 ┆ electric_bike ┆ 2024-02-29 08:47:09.664 ┆ 2024-03-01 09:47:02.393 │
    │ E23F7822B3D53E2A ┆ classic_bike  ┆ 2024-02-29 09:57:07.150 ┆ 2024-03-01 10:57:00.848 │
    │ B0B6437C50C3AB3E ┆ electric_bike ┆ 2024-02-29 10:29:41.981 ┆ 2024-03-01 11:29:21.539 │
    │ …                ┆ …             ┆ …                       ┆ …                       │
    │ 197C0ABDD3348135 ┆ classic_bike  ┆ 2024-03-31 23:55:37.938 ┆ 2024-03-31 23:59:08.301 │
    │ 702FEBD6D9CCE4BC ┆ classic_bike  ┆ 2024-03-31 23:55:40.087 ┆ 2024-03-31 23:57:26.335 │
    │ ECA4FC65950ADDDB ┆ electric_bike ┆ 2024-03-31 23:55:41.173 ┆ 2024-03-31 23:57:25.079 │
    │ D8B20517A4AB7D60 ┆ classic_bike  ┆ 2024-03-31 23:56:17.935 ┆ 2024-03-31 23:57:18.475 │
    │ 6BC5FAFEAC948FB1 ┆ electric_bike ┆ 2024-03-31 23:57:16.025 ┆ 2024-03-31 23:59:22.134 │
    └──────────────────┴───────────────┴─────────────────────────┴─────────────────────────┘
    shape: (2_663_295, 4)
    ┌─────────────────────────────┬──────────────────┬────────────────────────────┬────────────────┐
    │ start_station_name          ┆ start_station_id ┆ end_station_name           ┆ end_station_id │
    │ ---                         ┆ ---              ┆ ---                        ┆ ---            │
    │ str                         ┆ str              ┆ str                        ┆ str            │
    ╞═════════════════════════════╪══════════════════╪════════════════════════════╪════════════════╡
    │ 61 St & 39 Ave              ┆ 6307.07          ┆ null                       ┆ null           │
    │ E 54 St & 1 Ave             ┆ 6608.09          ┆ null                       ┆ null           │
    │ FDR Drive & E 35 St         ┆ 6230.04          ┆ null                       ┆ null           │
    │ E 6 St & Ave B              ┆ 5584.04          ┆ null                       ┆ null           │
    │ Eastern Pkwy & Brooklyn Ave ┆ 3871.02          ┆ null                       ┆ null           │
    │ …                           ┆ …                ┆ …                          ┆ …              │
    │ E 59 St & Madison Ave       ┆ 6801.01          ┆ 3 Ave & E 62 St            ┆ 6762.04        │
    │ Amsterdam Ave & W 119 St    ┆ 7727.07          ┆ Morningside Ave & W 123 St ┆ 7741.01        │
    │ S 4 St & Wythe Ave          ┆ 5204.05          ┆ S 3 St & Bedford Ave       ┆ 5235.05        │
    │ Division St & Bowery        ┆ 5270.08          ┆ Division St & Bowery       ┆ 5270.08        │
    │ Montrose Ave & Bushwick Ave ┆ 5068.02          ┆ Humboldt St & Varet St     ┆ 4956.02        │
    └─────────────────────────────┴──────────────────┴────────────────────────────┴────────────────┘
    shape: (2_663_295, 5)
    ┌───────────┬────────────┬───────────┬────────────┬───────────────┐
    │ start_lat ┆ start_lng  ┆ end_lat   ┆ end_lng    ┆ member_casual │
    │ ---       ┆ ---        ┆ ---       ┆ ---        ┆ ---           │
    │ f64       ┆ f64        ┆ f64       ┆ f64        ┆ str           │
    ╞═══════════╪════════════╪═══════════╪════════════╪═══════════════╡
    │ 40.7471   ┆ -73.9028   ┆ null      ┆ null       ┆ member        │
    │ 40.756265 ┆ -73.964179 ┆ null      ┆ null       ┆ member        │
    │ 40.744219 ┆ -73.971212 ┆ null      ┆ null       ┆ member        │
    │ 40.724537 ┆ -73.981854 ┆ null      ┆ null       ┆ member        │
    │ 40.66939  ┆ -73.94514  ┆ null      ┆ null       ┆ member        │
    │ …         ┆ …          ┆ …         ┆ …          ┆ …             │
    │ 40.763505 ┆ -73.971092 ┆ 40.763126 ┆ -73.965269 ┆ member        │
    │ 40.808625 ┆ -73.959621 ┆ 40.81     ┆ -73.955151 ┆ member        │
    │ 40.712996 ┆ -73.965971 ┆ 40.712605 ┆ -73.962644 ┆ member        │
    │ 40.714193 ┆ -73.996732 ┆ 40.714193 ┆ -73.996732 ┆ member        │
    │ 40.707678 ┆ -73.940297 ┆ 40.703172 ┆ -73.940636 ┆ member        │
    └───────────┴────────────┴───────────┴────────────┴───────────────┘

## Save Trips DataFrame as Parquet File

``` python
# Save with same base filename but .parquet extension
parquet_trips_filename = trips_filename.replace('.zip', '.parquet')
trips.write_parquet(f'{data_dir}/{parquet_trips_filename}')
```

# Neighbourhoods

## Download GeoJSON File

``` python
# Download geojson if not already present
if not geojson_path.exists():
    !curl -L -o {geojson_path} {geojson_url}

#!python -m json.tool {data_dir}/{geojson_filename}
```

## Display Sample of GeoJSON

``` python
with open(f"{data_dir}/{geojson_filename}") as f:
    geojson = json.load(f)

# Pretty-print the first feature
if "features" in geojson and len(geojson["features"]) > 0:
    print(json.dumps(geojson["features"][0], indent=2))
```

    {
      "type": "Feature",
      "properties": {
        "neighborhood": "Allerton",
        "boroughCode": "2",
        "borough": "Bronx",
        "X.id": "http://nyc.pediacities.com/Resource/Neighborhood/Allerton"
      },
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              -73.84859700000018,
              40.871670000000115
            ],
            [
              -73.84582253683678,
              40.870239076236174
            ],
            [
              -73.85455918463374,
              40.85995383576425
            ],
            [
              -73.85466543306826,
              40.859585694988056
            ],
            [
              -73.85638870335896,
              40.85759363530448
            ],
            [
              -73.86888180915341,
              40.857223150158326
            ],
            [
              -73.86831755272824,
              40.85786206225831
            ],
            [
              -73.86955371467232,
              40.85778409560018
            ],
            [
              -73.87102485762065,
              40.857309948816905
            ],
            [
              -73.87048054998716,
              40.865413584098484
            ],
            [
              -73.87055489856489,
              40.86970279858986
            ],
            [
              -73.86721594442561,
              40.86968966363671
            ],
            [
              -73.85745,
              40.86953300000018
            ],
            [
              -73.85555000000011,
              40.871813000000145
            ],
            [
              -73.85359796757658,
              40.8732883686742
            ],
            [
              -73.84859700000018,
              40.871670000000115
            ]
          ]
        ]
      }
    }

## Convert GeoJSON to Polars DataFrame

``` python
neighbourhoods = (
    pl.read_json(f"{data_dir}/{geojson_filename}")
    .select("features")
    .explode("features")
    .unnest("features")
    .unnest("properties")
    .select("neighborhood", "borough", "geometry")
    .unnest("geometry")
    .with_columns(polygon=pl.col("coordinates").list.first())
    .select("neighborhood", "borough", "polygon")
    .filter(pl.col("borough") != "Staten Island")
    .sort("neighborhood")
)
```

## Inspect Neighbourhoods DataFrame

``` python
neighbourhoods.describe()
```

<div>

<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (9, 4)</small>

| statistic    | neighborhood | borough  | polygon |
|--------------|--------------|----------|---------|
| str          | str          | str      | f64     |
| "count"      | "258"        | "258"    | 258.0   |
| "null_count" | "0"          | "0"      | 0.0     |
| "mean"       | null         | null     | null    |
| "std"        | null         | null     | null    |
| "min"        | "Allerton"   | "Bronx"  | null    |
| "25%"        | null         | null     | null    |
| "50%"        | null         | null     | null    |
| "75%"        | null         | null     | null    |
| "max"        | "Woodside"   | "Queens" | null    |

</div>

</div>

## Save Neighbourhoods as Parquet File

``` python
# Create the Parquet filename by replacing .geojson with .parquet
neighbourhoods_parquet_filename = f"{data_dir}/{geojson_filename.replace('.geojson', '.parquet')}"

# Save the DataFrame as a Parquet file
neighbourhoods.write_parquet(neighbourhoods_parquet_filename)
```

# Display File Sizes

CSV (547M) vs Parquet (93M)

GeoJSON (1.5M) vs Parquet (357K)

``` python
!ls -lh {data_dir}/*.csv {data_dir}/*.geojson {data_dir}/*.parquet
```

    -rw-r--r-- 1 solaris solaris 547M Jul 31  2024 data/202403-citibike-tripdata.csv
    -rw-rw-r-- 1 solaris solaris  93M Jun 20 18:01 data/202403-citibike-tripdata.csv.parquet
    -rw-rw-r-- 1 solaris solaris 1.5M Jun 18 15:42 data/nyc-neighbourhoods.geojson
    -rw-rw-r-- 1 solaris solaris 357K Jun 20 18:01 data/nyc-neighbourhoods.parquet
    -rw-rw-r-- 1 solaris solaris  21K Jun 20 18:00 data/stations_borough_neighborhood.parquet
    -rw-rw-r-- 1 solaris solaris  44K Jun 20 18:01 data/stations.parquet
    -rw-rw-r-- 1 solaris solaris  94M Jun 20 18:00 data/tips_stations_borough_neighbourhood.parquet
    -rw-rw-r-- 1 solaris solaris  94M Jun 20 17:51 data/trips_stations_borough_neighbourhood.parquet
    -rw-rw-r-- 1 solaris solaris  89M Jun 20 18:00 data/trips_transformed.parquet

# Transform Trips

``` python
trips = trips.select(
  bike_type = pl.col("rideable_type")
  .str.split("_")
  .list.get(0)
  .cast(pl.Categorical),
  rider_type = pl.col("member_casual").cast(pl.Categorical),
  datetime_start = pl.col("started_at"),
  datetime_end = pl.col("ended_at"),
  station_start = pl.col("start_station_name"),
  station_end = pl.col("end_station_name"),
  lon_start = pl.col("start_lng"),
  lat_start = pl.col("start_lat"),
  lon_end = pl.col("end_lng"),
  lat_end = pl.col("end_lat")
).with_columns(
  duration = (pl.col("datetime_end") - pl.col("datetime_start"))
)
print(trips)
trips.write_parquet(data_dir / "trips_transformed.parquet")
```

    shape: (2_663_295, 11)
    ┌───────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬──────────┐
    │ bike_type ┆ rider_typ ┆ datetime_ ┆ datetime_ ┆ … ┆ lat_start ┆ lon_end   ┆ lat_end   ┆ duration │
    │ ---       ┆ e         ┆ start     ┆ end       ┆   ┆ ---       ┆ ---       ┆ ---       ┆ ---      │
    │ cat       ┆ ---       ┆ ---       ┆ ---       ┆   ┆ f64       ┆ f64       ┆ f64       ┆ duration │
    │           ┆ cat       ┆ datetime[ ┆ datetime[ ┆   ┆           ┆           ┆           ┆ [μs]     │
    │           ┆           ┆ μs]       ┆ μs]       ┆   ┆           ┆           ┆           ┆          │
    ╞═══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪══════════╡
    │ classic   ┆ member    ┆ 2024-02-2 ┆ 2024-03-0 ┆ … ┆ 40.7471   ┆ null      ┆ null      ┆ 1d 59m   │
    │           ┆           ┆ 9 00:20:2 ┆ 1 01:20:2 ┆   ┆           ┆           ┆           ┆ 54s      │
    │           ┆           ┆ 7.570     ┆ 2.196     ┆   ┆           ┆           ┆           ┆ 626ms    │
    │ classic   ┆ member    ┆ 2024-02-2 ┆ 2024-03-0 ┆ … ┆ 40.756265 ┆ null      ┆ null      ┆ 1d 59m   │
    │           ┆           ┆ 9 07:54:3 ┆ 1 08:54:1 ┆   ┆           ┆           ┆           ┆ 38s      │
    │           ┆           ┆ 4.223     ┆ 2.611     ┆   ┆           ┆           ┆           ┆ 388ms    │
    │ electric  ┆ member    ┆ 2024-02-2 ┆ 2024-03-0 ┆ … ┆ 40.744219 ┆ null      ┆ null      ┆ 1d 59m   │
    │           ┆           ┆ 9 08:47:0 ┆ 1 09:47:0 ┆   ┆           ┆           ┆           ┆ 52s      │
    │           ┆           ┆ 9.664     ┆ 2.393     ┆   ┆           ┆           ┆           ┆ 729ms    │
    │ classic   ┆ member    ┆ 2024-02-2 ┆ 2024-03-0 ┆ … ┆ 40.724537 ┆ null      ┆ null      ┆ 1d 59m   │
    │           ┆           ┆ 9 09:57:0 ┆ 1 10:57:0 ┆   ┆           ┆           ┆           ┆ 53s      │
    │           ┆           ┆ 7.150     ┆ 0.848     ┆   ┆           ┆           ┆           ┆ 698ms    │
    │ electric  ┆ member    ┆ 2024-02-2 ┆ 2024-03-0 ┆ … ┆ 40.66939  ┆ null      ┆ null      ┆ 1d 59m   │
    │           ┆           ┆ 9 10:29:4 ┆ 1 11:29:2 ┆   ┆           ┆           ┆           ┆ 39s      │
    │           ┆           ┆ 1.981     ┆ 1.539     ┆   ┆           ┆           ┆           ┆ 558ms    │
    │ …         ┆ …         ┆ …         ┆ …         ┆ … ┆ …         ┆ …         ┆ …         ┆ …        │
    │ classic   ┆ member    ┆ 2024-03-3 ┆ 2024-03-3 ┆ … ┆ 40.763505 ┆ -73.96526 ┆ 40.763126 ┆ 3m 30s   │
    │           ┆           ┆ 1 23:55:3 ┆ 1 23:59:0 ┆   ┆           ┆ 9         ┆           ┆ 363ms    │
    │           ┆           ┆ 7.938     ┆ 8.301     ┆   ┆           ┆           ┆           ┆          │
    │ classic   ┆ member    ┆ 2024-03-3 ┆ 2024-03-3 ┆ … ┆ 40.808625 ┆ -73.95515 ┆ 40.81     ┆ 1m 46s   │
    │           ┆           ┆ 1 23:55:4 ┆ 1 23:57:2 ┆   ┆           ┆ 1         ┆           ┆ 248ms    │
    │           ┆           ┆ 0.087     ┆ 6.335     ┆   ┆           ┆           ┆           ┆          │
    │ electric  ┆ member    ┆ 2024-03-3 ┆ 2024-03-3 ┆ … ┆ 40.712996 ┆ -73.96264 ┆ 40.712605 ┆ 1m 43s   │
    │           ┆           ┆ 1 23:55:4 ┆ 1 23:57:2 ┆   ┆           ┆ 4         ┆           ┆ 906ms    │
    │           ┆           ┆ 1.173     ┆ 5.079     ┆   ┆           ┆           ┆           ┆          │
    │ classic   ┆ member    ┆ 2024-03-3 ┆ 2024-03-3 ┆ … ┆ 40.714193 ┆ -73.99673 ┆ 40.714193 ┆ 1m 540ms │
    │           ┆           ┆ 1 23:56:1 ┆ 1 23:57:1 ┆   ┆           ┆ 2         ┆           ┆          │
    │           ┆           ┆ 7.935     ┆ 8.475     ┆   ┆           ┆           ┆           ┆          │
    │ electric  ┆ member    ┆ 2024-03-3 ┆ 2024-03-3 ┆ … ┆ 40.707678 ┆ -73.94063 ┆ 40.703172 ┆ 2m 6s    │
    │           ┆           ┆ 1 23:57:1 ┆ 1 23:59:2 ┆   ┆           ┆ 6         ┆           ┆ 109ms    │
    │           ┆           ┆ 6.025     ┆ 2.134     ┆   ┆           ┆           ┆           ┆          │
    └───────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴──────────┘

# Clean Trips

``` python
trips = (
  trips.drop_nulls()
  .filter(
    (pl.col("datetime_start") >= pl.date(2024, 3, 1))
    & (pl.col("datetime_end") < pl.date(2024, 4, 1)) 
  )
  .filter(
    ~(
      (pl.col("station_start") == pl.col("station_end"))
      & (pl.col("duration").dt.total_seconds() < 5 * 60)
    )
  )
)
print(f"Total number of trips: {trips.height}")
trips.write_parquet(data_dir / "trips_transformed.parquet")
```

    Total number of trips: 2639170

# Add Trip Distance

``` python
trips = trips.with_columns(
  distance = pl.concat_list("lon_start", "lat_start").geo.haversine_distance(
    pl.concat_list("lon_end", "lat_end")
  )
  / 1000
)
print(trips)
trips.write_parquet(data_dir / "trips_transformed.parquet")
```

    shape: (2_639_170, 12)
    ┌───────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬──────────┐
    │ bike_type ┆ rider_typ ┆ datetime_ ┆ datetime_ ┆ … ┆ lon_end   ┆ lat_end   ┆ duration  ┆ distance │
    │ ---       ┆ e         ┆ start     ┆ end       ┆   ┆ ---       ┆ ---       ┆ ---       ┆ ---      │
    │ cat       ┆ ---       ┆ ---       ┆ ---       ┆   ┆ f64       ┆ f64       ┆ duration[ ┆ f64      │
    │           ┆ cat       ┆ datetime[ ┆ datetime[ ┆   ┆           ┆           ┆ μs]       ┆          │
    │           ┆           ┆ μs]       ┆ μs]       ┆   ┆           ┆           ┆           ┆          │
    ╞═══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪══════════╡
    │ electric  ┆ member    ┆ 2024-03-0 ┆ 2024-03-0 ┆ … ┆ -74.00731 ┆ 40.707065 ┆ 27m 36s   ┆ 4.842569 │
    │           ┆           ┆ 1 00:00:0 ┆ 1 00:27:3 ┆   ┆ 9         ┆           ┆ 805ms     ┆          │
    │           ┆           ┆ 2.490     ┆ 9.295     ┆   ┆           ┆           ┆           ┆          │
    │ electric  ┆ member    ┆ 2024-03-0 ┆ 2024-03-0 ┆ … ┆ -73.92731 ┆ 40.810893 ┆ 9m 25s    ┆ 2.659582 │
    │           ┆           ┆ 1 00:00:0 ┆ 1 00:09:2 ┆   ┆ 1         ┆           ┆ 264ms     ┆          │
    │           ┆           ┆ 4.120     ┆ 9.384     ┆   ┆           ┆           ┆           ┆          │
    │ electric  ┆ casual    ┆ 2024-03-0 ┆ 2024-03-0 ┆ … ┆ -73.98918 ┆ 40.742869 ┆ 3m 29s    ┆ 0.398795 │
    │           ┆           ┆ 1 00:00:0 ┆ 1 00:03:3 ┆   ┆ 6         ┆           ┆ 483ms     ┆          │
    │           ┆           ┆ 5.209     ┆ 4.692     ┆   ┆           ┆           ┆           ┆          │
    │ classic   ┆ member    ┆ 2024-03-0 ┆ 2024-03-0 ┆ … ┆ -74.01321 ┆ 40.705945 ┆ 30m 56s   ┆ 5.09153  │
    │           ┆           ┆ 1 00:00:0 ┆ 1 00:31:0 ┆   ┆ 9         ┆           ┆ 960ms     ┆          │
    │           ┆           ┆ 5.259     ┆ 2.219     ┆   ┆           ┆           ┆           ┆          │
    │ electric  ┆ member    ┆ 2024-03-0 ┆ 2024-03-0 ┆ … ┆ -73.97988 ┆ 40.668663 ┆ 11m 32s   ┆ 3.08728  │
    │           ┆           ┆ 1 00:00:0 ┆ 1 00:11:4 ┆   ┆ 1         ┆           ┆ 483ms     ┆          │
    │           ┆           ┆ 9.837     ┆ 2.320     ┆   ┆           ┆           ┆           ┆          │
    │ …         ┆ …         ┆ …         ┆ …         ┆ … ┆ …         ┆ …         ┆ …         ┆ …        │
    │ classic   ┆ member    ┆ 2024-03-3 ┆ 2024-03-3 ┆ … ┆ -73.97772 ┆ 40.729387 ┆ 1m 41s    ┆ 0.272175 │
    │           ┆           ┆ 1 23:55:2 ┆ 1 23:57:1 ┆   ┆ 4         ┆           ┆ 374ms     ┆          │
    │           ┆           ┆ 9.002     ┆ 0.376     ┆   ┆           ┆           ┆           ┆          │
    │ classic   ┆ member    ┆ 2024-03-3 ┆ 2024-03-3 ┆ … ┆ -73.96526 ┆ 40.763126 ┆ 3m 30s    ┆ 0.492269 │
    │           ┆           ┆ 1 23:55:3 ┆ 1 23:59:0 ┆   ┆ 9         ┆           ┆ 363ms     ┆          │
    │           ┆           ┆ 7.938     ┆ 8.301     ┆   ┆           ┆           ┆           ┆          │
    │ classic   ┆ member    ┆ 2024-03-3 ┆ 2024-03-3 ┆ … ┆ -73.95515 ┆ 40.81     ┆ 1m 46s    ┆ 0.406138 │
    │           ┆           ┆ 1 23:55:4 ┆ 1 23:57:2 ┆   ┆ 1         ┆           ┆ 248ms     ┆          │
    │           ┆           ┆ 0.087     ┆ 6.335     ┆   ┆           ┆           ┆           ┆          │
    │ electric  ┆ member    ┆ 2024-03-3 ┆ 2024-03-3 ┆ … ┆ -73.96264 ┆ 40.712605 ┆ 1m 43s    ┆ 0.283781 │
    │           ┆           ┆ 1 23:55:4 ┆ 1 23:57:2 ┆   ┆ 4         ┆           ┆ 906ms     ┆          │
    │           ┆           ┆ 1.173     ┆ 5.079     ┆   ┆           ┆           ┆           ┆          │
    │ electric  ┆ member    ┆ 2024-03-3 ┆ 2024-03-3 ┆ … ┆ -73.94063 ┆ 40.703172 ┆ 2m 6s     ┆ 0.501835 │
    │           ┆           ┆ 1 23:57:1 ┆ 1 23:59:2 ┆   ┆ 6         ┆           ┆ 109ms     ┆          │
    │           ┆           ┆ 6.025     ┆ 2.134     ┆   ┆           ┆           ┆           ┆          │
    └───────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴──────────┘

# Add borough and neighbourhood

``` python
stations = pl.read_parquet(data_dir / "stations.parquet")
print(stations)

stations = (
  stations.with_columns(point = pl.concat_list("lon", "lat"))
  .join(neighbourhoods, how = "cross")
  .with_columns(
    in_neighbourhood = pl.col("point").geo.point_in_polygon(pl.col("polygon"))
  )
  .filter(pl.col("in_neighbourhood"))
  .unique("station")
  .select(
    "station",
    "borough",
    "neighborhood"
  )
)
print(stations)
stations.write_parquet(data_dir / "stations_borough_neighborhood.parquet")
```

    shape: (2_143, 3)
    ┌──────────────────────────────┬────────────┬───────────┐
    │ station                      ┆ lon        ┆ lat       │
    │ ---                          ┆ ---        ┆ ---       │
    │ str                          ┆ f64        ┆ f64       │
    ╞══════════════════════════════╪════════════╪═══════════╡
    │ 1 Ave & E 110 St             ┆ -73.938203 ┆ 40.792327 │
    │ 1 Ave & E 16 St              ┆ -73.981656 ┆ 40.732219 │
    │ 1 Ave & E 18 St              ┆ -73.980544 ┆ 40.733876 │
    │ 1 Ave & E 30 St              ┆ -73.975361 ┆ 40.741457 │
    │ 1 Ave & E 38 St              ┆ -73.971822 ┆ 40.746202 │
    │ …                            ┆ …          ┆ …         │
    │ Wyckoff Ave & Stanhope St    ┆ -73.917914 ┆ 40.703545 │
    │ Wyckoff St & 3 Ave           ┆ -73.982586 ┆ 40.682755 │
    │ Wythe Ave & Metropolitan Ave ┆ -73.963198 ┆ 40.716887 │
    │ Wythe Ave & N 13 St          ┆ -73.957099 ┆ 40.722741 │
    │ Yankee Ferry Terminal        ┆ -74.016756 ┆ 40.687066 │
    └──────────────────────────────┴────────────┴───────────┘
    shape: (2_133, 3)
    ┌────────────────────────────┬───────────┬───────────────────┐
    │ station                    ┆ borough   ┆ neighborhood      │
    │ ---                        ┆ ---       ┆ ---               │
    │ str                        ┆ str       ┆ str               │
    ╞════════════════════════════╪═══════════╪═══════════════════╡
    │ Broadway & W 25 St         ┆ Manhattan ┆ Flatiron District │
    │ Bushwick Ave & Forrest St  ┆ Brooklyn  ┆ Bushwick          │
    │ New York Ave & Lenox Rd    ┆ Brooklyn  ┆ East Flatbush     │
    │ Wales Ave & E 147 St       ┆ Bronx     ┆ Mott Haven        │
    │ Clinton St & Grand St      ┆ Manhattan ┆ Lower East Side   │
    │ …                          ┆ …         ┆ …                 │
    │ 11 St & 43 Ave             ┆ Queens    ┆ Long Island City  │
    │ Underhill Ave & Pacific St ┆ Brooklyn  ┆ Prospect Heights  │
    │ 20 Ave & Shore Blvd        ┆ Queens    ┆ Ditmars Steinway  │
    │ 27 Ave & 3 St              ┆ Queens    ┆ Astoria           │
    │ 7 St & 3 Ave               ┆ Brooklyn  ┆ Gowanus           │
    └────────────────────────────┴───────────┴───────────────────┘

# Join Trips with Stations / Borough Neighbourhood

``` python
trips = (
  trips.join(
    stations.select(pl.all().name.suffix("_start")), on = "station_start"
  )
  .join(stations.select(pl.all().name.suffix("_end")), on = "station_end")
  .select(
    "bike_type",
    "rider_type",
    "datetime_start",
    "datetime_end",
    "duration",
    "station_start",
    "station_end",
    "neighborhood_start",
    "neighborhood_end",
    "borough_start",
    "borough_end",
    "lat_start",
    "lon_start",
    "lat_end",
    "lon_end",
    "distance"
  )
)
trips.write_parquet(data_dir / "trips_stations_borough_neighbourhood.parquet")
print(trips)
```

    shape: (2_638_971, 16)
    ┌───────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬──────────┐
    │ bike_type ┆ rider_typ ┆ datetime_ ┆ datetime_ ┆ … ┆ lon_start ┆ lat_end   ┆ lon_end   ┆ distance │
    │ ---       ┆ e         ┆ start     ┆ end       ┆   ┆ ---       ┆ ---       ┆ ---       ┆ ---      │
    │ cat       ┆ ---       ┆ ---       ┆ ---       ┆   ┆ f64       ┆ f64       ┆ f64       ┆ f64      │
    │           ┆ cat       ┆ datetime[ ┆ datetime[ ┆   ┆           ┆           ┆           ┆          │
    │           ┆           ┆ μs]       ┆ μs]       ┆   ┆           ┆           ┆           ┆          │
    ╞═══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪══════════╡
    │ electric  ┆ member    ┆ 2024-03-0 ┆ 2024-03-0 ┆ … ┆ -73.99507 ┆ 40.707065 ┆ -74.00731 ┆ 4.842569 │
    │           ┆           ┆ 1 00:00:0 ┆ 1 00:27:3 ┆   ┆ 1         ┆           ┆ 9         ┆          │
    │           ┆           ┆ 2.490     ┆ 9.295     ┆   ┆           ┆           ┆           ┆          │
    │ electric  ┆ member    ┆ 2024-03-0 ┆ 2024-03-0 ┆ … ┆ -73.89657 ┆ 40.810893 ┆ -73.92731 ┆ 2.659582 │
    │           ┆           ┆ 1 00:00:0 ┆ 1 00:09:2 ┆   ┆ 6         ┆           ┆ 1         ┆          │
    │           ┆           ┆ 4.120     ┆ 9.384     ┆   ┆           ┆           ┆           ┆          │
    │ electric  ┆ casual    ┆ 2024-03-0 ┆ 2024-03-0 ┆ … ┆ -73.98855 ┆ 40.742869 ┆ -73.98918 ┆ 0.398795 │
    │           ┆           ┆ 1 00:00:0 ┆ 1 00:03:3 ┆   ┆ 9         ┆           ┆ 6         ┆          │
    │           ┆           ┆ 5.209     ┆ 4.692     ┆   ┆           ┆           ┆           ┆          │
    │ classic   ┆ member    ┆ 2024-03-0 ┆ 2024-03-0 ┆ … ┆ -73.99520 ┆ 40.705945 ┆ -74.01321 ┆ 5.09153  │
    │           ┆           ┆ 1 00:00:0 ┆ 1 00:31:0 ┆   ┆ 8         ┆           ┆ 9         ┆          │
    │           ┆           ┆ 5.259     ┆ 2.219     ┆   ┆           ┆           ┆           ┆          │
    │ electric  ┆ member    ┆ 2024-03-0 ┆ 2024-03-0 ┆ … ┆ -73.95755 ┆ 40.668663 ┆ -73.97988 ┆ 3.08728  │
    │           ┆           ┆ 1 00:00:0 ┆ 1 00:11:4 ┆   ┆ 9         ┆           ┆ 1         ┆          │
    │           ┆           ┆ 9.837     ┆ 2.320     ┆   ┆           ┆           ┆           ┆          │
    │ …         ┆ …         ┆ …         ┆ …         ┆ … ┆ …         ┆ …         ┆ …         ┆ …        │
    │ classic   ┆ member    ┆ 2024-03-3 ┆ 2024-03-3 ┆ … ┆ -73.97455 ┆ 40.729387 ┆ -73.97772 ┆ 0.272175 │
    │           ┆           ┆ 1 23:55:2 ┆ 1 23:57:1 ┆   ┆ 2         ┆           ┆ 4         ┆          │
    │           ┆           ┆ 9.002     ┆ 0.376     ┆   ┆           ┆           ┆           ┆          │
    │ classic   ┆ member    ┆ 2024-03-3 ┆ 2024-03-3 ┆ … ┆ -73.97109 ┆ 40.763126 ┆ -73.96526 ┆ 0.492269 │
    │           ┆           ┆ 1 23:55:3 ┆ 1 23:59:0 ┆   ┆ 2         ┆           ┆ 9         ┆          │
    │           ┆           ┆ 7.938     ┆ 8.301     ┆   ┆           ┆           ┆           ┆          │
    │ classic   ┆ member    ┆ 2024-03-3 ┆ 2024-03-3 ┆ … ┆ -73.95962 ┆ 40.81     ┆ -73.95515 ┆ 0.406138 │
    │           ┆           ┆ 1 23:55:4 ┆ 1 23:57:2 ┆   ┆ 1         ┆           ┆ 1         ┆          │
    │           ┆           ┆ 0.087     ┆ 6.335     ┆   ┆           ┆           ┆           ┆          │
    │ electric  ┆ member    ┆ 2024-03-3 ┆ 2024-03-3 ┆ … ┆ -73.96597 ┆ 40.712605 ┆ -73.96264 ┆ 0.283781 │
    │           ┆           ┆ 1 23:55:4 ┆ 1 23:57:2 ┆   ┆ 1         ┆           ┆ 4         ┆          │
    │           ┆           ┆ 1.173     ┆ 5.079     ┆   ┆           ┆           ┆           ┆          │
    │ electric  ┆ member    ┆ 2024-03-3 ┆ 2024-03-3 ┆ … ┆ -73.94029 ┆ 40.703172 ┆ -73.94063 ┆ 0.501835 │
    │           ┆           ┆ 1 23:57:1 ┆ 1 23:59:2 ┆   ┆ 7         ┆           ┆ 6         ┆          │
    │           ┆           ┆ 6.025     ┆ 2.134     ┆   ┆           ┆           ┆           ┆          │
    └───────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴──────────┘

``` python
print(trips[:, :4])
print(trips[:, 4:7])
print(trips[:, 7:11])
print(trips[:, 11:])
```

    shape: (2_638_971, 4)
    ┌───────────┬────────────┬─────────────────────────┬─────────────────────────┐
    │ bike_type ┆ rider_type ┆ datetime_start          ┆ datetime_end            │
    │ ---       ┆ ---        ┆ ---                     ┆ ---                     │
    │ cat       ┆ cat        ┆ datetime[μs]            ┆ datetime[μs]            │
    ╞═══════════╪════════════╪═════════════════════════╪═════════════════════════╡
    │ electric  ┆ member     ┆ 2024-03-01 00:00:02.490 ┆ 2024-03-01 00:27:39.295 │
    │ electric  ┆ member     ┆ 2024-03-01 00:00:04.120 ┆ 2024-03-01 00:09:29.384 │
    │ electric  ┆ casual     ┆ 2024-03-01 00:00:05.209 ┆ 2024-03-01 00:03:34.692 │
    │ classic   ┆ member     ┆ 2024-03-01 00:00:05.259 ┆ 2024-03-01 00:31:02.219 │
    │ electric  ┆ member     ┆ 2024-03-01 00:00:09.837 ┆ 2024-03-01 00:11:42.320 │
    │ …         ┆ …          ┆ …                       ┆ …                       │
    │ classic   ┆ member     ┆ 2024-03-31 23:55:29.002 ┆ 2024-03-31 23:57:10.376 │
    │ classic   ┆ member     ┆ 2024-03-31 23:55:37.938 ┆ 2024-03-31 23:59:08.301 │
    │ classic   ┆ member     ┆ 2024-03-31 23:55:40.087 ┆ 2024-03-31 23:57:26.335 │
    │ electric  ┆ member     ┆ 2024-03-31 23:55:41.173 ┆ 2024-03-31 23:57:25.079 │
    │ electric  ┆ member     ┆ 2024-03-31 23:57:16.025 ┆ 2024-03-31 23:59:22.134 │
    └───────────┴────────────┴─────────────────────────┴─────────────────────────┘
    shape: (2_638_971, 3)
    ┌───────────────┬──────────────────────────────┬────────────────────────────┐
    │ duration      ┆ station_start                ┆ station_end                │
    │ ---           ┆ ---                          ┆ ---                        │
    │ duration[μs]  ┆ str                          ┆ str                        │
    ╞═══════════════╪══════════════════════════════╪════════════════════════════╡
    │ 27m 36s 805ms ┆ W 30 St & 8 Ave              ┆ Maiden Ln & Pearl St       │
    │ 9m 25s 264ms  ┆ Longwood Ave & Southern Blvd ┆ Lincoln Ave & E 138 St     │
    │ 3m 29s 483ms  ┆ Broadway & W 29 St           ┆ Broadway & W 25 St         │
    │ 30m 56s 960ms ┆ W 30 St & 8 Ave              ┆ Broadway & Morris St       │
    │ 11m 32s 483ms ┆ DeKalb Ave & Franklin Ave    ┆ 6 St & 7 Ave               │
    │ …             ┆ …                            ┆ …                          │
    │ 1m 41s 374ms  ┆ Ave C & E 16 St              ┆ E 14 St & Ave B            │
    │ 3m 30s 363ms  ┆ E 59 St & Madison Ave        ┆ 3 Ave & E 62 St            │
    │ 1m 46s 248ms  ┆ Amsterdam Ave & W 119 St     ┆ Morningside Ave & W 123 St │
    │ 1m 43s 906ms  ┆ S 4 St & Wythe Ave           ┆ S 3 St & Bedford Ave       │
    │ 2m 6s 109ms   ┆ Montrose Ave & Bushwick Ave  ┆ Humboldt St & Varet St     │
    └───────────────┴──────────────────────────────┴────────────────────────────┘
    shape: (2_638_971, 4)
    ┌─────────────────────┬─────────────────────┬───────────────┬─────────────┐
    │ neighborhood_start  ┆ neighborhood_end    ┆ borough_start ┆ borough_end │
    │ ---                 ┆ ---                 ┆ ---           ┆ ---         │
    │ str                 ┆ str                 ┆ str           ┆ str         │
    ╞═════════════════════╪═════════════════════╪═══════════════╪═════════════╡
    │ Chelsea             ┆ Financial District  ┆ Manhattan     ┆ Manhattan   │
    │ Longwood            ┆ Mott Haven          ┆ Bronx         ┆ Bronx       │
    │ Midtown             ┆ Flatiron District   ┆ Manhattan     ┆ Manhattan   │
    │ Chelsea             ┆ Financial District  ┆ Manhattan     ┆ Manhattan   │
    │ Bedford-Stuyvesant  ┆ Park Slope          ┆ Brooklyn      ┆ Brooklyn    │
    │ …                   ┆ …                   ┆ …             ┆ …           │
    │ Stuyvesant Town     ┆ Stuyvesant Town     ┆ Manhattan     ┆ Manhattan   │
    │ Upper East Side     ┆ Upper East Side     ┆ Manhattan     ┆ Manhattan   │
    │ Morningside Heights ┆ Morningside Heights ┆ Manhattan     ┆ Manhattan   │
    │ Williamsburg        ┆ Williamsburg        ┆ Brooklyn      ┆ Brooklyn    │
    │ Williamsburg        ┆ Williamsburg        ┆ Brooklyn      ┆ Brooklyn    │
    └─────────────────────┴─────────────────────┴───────────────┴─────────────┘
    shape: (2_638_971, 5)
    ┌───────────┬────────────┬───────────┬────────────┬──────────┐
    │ lat_start ┆ lon_start  ┆ lat_end   ┆ lon_end    ┆ distance │
    │ ---       ┆ ---        ┆ ---       ┆ ---        ┆ ---      │
    │ f64       ┆ f64        ┆ f64       ┆ f64        ┆ f64      │
    ╞═══════════╪════════════╪═══════════╪════════════╪══════════╡
    │ 40.749614 ┆ -73.995071 ┆ 40.707065 ┆ -74.007319 ┆ 4.842569 │
    │ 40.816459 ┆ -73.896576 ┆ 40.810893 ┆ -73.927311 ┆ 2.659582 │
    │ 40.746424 ┆ -73.988559 ┆ 40.742869 ┆ -73.989186 ┆ 0.398795 │
    │ 40.749653 ┆ -73.995208 ┆ 40.705945 ┆ -74.013219 ┆ 5.09153  │
    │ 40.69067  ┆ -73.957559 ┆ 40.668663 ┆ -73.979881 ┆ 3.08728  │
    │ …         ┆ …          ┆ …         ┆ …          ┆ …        │
    │ 40.729848 ┆ -73.974552 ┆ 40.729387 ┆ -73.977724 ┆ 0.272175 │
    │ 40.763505 ┆ -73.971092 ┆ 40.763126 ┆ -73.965269 ┆ 0.492269 │
    │ 40.808625 ┆ -73.959621 ┆ 40.81     ┆ -73.955151 ┆ 0.406138 │
    │ 40.712996 ┆ -73.965971 ┆ 40.712605 ┆ -73.962644 ┆ 0.283781 │
    │ 40.707678 ┆ -73.940297 ┆ 40.703172 ┆ -73.940636 ┆ 0.501835 │
    └───────────┴────────────┴───────────┴────────────┴──────────┘
