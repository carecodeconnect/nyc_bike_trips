# Collect & Transform Data


# Import Modules

``` python
from pathlib import Path
import json
import polars as pl
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
    -rw-rw-r-- 1 solaris solaris  93M Jun 20 15:44 data/202403-citibike-tripdata.csv.parquet
    -rw-rw-r-- 1 solaris solaris 1.5M Jun 18 15:42 data/nyc-neighbourhoods.geojson
    -rw-rw-r-- 1 solaris solaris 357K Jun 20 15:44 data/nyc-neighbourhoods.parquet
    -rw-rw-r-- 1 solaris solaris  44K Jun 20 15:27 data/stations.parquet
