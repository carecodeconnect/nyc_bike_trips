from pathlib import Path
import json
import polars as pl

# The command below builds the polars_geo plugin in Rust locally
# This only needs to be run once
#! cd polars_geo && uv run maturin develop --release
import polars_geo

data_dir = Path("data")
csv_trips_filename = "202403-citibike-tripdata.csv"
geojson_filename = "nyc-neighbourhoods.geojson"

trips = (
        pl.scan_csv(
        f"{data_dir}/{csv_trips_filename}", 
        try_parse_dates = True,
        schema_overrides = {
            "start_station_id": pl.String,
            "end_station_id": pl.String
        },
    )
    .select(
        bike_type = pl.col("rideable_type").str.split("_").list.get(0),
        rider_type = pl.col("member_casual"),
        datetime_start = pl.col("started_at"),
        datetime_end = pl.col("ended_at"),
        station_start = pl.col("start_station_name"),
        station_end = pl.col("end_station_name"),
        lon_start = pl.col("start_lng"),
        lat_start = pl.col("start_lat"),
        lon_end = pl.col("end_lng"),
        lat_end = pl.col("end_lat")
    )
    .with_columns(duration = (pl.col("datetime_end") - pl.col("datetime_start")))
    .drop_nulls()
    .filter(
        ~(
            (pl.col("station_start") == pl.col("station_end"))
            & (pl.col("duration").dt.total_seconds() < 5 * 60)
        )
    )
    .with_columns(
        distance = pl.concat_list(
            "lon_start", "lat_start"
        ).geo.haversine_distance(pl.concat_list("lon_end", "lat_end"))
        / 1000
    )
).collect()

#print(trips)

neighbourhoods = (
    pl.read_json(data_dir / geojson_filename)
    .lazy()
    .select("features")
    .explode("features")
    .unnest("features")
    .unnest("properties")
    .select("neighborhood", "borough", "geometry")
    .unnest("geometry")
    .with_columns(polygon = pl.col("coordinates").list.first())
    .select("neighborhood", "borough", "polygon")
    .sort("neighborhood")
    .filter(pl.col("borough") != "Staten Island")
)

#neighbourhoods

#type(neighbourhoods)

stations = (
    trips.lazy()
    .group_by(station = pl.col("station_start"))
    .agg(
        lat = pl.col("lat_start").median(),
        lon = pl.col("lon_start").median()
    )
    .with_columns(point = pl.concat_list("lon", "lat"))
    .drop_nulls()
    .join(neighbourhoods, how = "cross")
    .with_columns(
        in_neighbourhood = pl.col("point").geo.point_in_polygon(pl.col("polygon"))
    )
    .filter(pl.col("in_neighbourhood"))
    .unique("station")
    .select(
        pl.col("station"),
        pl.col("borough"),
        pl.col("neighborhood")
    )
).collect()

print(stations)
type(stations)

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
        "lat_start",
        "lon_start",
        "lat_end",
        "lon_end",
        "distance"
    )
)

print(trips)
