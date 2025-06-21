from pathlib import Path
import json
import polars as pl
import polars_geo


class CitibikeAnalyser:
    def __init__(self, data_dir: str = "data"):
        """Initialize the Citibike analyser with data directory."""
        self.data_dir = Path(data_dir)
        self.trips = None
        self.neighbourhoods = None
        self.stations = None
        
    def load_trips(self, csv_filename: str = "202403-citibike-tripdata.csv") -> pl.DataFrame:
        """Load and preprocess trip data from CSV."""
        trips = (
            pl.scan_csv(
                self.data_dir / csv_filename,
                try_parse_dates=True,
                schema_overrides={
                    "start_station_id": pl.String,
                    "end_station_id": pl.String
                },
            )
            .select(
                bike_type=pl.col("rideable_type").str.split("_").list.get(0),
                rider_type=pl.col("member_casual"),
                datetime_start=pl.col("started_at"),
                datetime_end=pl.col("ended_at"),
                station_start=pl.col("start_station_name"),
                station_end=pl.col("end_station_name"),
                lon_start=pl.col("start_lng"),
                lat_start=pl.col("start_lat"),
                lon_end=pl.col("end_lng"),
                lat_end=pl.col("end_lat")
            )
            .with_columns(duration=(pl.col("datetime_end") - pl.col("datetime_start")))
            .drop_nulls()
            .filter(
                ~(
                    (pl.col("station_start") == pl.col("station_end"))
                    & (pl.col("duration").dt.total_seconds() < 5 * 60)
                )
            )
            .with_columns(
                distance=pl.concat_list(
                    "lon_start", "lat_start"
                ).geo.haversine_distance(pl.concat_list("lon_end", "lat_end"))
                / 1000
            )
        ).collect()
        
        self.trips = trips
        return trips
    
    def load_neighbourhoods(self, geojson_filename: str = "nyc-neighbourhoods.geojson") -> pl.LazyFrame:
        """Load neighbourhood data from GeoJSON."""
        neighbourhoods = (
            pl.read_json(self.data_dir / geojson_filename)
            .lazy()
            .select("features")
            .explode("features")
            .unnest("features")
            .unnest("properties")
            .select("neighborhood", "borough", "geometry")
            .unnest("geometry")
            .with_columns(polygon=pl.col("coordinates").list.first())
            .select("neighborhood", "borough", "polygon")
            .sort("neighborhood")
            .filter(pl.col("borough") != "Staten Island")
        )
        
        self.neighbourhoods = neighbourhoods
        return neighbourhoods
    
    def process_stations(self) -> pl.DataFrame:
        """Process stations and assign them to neighborhoods."""
        if self.trips is None:
            raise ValueError("Trips data not loaded. Call load_trips() first.")
        if self.neighbourhoods is None:
            raise ValueError("Neighborhoods data not loaded. Call load_neighbourhoods() first.")
        
        stations = (
            self.trips.lazy()
            .group_by(station=pl.col("station_start"))
            .agg(
                lat=pl.col("lat_start").median(),
                lon=pl.col("lon_start").median()
            )
            .with_columns(point=pl.concat_list("lon", "lat"))
            .drop_nulls()
            .join(self.neighbourhoods, how="cross")
            .with_columns(
                in_neighbourhood=pl.col("point").geo.point_in_polygon(pl.col("polygon"))
            )
            .filter(pl.col("in_neighbourhood"))
            .unique("station")
            .select(
                pl.col("station"),
                pl.col("borough"),
                pl.col("neighborhood")
            )
        ).collect()
        
        self.stations = stations
        return stations
    
    def enrich_trips_with_stations(self) -> pl.DataFrame:
        """Enrich trip data with station neighbourhood information."""
        if self.trips is None:
            raise ValueError("Trips data not loaded. Call load_trips() first.")
        if self.stations is None:
            raise ValueError("Stations data not processed. Call process_stations() first.")
        
        enriched_trips = (
            self.trips.join(
                self.stations.select(pl.all().name.suffix("_start")), on="station_start"
            )
            .join(self.stations.select(pl.all().name.suffix("_end")), on="station_end")
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
        
        return enriched_trips
    
    def run_full_analysis(self, csv_filename: str = "202403-citibike-tripdata.csv", 
                         geojson_filename: str = "nyc-neighbourhoods.geojson") -> pl.DataFrame:
        """Run the complete analysis pipeline."""
        print("Loading trips data...")
        self.load_trips(csv_filename)
        
        print("Loading neighbourhoods data...")
        self.load_neighbourhoods(geojson_filename)
        
        print("Processing stations...")
        self.process_stations()
        
        print("Enriching trips with station data...")
        enriched_trips = self.enrich_trips_with_stations()
        
        return enriched_trips
    
    def get_summary_stats(self) -> dict:
        """Get summary statistics of the processed data."""
        if self.trips is None:
            raise ValueError("No data loaded. Run analysis first.")
        
        return {
            "total_trips": len(self.trips),
            "total_stations": len(self.stations) if self.stations is not None else 0,
            "total_neighbourhoods": self.neighbourhoods.collect().height if self.neighbourhoods is not None else 0,
            "date_range": {
                "start": self.trips["datetime_start"].min(),
                "end": self.trips["datetime_start"].max()
            },
            "avg_distance_km": self.trips["distance"].mean(),
            "avg_duration_minutes": self.trips["duration"].dt.total_seconds().mean() / 60
        }


# Example usage
if __name__ == "__main__":
    # Build the polars_geo plugin (only needed once)
    # import subprocess
    # subprocess.run(["cd", "polars_geo", "&&", "uv", "run", "maturin", "develop", "--release"], shell=True)
    
    # Create analyzer instance
    analyser = CitibikeAnalyser()
    
    # Run full analysis
    enriched_trips = analyser.run_full_analysis()
    
    # Print results
    print("\nStations with neighbourhoods:")
    print(analyser.stations)
    
    print("\nEnriched trips:")
    print(enriched_trips)
    
    # Get summary statistics
    stats = analyser.get_summary_stats()
    print("\nSummary Statistics:")
    for key, value in stats.items():
        print(f"{key}: {value}")