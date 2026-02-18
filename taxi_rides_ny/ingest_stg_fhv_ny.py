import duckdb
import requests
from pathlib import Path
import click

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

def download_and_convert_files(taxi_type, years=None):
    """Download and convert taxi data files.
    
    Args:
        taxi_type: Type of taxi data (yellow, green, fhv)
        years: List of years to download. Defaults to [2019, 2020]
    """
    if years is None:
        years = [2019, 2020]
    
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    for year in years:
        for month in range(1, 13):
            parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            # Download CSV.gz file
            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            response = requests.get(f"{BASE_URL}/{taxi_type}/{csv_gz_filename}", stream=True)
            response.raise_for_status()

            with open(csv_gz_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"Converting {csv_gz_filename} to Parquet...")
            con = duckdb.connect()
            con.execute(f"""
                COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}'))
                TO '{parquet_filepath}' (FORMAT PARQUET)
            """)
            con.close()

            # Remove the CSV.gz file to save space
            csv_gz_filepath.unlink()
            print(f"Completed {parquet_filename}")

def update_gitignore():
    gitignore_path = Path(".gitignore")

    # Read existing content or start with empty string
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    # Add data/ if not already present
    if 'data/' not in content:
        with open(gitignore_path, 'a') as f:
            f.write('\n# Data directory\ndata/\n' if content else '# Data directory\ndata/\n')

def load_to_duckdb(taxi_types):
    """Load parquet files into DuckDB tables.
    
    Args:
        taxi_types: List of taxi types to load (yellow, green, fhv)
    """
    print("Loading parquet files into DuckDB...")
    con = duckdb.connect("taxi_rides_ny.duckdb")
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")

    # Load yellow and green taxi data
    for taxi_type in ["yellow", "green"]:
        if taxi_type in taxi_types and Path(f"data/{taxi_type}").exists():
            print(f"Creating prod.{taxi_type}_tripdata table...")
            con.execute(f"""
                CREATE OR REPLACE TABLE prod.{taxi_type}_tripdata AS
                SELECT * FROM read_parquet('data/{taxi_type}/*.parquet', union_by_name=true)
            """)
            
            # Get row count
            result = con.execute(f"SELECT COUNT(*) FROM prod.{taxi_type}_tripdata").fetchone()
            print(f"  Loaded {result[0]:,} rows")
    
    # Load FHV taxi data
    if "fhv" in taxi_types and Path("data/fhv").exists():
        print(f"Creating prod.fhv_tripdata table...")
        con.execute(f"""
            CREATE OR REPLACE TABLE prod.fhv_tripdata AS
            SELECT * FROM read_parquet('data/fhv/*.parquet', union_by_name=true)
        """)
        
        # Get row count
        result = con.execute(f"SELECT COUNT(*) FROM prod.fhv_tripdata").fetchone()
        print(f"  Loaded {result[0]:,} rows")

    con.close()
    print("DuckDB loading complete!")

@click.command()
@click.option('--skip-download', is_flag=True, help='Skip downloading files and only load to DuckDB')
@click.option('--download-only', is_flag=True, help='Only download files without loading to DuckDB')
@click.option('--yellow', is_flag=True, help='Include yellow taxi data (2019-2020)')
@click.option('--green', is_flag=True, help='Include green taxi data (2019-2020)')
@click.option('--fhv', is_flag=True, help='Include FHV (For-Hire Vehicle) taxi data')
@click.option('--years', default='2019', help='Comma-separated years to download (e.g., "2019" or "2019,2020")')
def main(skip_download, download_only, yellow, green, fhv, years):
    """Ingest NYC taxi data: download parquet files and/or load into DuckDB.
    
    If no taxi type flags are specified, all types are downloaded.
    """
    # Parse years
    year_list = [int(y.strip()) for y in years.split(',')]
    
    # Determine which taxi types to process
    # If no flags specified, default to all
    taxi_types = []
    if not (yellow or green or fhv):
        taxi_types = ["yellow", "green", "fhv"]
    else:
        if yellow:
            taxi_types.append("yellow")
        if green:
            taxi_types.append("green")
        if fhv:
            taxi_types.append("fhv")
    
    print(f"Processing taxi types: {', '.join(taxi_types)}")
    print(f"Years: {', '.join(map(str, year_list))}")
    print()
    
    if not skip_download:
        # Update .gitignore to exclude data directory
        update_gitignore()
        
        # Download and convert files for selected taxi types
        for taxi_type in taxi_types:
            print(f"\nDownloading {taxi_type} taxi data...")
            download_and_convert_files(taxi_type, years=year_list)
    
    if not download_only:
        # Load to DuckDB
        load_to_duckdb(taxi_types)

if __name__ == "__main__":
    main()