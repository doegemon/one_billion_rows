import time
import duckdb


def create_duckdb(file_path: str) -> None:
    duckdb.sql(
        f"""
    SELECT station, 
      MIN(temperature) AS min_temperature, 
      CAST(AVG(temperature) AS DECIMAL(3,1)) AS avg_temperature, 
      MAX(temperature) AS max_temperature
    FROM read_csv('{file_path}', auto_detect=False, sep=';', columns={{'station': 'VARCHAR', 'temperature': 'DECIMAL(3,1)'}})
    GROUP BY station
    ORDER BY station
  """
    ).show()


if __name__ == "__main__":
    data_path: str = "../data/weather_measurements.txt"

    print("Starting file processing")

    start_time: float = time.time()
    create_duckdb(file_path=data_path)
    end_time: float = time.time()

    print(f"\nProcessing completed in {end_time - start_time:.2f} seconds.")
