import time
import polars as pl

def create_pl_df(file_path: str) -> pl.DataFrame:
  pl.Config.set_streaming_chunk_size(4_000_000)

  pl_df: pl.DataFrame = pl.scan_csv(file_path, separator=";", has_header=False, 
                      new_columns=["station", "measure"], schema={"station": pl.String, "measure": pl.Float64})
  
  pl_df_agg: pl.DataFrame = pl_df.group_by("station").agg(
                max = pl.col("measure").max(),
                min = pl.col("measure").min(), 
                mean = pl.col("measure").mean()
              ).sort("station").collect(streaming=True)
  
  return pl_df_agg


if __name__ == "__main__":
  data_path: str = "../data/weather_measurements.txt"

  print("Starting file processing")

  start_time: float = time.time()
  df: pl.DataFrame = create_pl_df(file_path=data_path)
  end_time: float = time.time()
  
  print(df.head())
  print(f"\nProcessing completed in {end_time - start_time:.2f} seconds.")