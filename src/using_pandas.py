import time
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

CONCURRENCY: int = cpu_count()


def process_chunk(chunk):
    agg_data: pd.DataFrame = (
        chunk.groupby("station")["measure"].agg(["min", "max", "mean"]).reset_index()
    )
    return agg_data


def create_df(file_path, row_total, chunk_size):
    total_chunks: int = row_total // chunk_size + (1 if row_total % chunk_size else 0)
    results: list = []

    with pd.read_csv(
        file_path,
        sep=";",
        header=None,
        names=["station", "measure"],
        chunksize=chunk_size,
    ) as reader:
        with Pool(CONCURRENCY) as pool:
            for chunk in tqdm(reader, total=total_chunks, desc="Processing"):
                result = pool.apply_async(process_chunk, (chunk,))
                results.append(result)

            results: list = [result.get() for result in results]

    final_df: pd.DataFrame = pd.concat(results, ignore_index=True)

    final_agg_df: pd.DataFrame = (
        final_df.groupby("station")
        .agg({"min": "min", "max": "max", "mean": "mean"})
        .reset_index()
        .sort_values("station")
    )

    return final_agg_df


if __name__ == "__main__":
    NUMBER_OF_ROWS: int = 1_000_000_000
    chunk_size: int = 100_000_000
    data_path: str = "../data/weather_measurements.txt"

    print("Starting file processing")

    start_time: float = time.time()
    df: pd.DataFrame = create_df(
        file_path=data_path, row_total=NUMBER_OF_ROWS, chunk_size=chunk_size
    )
    end_time: float = time.time()

    print(df.head())
    print(f"\nProcessing completed in {end_time - start_time:.2f} seconds.")
