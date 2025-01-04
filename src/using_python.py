import time
from csv import reader
from tqdm import tqdm
from collections import defaultdict, Counter

NUMBER_OF_ROWS: int = 1_000_000_000


def file_processing(file_path: str) -> dict:
    """
    Function that reads the file and aggregates the data by weather station
    """
    minimum: defaultdict = defaultdict(lambda: float("inf"))
    maximum: defaultdict = defaultdict(lambda: float("-inf"))
    sum_: defaultdict = defaultdict(float)
    measurements: Counter = Counter()

    with open(file=file_path, mode="r", encoding="utf-8") as file:
        _reader = reader(file, delimiter=";")
        for row in tqdm(_reader, total=NUMBER_OF_ROWS, desc="Processing"):
            station_name, temperature = str(row[0]), float(row[1])
            measurements.update([station_name])
            minimum[station_name] = min(minimum[station_name], temperature)
            maximum[station_name] = max(maximum[station_name], temperature)
            sum_[station_name] += temperature

    print("Data loaded, calculating statistics...")

    results: dict = {}
    for station, n_measurements in measurements.items():
        mean_temp: float = sum_[station] / n_measurements
        results[station] = (minimum[station], mean_temp, maximum[station])

    print("Statistic measured, sorting values...")

    sorted_results: dict = dict(sorted(results.items()))
    formatted_results: dict = {
        station: f"{min_temp:.1f}/{mean_temp:.1f}/{max_temp:.1f}"
        for station, (min_temp, mean_temp, max_temp) in sorted_results.items()
    }

    return formatted_results


if __name__ == "__main__":
    data_path: str = "../data/weather_measurements.txt"

    print("Starting file processing")
    start_time: float = time.time()

    final_results: dict = file_processing(file_path=data_path)

    end_time: float = time.time()

    for station, metrics in final_results.items():
        print(station, metrics, sep=": ")

    print(f"\nProcessing completed in {end_time - start_time:.2f} seconds.")
