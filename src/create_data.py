import os
import sys
import random
import time


def check_args(file_args: list) -> None:
    """
    Sanity checks out the input and prints out usage if input is not a positive integer
    """
    try:
        if len(file_args) != 2 or int(file_args[1]) <= 0:
            raise Exception()
    except:
        print(
            "Usage: create_measurements.sh <positive integer number of records to create>"
        )
        print("You can use underscore notation for large number of records.")
        print("For example: 1_000_000_000 for one billion")
        exit()


def build_weather_station_name_list(file_path: str) -> list:
    """
    Collects the weather station names from the initial data provided and returns the unique names (removing duplicates)
    """
    station_names: list = []
    with open(file=file_path, mode="r", encoding="utf-8") as file:
        file_contents: str = file.read()
    for station in file_contents.splitlines():
        if "#" in station:
            next
        else:
            station_names.append(station.split(";")[0])
    return list(set(station_names))


def convert_bytes(num: float) -> str:
    """
    Convert bytes to a human-readable format (e.g., KiB, MiB, GiB)
    """
    for x in ["bytes", "KiB", "MiB", "GiB"]:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def format_elapsed_time(seconds: float) -> str:
    """
    Format elapsed time in a human-readable format
    """
    if seconds < 60:
        return f"{seconds:.3f} seconds"
    elif seconds < 3600:
        minutes, seconds = divmod(seconds, 60)
        return f"{int(minutes)} minutes {int(seconds)} seconds"
    else:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if minutes == 0:
            return f"{int(hours)} hours {int(seconds)} seconds"
        else:
            return f"{int(hours)} hours {int(minutes)} minutes {int(seconds)} seconds"


def estimate_file_size(weather_station_names: list, num_rows_to_create: int) -> str:
    """
    Tries to estimate how large the file with the generated data will be
    """
    max_string: float = float("-inf")
    min_string: float = float("inf")
    per_record_size: float = 0
    record_size_unit: str = "bytes"

    for station in weather_station_names:
        if len(station) > max_string:
            max_string: float = len(station)
        if len(station) < min_string:
            min_string: float = len(station)
        per_record_size = ((max_string + min_string * 2) + len(",-123.4")) / 2

    total_file_size: float = num_rows_to_create * per_record_size
    human_file_size: str = convert_bytes(total_file_size)

    return f"The estimated file size is: {human_file_size}.\n The final size will probably be smaller."


def build_test_data(
    weather_station_names: list, num_rows_to_create: int, file_path: str
) -> None:
    """
    Generates and writes in the file the requested length of test data
    """
    start_time: float = time.time()
    coldest_temp: float = -99.9
    hottest_temp: float = 99.9
    station_names_10k_max: list = random.choices(weather_station_names, k=10_000)
    batch_size: int = (
        10_000  # instead of writing line by line to file, process a batch of stations
    )
    progress_step: int = max(1, (num_rows_to_create // batch_size) // 100)
    print("Creating file...")

    try:
        with open(file=file_path, mode="w", encoding="utf-8") as file:
            for s in range(0, num_rows_to_create // batch_size):
                batch: list = random.choices(station_names_10k_max, k=batch_size)
                prepped_deviated_batch: str = "\n".join(
                    [
                        f"{station};{random.uniform(coldest_temp, hottest_temp):.1f}"
                        for station in batch
                    ]
                )
                file.write(prepped_deviated_batch + "\n")
        sys.stdout.write("\n")
    except Exception as e:
        print("Something went wrong. Printing error info and exiting...")
        print(e)
        exit()

    end_time: float = time.time()
    elapsed_time: float = end_time - start_time
    file_size: int = os.path.getsize(file_path)
    human_file_size: str = convert_bytes(file_size)

    print("File successfully created!")
    print(f"Final size:  {human_file_size}")
    print(f"Time elapsed: {format_elapsed_time(elapsed_time)}")


def main() -> None:
    """
    Main program function
    """
    num_rows_to_create: int = 1_000_000_000
    initial_file_path = "../data/weather_stations.csv"
    final_file_path = "../data/weather_measurements.txt"
    weather_station_names: list = []
    weather_station_names = build_weather_station_name_list(file_path=initial_file_path)
    print(estimate_file_size(weather_station_names, num_rows_to_create))
    build_test_data(
        weather_station_names, num_rows_to_create, file_path=final_file_path
    )
    print("Test file done.")


if __name__ == "__main__":
    main()
exit()
