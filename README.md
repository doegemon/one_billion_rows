# One Billion Rows: Processing Data with Python
## Introduction
The goal of this project is to demonstrate how to efficiently process a massive data file containing 1 billion rows (~16GB), specifically to calculate statistics (including aggregation and sorting which are heavy operations) using Python.

This project was inspired by [The One Billion Row Challenge](https://github.com/gunnarmorling/1brc), originally proposed for Java.

The original data file (_.csv_ format) consists of temperature measurements from various weather stations. Each record follows the format `<string: station name>;<double: measurement>`, with the temperature being displayed to one decimal place.

Here are ten example lines from the file:
```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
St. Johns;15.2
Cracow;12.6
Bridgetown;26.9
Istanbul;6.2
Roseau;34.4
Conakry;31.2
Istanbul;23.0
```

The first step is to create the dataset with 1 billion rows from the .csv file provided in the challenge. 

The challenge itself is to develop a Python program capable of reading this large file and calculating the minimum, average (rounded to one decimal place) and maximum temperature for each station, displaying the results in a table sorted by station name.

The output should look like this: 
| station      | min_temperature | mean_temperature | max_temperature |
|--------------|-----------------|------------------|-----------------|
| Abha         | -31.1           | 18.0             | 66.5            |
| Abidjan      | -25.9           | 26.0             | 74.6            |
| Abéché       | -19.8           | 29.4             | 79.9            |
| Accra        | -24.8           | 26.4             | 76.3            |
| Addis Ababa  | -31.8           | 16.0             | 63.9            |
| Adelaide     | -31.8           | 17.3             | 71.5            |
| ...          | ...             | ...              | ...             |
| Yangon       | -23.6           | 27.5             | 77.3            |
| Yaoundé      | -26.2           | 23.8             | 73.4            |
| Yellowknife  | -53.4           | -4.3             | 46.7            |
| Yerevan      | -38.6           | 12.4             | 62.8            |
| Yinchuan     | -45.2           | 9.0              | 56.9            |
| Zagreb       | -39.2           | 10.7             | 58.1            |

## Results
After creating the different Python files, the tests were carried out on a computer equipped with an AMD Ryzen 5 3600 processor and 16GB of RAM. 

The implementations used pure Python, Pandas, Polars and DuckDB approaches. The runtime results for processing the 1 billion line file are shown below:
| Implementation | Time |
| --- | --- |
| Only Python | ~35 minutes (2098 seconds) |
| Python + Pandas | 6 minutes (360 seconds) |
| Python + Polars | 1 minute and 10 seconds |
| Python + Duckdb | 48 seconds |

## Conclusions
This project clearly highlighted the effectiveness of various Python libraries in handling large volumes of data. 

Traditional methods such as pure Python (~35 minutes) and Pandas (6 minutes) required a series of tactics to implement batch processing.

On the other hand, libraries such as Polars and DuckDB have proven to be exceptionally effective, requiring fewer lines of code due to their inherent ability to distribute data in “streaming batches” more efficiently.

In the end, DuckDB excelled, achieving the lowest execution time thanks to its execution and data processing strategy.

These results emphasize the importance of selecting the right tool for large-scale data analysis, demonstrating that Python, with the right libraries, is a powerful choice for tackling big data challenges.

## References
This project is part of the projects created at [Jornada de Dados](https://suajornadadedados.com.br/).  

The repository used as reference for this project can be found [here](https://github.com/lvgalvao/One-Billion-Row-Challenge-Python/tree/main).