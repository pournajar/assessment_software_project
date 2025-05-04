# assessment_software_project

## Overview

This project consists of various tasks related to **data processing, stream processing, and database operations**.

## Project Structure

- The `resources` folder contains **five files**, including:
    - `config.properties`: Stores MySQL connection details.
    - `config(example).properties`: A sample configuration file for reference.
- All tasks are implemented in `src\main\java\com\asessment\app\tasks`.

## Task 1

- `Task1PrimaryOperation` implements the core logic for **Task 1**.
- The program reads **input data** from **command-line arguments**.
- A sample input file (`data.txt`) is provided in the **resources** folder for testing.

## Task 2

`Task2` class Handles stream processing by:

✅ Reading data from **three files** (`names.txt`, `nationality.txt`, and `scores.txt`) in resources.

✅ **Connecting** the three data stream.

✅ **Deduplicating** the merged stream.

✅ **Grouping players** by nationality.


`Task2WithSQL` class includes other operations of task 2.
Due to **hardware limitations**, this class uses generated sample data instead of reading from external files.

It performs the following operations:

✅ Create a sample data stream (`Tuple3 <name, nationality, score>`)

✅ Groups data by nationality and computes the average score.

✅ Finds the highest-scoring player per nationality.

✅ Displays the average and top player for each nationality.

✅ Determines the overall best-performing nationality.

✅ Identifies the top three individual players overall.


## Task 3

- `Task3` class includes connecting a `mySqlSource` and print real time data .

- `DataBaseHlper` class initializes the `player` table using input file (`data.txt`).
- After reading the input data, a JDBC Sink is defined to store players in MySQL.  
