# assessment_software_project
Assessment Software Project for Applicants of the AI-Driven Edge-Cloud Computing Position

## Overview
There are 5 files in resources. The first one is config.properties that includes information to connect to MySQL database. As an example, there is config(example).properties file.

All tasks are located in `src\main\java\com\asessment\app\tasks` path.

## Task 1

`Task1PrimaryOperation` class includes primary operation for task1.
To test, Input data is arrived from program arguments. The example file is included in resources (data.txt).
 
## Task 2

`Task2` class includes:
✅ Reading data from three files (`names.txt`, `nationality.txt`, and `scores.txt`) in resources.
✅ connecting the three data stream.
✅ Deduplicating the merged stream.
✅ Grouping players by nationality.

`Task2WithSQL` class includes other operations of task 2.
Because of limitations in my laptop, I had to use simple generated data as input stream instead of previous three files.

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
