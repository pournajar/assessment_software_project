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
- reading from 3 separate files located in the resources: names.txt, nationality.txt, and scores.txt.
- connecting 3 data stream
- deduplicate the connected data stream
- grouping by nationality

`Task2WithSQL` class includes other operations of task 2.
Because of limitations in my laptop, I had to use simple generated data as input stream instead of previous three files.

It performs the following operations:
✔
- Create a sample data stream (`Tuple3 <name, nationality, score>`)
- Data Grouping by Nationality and Average Score Calculation
- Get the Highest Score per nationality
- Display Average and Top player per nationality
- Get Overall Best Group
- Get Top Three Individuals

## Task 3

`Task3` class includes connecting a `mySqlSource` and print real time data .

`DataBaseHlper` class includes initialization of player table using input file (data.txt).
After reading input data, a JDBC Sink defined to write players to MySQL database.  
