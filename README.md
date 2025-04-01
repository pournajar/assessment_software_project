# assessment_software_project
Assessment Software Project for Applicants of the AI-Driven Edge-Cloud Computing Position

# Overview
The goal of this assessment is to implement a stream processing pipeline using Apache Flink and Kafka. The project consists of three progressively complex tasks. Applicants should aim for sophisticated and performance-efficient solutions, as simpler implementations will receive lower evaluations. Below are the details of the pipeline and the required steps for each task.

Task 1: Setting Up a Basic Flink Pipeline

1. Develop a Basic Flink Streaming Pipeline
- Implement the pipeline using Java.
- Utilize Gradle for project management.
2. Implement Unit Tests
- Use JUnit to validate functionality.

Task 2: Enhancing the Flink Pipeline and Integrating Kafka

1. Extend the Flink Pipeline
- Implement multiple operators, some as producers and others as consumers.
2. Optimize Kafka Integration
- Specify multiple partitions for Kafka topics to enhance performance.

Task 3: Setting Up a Flink Pipeline on Kubernetes with database integration

1. Kubernetes Deployment
- Deploy the Flink pipeline on a Kubernetes cluster (you can use Minikube).
2. Integrate a database as a data source
- Use relational database (e.g, MysQL) as the source of input data
3. Flink's CDC connectors
- Setup Flink's CDC connectors to capture and process real-time changes from the database.

# Task Submission
Submit your solution via a PRIVATE GitHub repository. Provide access to the user dps-uibk and include:
- Executable orchestration of the pipeline.
- Necessary input files.
- Function code deployed.
- Kubernetes YAML files.
- Brief execution instructions.

# Pipeline specifications

Task 1 introduces a real-time Olympic data management pipeline. It continuously fetches player
information, including names, nationalities, and scores in each set. The primary objective is to
collect and consolidate input data, ensuring no repetition of individuals. The pipeline, depicted in
Figure 1, shows the operators and their dependencies.

O1: Data Joining

Collects three inputs (name, score, nationality) and consolidates the score and nationality
information for each participant.

O2: Duplicate Check

Verifies the absence of repetitive individuals and eliminates any duplicates to ensure
accurate and reliable data

Task 2 extends pipeline in Task1 to add more functionality to it by adding more operators. The
primary goal is to deliver comprehensive scores for each country while highlighting the top three
individuals and their respective countries. Figure 2 shows the pipeline containing the operators
and dependencies.
 
To achieve this, the pipeline incorporates several operators:

O1: Data Joining

Collects three inputs (name, score, nationality) and consolidates the score and nationality
information for each participant.

O2: Duplicate Check

Verifies the absence of repetitive individuals and eliminates any duplicates to ensure
accurate and reliable data.

O3: Data Grouping by Nationality

Organizes the collected data into groups based on the nationality of the participants.

O4: Average Score Calculation

Computes the average score for each group (nationality) by aggregating individual scores
within each group.

O5: Highest Score Retrieval

Identifies and retrieves the individual with the highest score within each nationality
group.

O6: Display Average and Top player

Presents the average score and the best-performing individual within each nationality
group.

O7: Overall Best Group and Top Three Individuals

Calculates the average score for each group (country) and identifies the best-performing
country. Additionally, displays the top three individuals across all countries.