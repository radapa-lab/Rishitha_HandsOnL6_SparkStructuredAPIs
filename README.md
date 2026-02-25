# Music Streaming Analysis Using Spark Structured APIs

Name: Rishitha Adapa
Student ID: 801461577

## Overview

This in-class activity analyzes user music listening behavior using Apache Spark Structured APIs (PySpark DataFrame API).  
It identifies each user’s favorite genre, computes average listen time, calculates genre loyalty scores (Top 10), and finds users who listen between 12 AM and 5 AM.

---

## Dataset Description

The project uses two CSV datasets:

### listening_logs.csv
- user_id  
- song_id  
- timestamp  
- duration_sec  

### songs_metadata.csv
- song_id  
- title  
- artist  
- genre  
- mood    

The datasets are joined using `song_id' to create a single DataFrame for analysis.

---

## Repository Structure

This repository contains the following important files:

- **main.py**  
  Contains the Spark Structured APIs code that executes all 4 tasks.

- **datagen.py**  
  Generates or refreshes sample input data (used only if required by the activity instructions).

- **listening_logs.csv**  
  Input dataset containing user listening records.

- **songs_metadata.csv**  
  Input dataset containing song metadata such as genre.

- **output.txt**  
  Saved program output (results of all tasks) generated after running the code.

- **README.md**  
  Documentation of the activity, tasks, and commands.

- **Requirements**  
  Project-related notes or setup requirements.

---

## Output Directory Structure

The results of all tasks are saved into a file named:

`output.txt'

This file is created in the project’s root directory when the code is executed using the output-saving command.

The `output.txt' file contains outputs for:
- Task 1 (User Favorite Genres)
- Task 2 (Average Listen Time)
- Task 3 (Top 10 Genre Loyalty Scores)
- Task 4 (Late Night Listeners)

---

## Tasks and Outputs

### Task 1: User Favorite Genres
Identifies the most frequently listened genre for each user.

**Output Columns:**
- user_id  
- favorite_genre  
- listen_count  

---

### Task 2: Average Listen Time
Calculates the average listening duration (in seconds) for each user.

**Output Columns:**
- user_id  
- avg_listen_time_sec  

---

### Task 3: Genre Loyalty Score (Top 10 Users)
Computes a loyalty score for each user:

loyalty_score = top_genre_plays / total_plays  

Displays top 10 users ranked by loyalty score.

**Output Columns:**
- user_id  
- total_plays  
- top_genre_plays  
- loyalty_score  

---

### Task 4: Late Night Listeners
Identifies distinct users who listened between 12:00 AM and 5:00 AM.

**Output:**
- Distinct user_id values  

---

## Execution Instructions

## Prerequisites

Before starting the assignment, ensure that we have the following software installed and properly configured:

### Python 3.x
Verify installation:


python --version


### PySpark
Install using pip:


pip install pyspark


### Apache Spark
Verify installation:


spark-submit --version


---

## Commands Used in This In-Class Activity (With Explanation)

Below are the key commands used during this activity and what each one means:

### 1) Check Spark Installation

spark-submit --version

**What it does:**  
Confirms Spark is installed and shows the Spark version and Java version being used.

---

### 2) Check Which Java Is Being Picked

where java

**What it does:**  
Shows the exact location of the `java.exe' being used by your terminal.  
This helps confirm whether Java 17 or Java 25 is being used.

---

### 3) Check Current Java Version

java -version

**What it does:**  
Displays the active Java version currently being used in the terminal.

---

### 4) Switch from Java 25 to Java 17 (Permanent Fix Using Environment Variables)

Updated System Environment Variables:

JAVA_HOME = C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot

Updated Path variable to include:

%JAVA_HOME%\bin

Removed or moved older Java 25 entries below Java 17 in the Path list.

**What it does:**
This permanently configures the system to use Java 17 instead of Java 25.
Spark is compatible with Java 8/11/17, but not fully compatible with Java 25.
After restarting the terminal, running:

java -version

confirmed that Java 17 was active.

---

### 5) Run the Spark Code Using Python

python main.py

**What it does:**  
Runs your Spark program locally and prints the output tables for all tasks in the terminal.

---

### 6) Save Output to a File (While Also Printing in Terminal)

python main.py | Tee-Object -FilePath output.txt

**What it does:**  
Runs the program and saves all printed output into `output.txt', while still showing the output in the terminal.  
This is useful for submission proof.

---

### 7) Verify That output.txt Exists

dir output.txt

**What it does:**  
Checks whether `output.txt' was created successfully in the current folder.

---

## Errors and Resolutions

### Java Version Compatibility Issue

Initially, Java 25 was installed on the system. However, Spark did not function properly with Java 25 and produced compatibility warnings.

To resolve this issue, Java 17 was installed and configured by updating the `JAVA_HOME` environment variable and modifying the system `PATH` to prioritize Java 17.

After switching to Java 17, Spark executed successfully without compatibility issues.

This highlights that Apache Spark is more stable and compatible with Java 17 compared to newer Java versions.

---

### Warning: winutils.exe not found
If you see:

Did not find winutils.exe  
HADOOP_HOME and hadoop.home.dir are unset  

**Meaning:**  
This is a common Spark-on-Windows warning. It does not stop execution and can be ignored for this activity.

---

### Native Hadoop Library Warning
If you see:

Unable to load native-hadoop library  

**Meaning:**  
Spark falls back to built-in Java classes automatically. This does not affect correctness of results.
