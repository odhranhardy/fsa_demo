# Databricks notebook source
# MAGIC %md
# MAGIC #Azure Databricks
# MAGIC
# MAGIC Azure Databricks is a unified analytics platform designed for data engineering, data science, and machine learning. It integrates seamlessly with Microsoft Azure and is built on Apache Spark, providing scalable data processing capabilities.
# MAGIC
# MAGIC With Azure Databricks, you can:
# MAGIC
# MAGIC - Process large datasets efficiently.
# MAGIC - Create interactive analytics and visualizations.
# MAGIC - Manage and orchestrate data workflows.
# MAGIC - Build and deploy machine learning models.
# MAGIC
# MAGIC Four main components we're going to focus on today are:
# MAGIC
# MAGIC - Notebooks
# MAGIC - Catalogue
# MAGIC - Clusters
# MAGIC - Scheduling
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction to Databricks Notebooks
# MAGIC Databricks notebooks are interactive documents that combine code, visualizations, and narrative text. They are essential tools for collaboration, allowing data scientists and engineers to develop, document, and share their workflows.

# COMMAND ----------

# Create a new notebook and navigate through its interface
print("Welcome to your new Databricks notebook!")

# COMMAND ----------

# MAGIC %md
# MAGIC # Interchangeable Cells
# MAGIC Databricks notebooks support multiple languages, allowing you to use Python, SQL, R, and more within the same notebook. This flexibility is powerful for data analysis and engineering tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##R

# COMMAND ----------

# DBTITLE 1,Creating and Filtering a Simple DataFrame in R
# MAGIC %r
# MAGIC # Create a small dataset
# MAGIC data <- data.frame(
# MAGIC   ID = 1:5,
# MAGIC   Name = c("Alice", "Bob", "Charlie", "David", "Eve"),
# MAGIC   Age = c(23, 35, 45, 29, 31),
# MAGIC   Score = c(88, 92, 78, 85, 90)
# MAGIC )
# MAGIC
# MAGIC # View the dataset
# MAGIC print(data)
# MAGIC
# MAGIC # Select rows where Age is greater than 30
# MAGIC selected_data <- subset(data, Age > 30)
# MAGIC
# MAGIC # View the selected data
# MAGIC print(selected_data)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python

# COMMAND ----------

# Python cell
import pandas as pd

# Create a small dataset
data = pd.DataFrame({
    'ID': [1, 2, 3, 4, 5],
    'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'Age': [25, 30, 35, 40, 45],
    'Score': [85, 90, 95, 80, 75]
})

# View the dataset
print(data)

# Select rows where Age is greater than 30
selected_data = data[data['Age'] > 30]

# View the selected data
print(selected_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ##PySpark

# COMMAND ----------

# Missing import


# Create a small dataset
data = [
    (1, "Alice", 25, 85),
    (2, "Bob", 30, 90),
    (3, "Charlie", 35, 95),
    (4, "David", 40, 80),
    (5, "Eve", 45, 75)
]

# Define the schema
columns = ["ID", "Name", "Age", "Score"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Show the dataset
df.show()

# Select rows where Age is greater than 30
selected_data = df.filter(col("Age") > 30)

# Show the selected data
selected_data.show()

# Register the Dateframe as a TempView called people



# A Function to filer a dataframe, to only show people whose names begin with a "D"


#filter_people(df).show()



# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL cell
# MAGIC SELECT * FROM people
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Using Python and Spark SQL
# MAGIC Databricks integrates Spark SQL with Python, enabling seamless data manipulation and querying.
# MAGIC

# COMMAND ----------

# Push the results back into a DataFrame using Python
result = spark.sql("SELECT * FROM people")
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Git Integration
# MAGIC Version control and collaboration are crucial in data projects. Databricks allows you to link your workspace to a Git repository.
# MAGIC
# MAGIC - Link your Databricks workspace to a Git repository.
# MAGIC - Commit changes and push to the repository

# COMMAND ----------

# MAGIC %md
# MAGIC # Run Notebook Commands
# MAGIC You can run one notebook from another using the `%run` magic command, which is useful for modularizing code.
# MAGIC

# COMMAND ----------

 # Use the %run magic command to run another notebook\n",
%run ./env_setup.py

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT everything from the 

# COMMAND ----------

# MAGIC %md
# MAGIC # Using the Unity Catalog
# MAGIC The Unity Catalog provides data governance and management capabilities. You can navigate, query, and manage datasets within the catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query data within the catalog\n",
# MAGIC "SELECT * FROM catalog_name.schema_name.table_name

# COMMAND ----------

# MAGIC %md
# MAGIC # Connecting to Clusters
# MAGIC Clusters are the computational resources in Databricks. You can create and configure clusters to run your notebooks.
# MAGIC
# MAGIC - Create and configure a cluster
# MAGIC - Connect to different clusters for different purposes
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Using the Scheduler
# MAGIC Scheduling jobs allows you to automate your workflows. You can create jobs to run notebooks at specified times.
# MAGIC
# MAGIC 1. Create a job to run a notebook.
# MAGIC 2. Set up a schedule for the job.
# MAGIC 3. Monitor and manage scheduled jobs.
