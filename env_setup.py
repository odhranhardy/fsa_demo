# Databricks notebook source
# MAGIC %md
# MAGIC #Initial Environment Setup Notepad
# MAGIC
# MAGIC - Create catalogue tables
# MAGIC - Add some data

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create a new catalogue called fsa
# MAGIC CREATE CATALOG IF NOT EXISTS fsa;
# MAGIC
# MAGIC -- Create a tables called commodity_tests
# MAGIC USE CATALOG fsa;
# MAGIC CREATE TABLE IF NOT EXISTS commodity_tests (
# MAGIC   reference_number STRING,	
# MAGIC   goods_location STRING,
# MAGIC   country_of_origin STRING,
# MAGIC   commodity_code INT,
# MAGIC   commodity_description STRING,
# MAGIC   tested_for STRING,
# MAGIC   status STRING
# MAGIC )
# MAGIC
# MAGIC -- Insert some sample data
# MAGIC INSERT INTO commodity_tests VALUES 
# MAGIC ("CHEDD.2024.0001","Valencia","Italy",301,"Fish","Salmonella","Accepted")
# MAGIC ("CHEDD.2024.0002","Valencia","Italy",301,"Fish","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0003","Barcelona","Spain",301,"Fish","Listeria","Accepted")
# MAGIC ("CHEDD.2024.0004","Madrid","Italy",301,"Fish","Salmonella","Rejected")
# MAGIC ("CHEDD.2024.0005","Barcelona","France",301,"Fish","Listeria","Pending")
# MAGIC ("CHEDD.2024.0006","Madrid","Italy",207,"Chicken","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0007","Barcelona","Italy",207,"Chicken","E. coli","Pending")
# MAGIC ("CHEDD.2024.0008","Valencia","Italy",301,"Fish","Salmonella","Accepted")
# MAGIC ("CHEDD.2024.0009","Madrid","Spain",402,"Cheese","Listeria","Pending")
# MAGIC ("CHEDD.2024.0010","Madrid","Spain",301,"Fish","Listeria","Rejected")
# MAGIC ("CHEDD.2024.0011","Barcelona","Italy",301,"Fish","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0012","Barcelona","Italy",402,"Cheese","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0013","Valencia","Spain",301,"Fish","Salmonella","Accepted")
# MAGIC ("CHEDD.2024.0014","Madrid","France",402,"Cheese","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0015","Barcelona","Spain",207,"Chicken","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0016","Valencia","France",301,"Fish","Listeria","Rejected")
# MAGIC ("CHEDD.2024.0017","Madrid","France",207,"Chicken","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0018","Madrid","Spain",301,"Fish","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0019","Barcelona","Italy",207,"Chicken","Salmonella","Accepted")
# MAGIC ("CHEDD.2024.0020","Valencia","France",402,"Cheese","Salmonella","Accepted")
# MAGIC ("CHEDD.2024.0021","Barcelona","Spain",402,"Cheese","Listeria","Rejected")
# MAGIC ("CHEDD.2024.0022","Madrid","France",301,"Fish","Salmonella","Accepted")
# MAGIC ("CHEDD.2024.0023","Barcelona","Spain",301,"Fish","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0024","Barcelona","Spain",301,"Fish","E. coli","Pending")
# MAGIC ("CHEDD.2024.0025","Madrid","Spain",402,"Cheese","Listeria","Pending")
# MAGIC ("CHEDD.2024.0026","Valencia","France",402,"Cheese","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0027","Barcelona","Spain",402,"Cheese","Salmonella","Pending")
# MAGIC ("CHEDD.2024.0028","Barcelona","Italy",207,"Chicken","Listeria","Pending")
# MAGIC ("CHEDD.2024.0029","Barcelona","Italy",207,"Chicken","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0030","Valencia","Italy",207,"Chicken","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0031","Barcelona","Italy",207,"Chicken","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0032","Valencia","Italy",402,"Cheese","Listeria","Accepted")
# MAGIC ("CHEDD.2024.0033","Madrid","France",301,"Fish","Salmonella","Accepted")
# MAGIC ("CHEDD.2024.0034","Valencia","France",301,"Fish","E. coli","Pending")
# MAGIC ("CHEDD.2024.0035","Madrid","Spain",207,"Chicken","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0036","Valencia","Italy",301,"Fish","Listeria","Pending")
# MAGIC ("CHEDD.2024.0037","Barcelona","France",207,"Chicken","Salmonella","Rejected")
# MAGIC ("CHEDD.2024.0038","Barcelona","France",207,"Chicken","Salmonella","Rejected")
# MAGIC ("CHEDD.2024.0039","Barcelona","France",207,"Chicken","Listeria","Accepted")
# MAGIC ("CHEDD.2024.0040","Valencia","France",402,"Cheese","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0041","Valencia","France",301,"Fish","Listeria","Rejected")
# MAGIC ("CHEDD.2024.0042","Barcelona","France",207,"Chicken","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0043","Valencia","France",207,"Chicken","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0044","Valencia","France",207,"Chicken","Listeria","Rejected")
# MAGIC ("CHEDD.2024.0045","Valencia","Italy",301,"Fish","Salmonella","Accepted")
# MAGIC ("CHEDD.2024.0046","Valencia","France",402,"Cheese","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0047","Madrid","Italy",402,"Cheese","Listeria","Pending")
# MAGIC ("CHEDD.2024.0048","Madrid","Italy",301,"Fish","Salmonella","Pending")
# MAGIC ("CHEDD.2024.0049","Valencia","Spain",402,"Cheese","Salmonella","Rejected")
# MAGIC ("CHEDD.2024.0050","Valencia","France",301,"Fish","E. coli","Pending")
# MAGIC ("CHEDD.2024.0051","Barcelona","Italy",402,"Cheese","Salmonella","Rejected")
# MAGIC ("CHEDD.2024.0052","Barcelona","Spain",207,"Chicken","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0053","Valencia","Spain",207,"Chicken","Listeria","Pending")
# MAGIC ("CHEDD.2024.0054","Barcelona","Italy",207,"Chicken","Salmonella","Accepted")
# MAGIC ("CHEDD.2024.0055","Madrid","Spain",207,"Chicken","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0056","Madrid","France",207,"Chicken","Salmonella","Accepted")
# MAGIC ("CHEDD.2024.0057","Barcelona","France",402,"Cheese","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0058","Madrid","France",402,"Cheese","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0059","Madrid","France",301,"Fish","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0060","Valencia","France",402,"Cheese","Listeria","Rejected")
# MAGIC ("CHEDD.2024.0061","Valencia","Italy",207,"Chicken","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0062","Barcelona","Italy",301,"Fish","Salmonella","Accepted")
# MAGIC ("CHEDD.2024.0063","Barcelona","Italy",207,"Chicken","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0064","Valencia","Italy",301,"Fish","Salmonella","Accepted")
# MAGIC ("CHEDD.2024.0065","Barcelona","Italy",402,"Cheese","Listeria","Accepted")
# MAGIC ("CHEDD.2024.0066","Barcelona","France",402,"Cheese","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0067","Valencia","France",207,"Chicken","Listeria","Rejected")
# MAGIC ("CHEDD.2024.0068","Valencia","Spain",301,"Fish","Salmonella","Accepted")
# MAGIC ("CHEDD.2024.0069","Barcelona","Spain",301,"Fish","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0070","Madrid","Spain",301,"Fish","Salmonella","Rejected")
# MAGIC ("CHEDD.2024.0071","Valencia","France",301,"Fish","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0072","Valencia","France",402,"Cheese","Salmonella","Rejected")
# MAGIC ("CHEDD.2024.0073","Barcelona","Spain",402,"Cheese","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0074","Valencia","Italy",402,"Cheese","Salmonella","Pending")
# MAGIC ("CHEDD.2024.0075","Madrid","Spain",402,"Cheese","Salmonella","Pending")
# MAGIC ("CHEDD.2024.0076","Barcelona","Spain",402,"Cheese","Listeria","Rejected")
# MAGIC ("CHEDD.2024.0077","Valencia","France",402,"Cheese","Salmonella","Pending")
# MAGIC ("CHEDD.2024.0078","Barcelona","Spain",402,"Cheese","Salmonella","Accepted")
# MAGIC ("CHEDD.2024.0079","Valencia","Spain",301,"Fish","Listeria","Rejected")
# MAGIC ("CHEDD.2024.0080","Madrid","France",402,"Cheese","Listeria","Pending")
# MAGIC ("CHEDD.2024.0081","Madrid","Italy",402,"Cheese","Salmonella","Rejected")
# MAGIC ("CHEDD.2024.0082","Madrid","Italy",207,"Chicken","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0083","Barcelona","Spain",301,"Fish","Salmonella","Pending")
# MAGIC ("CHEDD.2024.0084","Barcelona","Italy",301,"Fish","Listeria","Rejected")
# MAGIC ("CHEDD.2024.0085","Valencia","Italy",207,"Chicken","Salmonella","Pending")
# MAGIC ("CHEDD.2024.0086","Madrid","Spain",301,"Fish","Salmonella","Rejected")
# MAGIC ("CHEDD.2024.0087","Valencia","France",207,"Chicken","Listeria","Rejected")
# MAGIC ("CHEDD.2024.0088","Valencia","Italy",207,"Chicken","Salmonella","Rejected")
# MAGIC ("CHEDD.2024.0089","Barcelona","France",402,"Cheese","Salmonella","Rejected")
# MAGIC ("CHEDD.2024.0090","Barcelona","France",402,"Cheese","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0091","Valencia","Spain",301,"Fish","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0092","Valencia","France",301,"Fish","E. coli","Accepted")
# MAGIC ("CHEDD.2024.0093","Barcelona","France",207,"Chicken","Salmonella","Accepted")
# MAGIC ("CHEDD.2024.0094","Barcelona","Spain",301,"Fish","Salmonella","Pending")
# MAGIC ("CHEDD.2024.0095","Valencia","Italy",207,"Chicken","E. coli","Rejected")
# MAGIC ("CHEDD.2024.0096","Valencia","Italy",207,"Chicken","Listeria","Pending")
# MAGIC ("CHEDD.2024.0097","Valencia","Italy",207,"Chicken","Listeria","Accepted")
# MAGIC ("CHEDD.2024.0098","Valencia","Italy",207,"Chicken","Salmonella","Rejected")
# MAGIC ("CHEDD.2024.0099","Valencia","Spain",301,"Fish","Salmonella","Rejected")
# MAGIC ("CHEDD.2024.0100","Valencia","Italy",207,"Chicken","Listeria","Pending")

# COMMAND ----------

# MAGIC %md
# MAGIC
