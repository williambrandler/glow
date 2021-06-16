# Databricks notebook source
# MAGIC %md
# MAGIC ### Simulate covariates, phenotypes and offset

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC ##### set variables

# COMMAND ----------

n_samples = 1000
n_phenotypes = 1
n_covariates = 10

# COMMAND ----------

user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
output_path = '/dbfs/home/{}/genomics/data/pandas/simulate_'.format(user) + str(n_samples) + '_samples_'
output_covariates = output_path + str(n_covariates) + '_covariates.csv'
output_quantitative_phenotypes = output_path + str(n_phenotypes) + '_quantitative_phenotypes.csv'
output_binary_phenotypes = output_path + str(n_phenotypes) + '_binary_phenotypes.csv'
output_offset = output_path + str(n_phenotypes) + '_offset_phenotypes.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### simulate covariates

# COMMAND ----------

covariates_quantitative =  pd.DataFrame(np.random.random((n_samples, n_covariates - 2)), 
                                           columns=['Q'+ str(i) for i in range(n_covariates - 2)])
covariates_binary = pd.DataFrame(np.random.randint(0, 2, (n_samples, 2)), 
                                           columns=['B'+ str(i) for i in range(2)])
covariates = pd.concat([covariates_binary, covariates_quantitative], axis=1)
covariates.index.name = "sample_id"
covariates.index = covariates.index.map(str)
covariates.head(5)

# COMMAND ----------

covariates.to_csv(output_covariates, index=True, header=True, sep = ',')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### simulate phenotypes

# COMMAND ----------

binary_phenotypes = pd.DataFrame(np.random.randint(0, 
                                                   2, 
                                                   size=(n_samples, n_phenotypes)), 
                                 columns=['P'+ str(i) for i in range(n_phenotypes)])
binary_phenotypes.index.name = "sample_id"
binary_phenotypes.index = binary_phenotypes.index.map(str)
binary_phenotypes.head(5)

# COMMAND ----------

binary_phenotypes.to_csv(output_binary_phenotypes, index=True, header=True, sep = ',')

# COMMAND ----------

quantitative_phenotypes = pd.DataFrame(np.random.normal(loc=0.0, 
                                           scale=1.0, 
                                           size=(n_samples, n_phenotypes)), 
                                           columns=['P'+ str(i) for i in range(n_phenotypes)])
quantitative_phenotypes.index.name = "sample_id"
quantitative_phenotypes.index = quantitative_phenotypes.index.map(str)
quantitative_phenotypes.head(5)

# COMMAND ----------

quantitative_phenotypes.to_csv(output_quantitative_phenotypes, index=True, header=True, sep = ',')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### simulate offset

# COMMAND ----------

offset = pd.DataFrame(np.random.normal(loc=0.0, 
                                       scale=1.0, 
                                       size=(n_samples, n_phenotypes)), 
                                       columns=['P'+ str(i) for i in range(n_phenotypes)])
offset.index.name = "sample_id"
offset.index = offset.index.map(str)
offset.head(5)

# COMMAND ----------

offset.to_csv(output_offset, index=True, header=True, sep = ',')

# COMMAND ----------


