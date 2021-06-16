# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Simulate pVCF
# MAGIC 
# MAGIC Uses the 1000 genomes to simulate a project-level VCF

# COMMAND ----------

# MAGIC %pip install bioinfokit==0.8.5

# COMMAND ----------

import pyspark.sql.functions as fx
from pyspark.sql.types import *
import glow
spark = glow.register(spark)

import random
import string
import pandas as pd
import numpy as np
import os

from bioinfokit import visuz

# COMMAND ----------

# MAGIC %md
# MAGIC ##### set paths

# COMMAND ----------

user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
template_vcf = "dbfs:/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
output_delta = 'dbfs:/home/{}/genomics/data/delta/simulate_pvcf.delta'.format(user)
output_vcf = 'dbfs:/home/{}/genomics/data/delta/simulate_pvcf.vcf'.format(user)
output_vcf_local = '/dbfs/home/{}/genomics/data/delta/simulate_pvcf.vcf'.format(user)
os.environ["output_vcf"] = output_vcf_local
gwas_results_path = 'dbfs:/home/{}/genomics/data/delta/simulate_pvcf_gwas_results.delta'.format(user)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### set variables

# COMMAND ----------

random_seed = 42
random.seed(random_seed)
n_samples = 1000
minor_allele_frequency_cutoff = 0.01

# COMMAND ----------

# MAGIC %md
# MAGIC ##### define functions

# COMMAND ----------

def hardy_weinberg_principle(minor_allele_frequency):
  """
  given a minor allele frequency, 
  return an array of frequencies for each genotype
  """
  p = 1-minor_allele_frequency
  q = minor_allele_frequency
  aa = p * p
  aA = 2 * p * q
  AA = q * q
  return [aa, aA, AA]

sample_id_list = [str(i) for i in range (0, n_samples)]

def simulate_genotypes(minor_allele_frequency, n_samples, sample_list=sample_id_list):
  """
  given an array that contains the minor_allele_frequency as the first element, 
  return a genotypes struct of length=n_samples that conforms to the Glow variant schema, 
  with genotypes that are in Hardy Weinberg Equilibrium
  """
  n_samples = int(n_samples)
  frequencies = hardy_weinberg_principle(minor_allele_frequency[0])
  calls = [[0,0], [0,1], [1,1]]
  genotype_list = random.choices(calls, k=n_samples, weights=frequencies)
  new_lst = [list(x) for x in zip(sample_id_list, genotype_list)]
  genotypes = [{"sampleId":x, "calls": y} for x, y in new_lst]
  return genotypes

simulate_genotypes_udf = udf(simulate_genotypes, ArrayType(StructType([
              StructField("sampleId", StringType(), True),
              StructField("calls", ArrayType(IntegerType(), True))
              ])))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read 1000 Genomes VCF

# COMMAND ----------

vcf = spark.read.format("vcf").load(template_vcf).drop("genotypes")

# COMMAND ----------

display(vcf)

# COMMAND ----------

simulated_vcf = vcf.where(fx.col("INFO_AF")[0] > minor_allele_frequency_cutoff). \
                    withColumn("genotypes", simulate_genotypes_udf(fx.col("INFO_AF"), 
                                                                   fx.lit(n_samples)))

# COMMAND ----------

display(simulated_vcf)

# COMMAND ----------

simulated_vcf.write.mode("overwrite").format("bigvcf").save(output_vcf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### check output VCF

# COMMAND ----------

# MAGIC %sh
# MAGIC head -n 100 $output_vcf

# COMMAND ----------

simulated_vcf.write.mode("overwrite").format("delta").save(output_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### check output delta table

# COMMAND ----------

delta_vcf = spark.read.format("delta").load(output_delta)

# COMMAND ----------

display(delta_vcf)

# COMMAND ----------

delta_vcf.count()
