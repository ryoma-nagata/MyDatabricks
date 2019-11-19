# Databricks notebook source
# MAGIC %md
# MAGIC ##マウント
# MAGIC 
# MAGIC 参考リンク
# MAGIC 
# MAGIC [ADパススルー](https://docs.microsoft.com/ja-jp/azure/databricks/data/data-sources/azure/adls-passthrough#adls-aad-credentials)
# MAGIC 
# MAGIC [マウント](https://docs.microsoft.com/ja-jp/azure/databricks/data/data-sources/azure/azure-datalake-gen2#mount-adls-gen2)

# COMMAND ----------

filesystem="" #<FileSystem名>
storageaccount=""#<ストレージアカウント名>

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class":   spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# COMMAND ----------


# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "",
  mount_point = "/mnt/raw",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://" + filesystem + "@" + storageaccount + ".dfs.core.windows.net/",
  mount_point = "/mnt/raw",
  extra_configs = configs ) 

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

# COMMAND ----------

df2 = spark.read.parquet("abfss://raw@bankdemoadls.dfs.core.windows.net/mgtrillionrows")
display(df2)

# COMMAND ----------

df3=df2.count()

# COMMAND ----------

print(df3)

# COMMAND ----------

