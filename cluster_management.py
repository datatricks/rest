# Databricks notebook source
# MAGIC %md
# MAGIC #Environment Variables

# COMMAND ----------

from env_vars import dbc_user
from env_vars import dbc_password
from env_vars import dbc_host

# COMMAND ----------

# MAGIC %md
# MAGIC #REST API

# COMMAND ----------

from rest_api import DBC_REST_API

# COMMAND ----------

rest = DBC_REST_API(dbc_user, dbc_password, dbc_host)

# COMMAND ----------

# MAGIC %md ####List Clusters

# COMMAND ----------

resp = rest.list_clusters()
rest.print_clusters(resp)

# COMMAND ----------

cluster_params = {
	"cluster_name": "REST-test",
	"spark_version": "2.1.x-scala2.10",
	"node_type_id": "r3.xlarge",
	"spark_conf": {
    	"spark.speculation": "true"
	},
	"aws_attributes": {
		"availability": "SPOT",
		"zone_id": "us-west-2a"
  	},
	"num_workers": 4
}

# COMMAND ----------

# MAGIC %md ####Create Cluster

# COMMAND ----------

resp = rest.create_cluster(cluster_params)

if resp.status_code == 200:
  print resp.json()
else:
  raise ValueError('Error in REST API Class')

# COMMAND ----------

resp = rest.list_clusters()
rest.print_clusters(resp)

# COMMAND ----------


