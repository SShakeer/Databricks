# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "291e22bc-3a42-4821-b35c-f5a8232511fa",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="ADB202320NEW",key="secretakv20231120"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/61022000-1776-4c3a-b2c7-00ca679f7d6f/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://input@azurestorage202307.dfs.core.windows.net/",
  mount_point = "/mnt/raw123",
  extra_configs = configs)
