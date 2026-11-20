# Databricks notebook source
# DBTITLE 1, Shared
# Define the webhook URL for shared
dev_shared_ingest_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/4b688a5685c945fc84185053eeece50c/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=DNQi1y0GlfFXJkdp_37xnjCX08IxFQXKicuK_xyc4ow"
dev_shared_trans_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_shared_ingest_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/4b688a5685c945fc84185053eeece50c/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=DNQi1y0GlfFXJkdp_37xnjCX08IxFQXKicuK_xyc4ow"
uat_shared_trans_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_shared_ingest_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/4b688a5685c945fc84185053eeece50c/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=DNQi1y0GlfFXJkdp_37xnjCX08IxFQXKicuK_xyc4ow"
preprd_shared_trans_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_shared_ingest_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/a7fc7da6cd414868bba133f9f3e2f596/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=T329S5HQbrK4oIKmJAJ40hbfPEms8CbYIltV-qUlYyQ"
prd_shared_trans_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/367c468d05e74afc9f848079894c393a/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=dEBmR25Rhx2LiClmG-8i3Tf29LEQUOizI7vtC_gX5IE"

# COMMAND ----------

# DBTITLE 1,travelbox
# Define the webhook URL for travelbox
dev_travelbox_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_travelbox_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_travelbox_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_travelbox_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/b562715883b5402a8d8258618c7f2547/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=LjBPGrrA9FSElSp2K6hP1BMXz8o6HEivhyy--kP5jaM"

# COMMAND ----------

# DBTITLE 1,maintenance

dev_maintenance_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_maintenance_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_maintenance_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_maintenance_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/2fdedb13163c49d1a6129578ab645232/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=Nes-8zqO8ngrE0p0gJcdYLti6fFHt7_d7ZR0miTYFUc"

# COMMAND ----------

# DBTITLE 1,maintenance_ride
# Define the webhook URL for maintenance_ride
dev_maintenance_ride_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_maintenance_ride_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_maintenance_ride_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_maintenance_ride_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/ca3c0851f10e43b1918b2aa4c4e6bd80/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=bSuA8jJxllBruev2BfMefvO-VRMUMyWmLlZ-a5MB0Tw"

# COMMAND ----------

# DBTITLE 1,mxdrr
#Data Ops Webhook URL
drr_prd_data_ops_webhook_url = 'https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/cu/13/workflows/84069cd70bb5477084c983c075db7de9/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=sIM9n022uPh-4ktGMCwyHybgAhYN2uSZpfniAlFNbMs'

# Define the webhook URL for drr
dev_drr_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_drr_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_drr_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_drr_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/37f3b698e0d44891967049bf823ab049/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=-PYszjeYPD9DXW6vDSiRC21jwvU4AjNkBCrBmfl3WeY"


dev_drr_ap_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_drr_ap_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_drr_ap_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_drr_ap_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/37f3b698e0d44891967049bf823ab049/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=-PYszjeYPD9DXW6vDSiRC21jwvU4AjNkBCrBmfl3WeY"

# COMMAND ----------

# DBTITLE 1,clymb_hourly
# Define the webhook URL for clymb_drr_hourly
dev_clymb_drr_hourly_attendance_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_clymb_drr_hourly_attendance_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_clymb_drr_hourly_attendance_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_clymb_drr_hourly_attendance_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

# COMMAND ----------

# DBTITLE 1,ea_drr
# Define the webhook URL for ea_drr
dev_ea_drr_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_ea_drr_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_ea_drr_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_ea_drr_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/e032e31652cd40a886b25f4834b5d02d/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=0JL3zVvcml5F73JzyboVroXaLCIBlfjOJZvv1iRQdrQ"


# COMMAND ----------

# DBTITLE 1,oracle_gl
# Define the webhook URL for oracle_gl
dev_oracle_gl_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_oracle_gl_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_oracle_gl_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_oracle_gl_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/b52e9597b98a45f88b9d6fa50a9d6a69/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=H7w1qDEt5OOL_WNBcHAAXqvZ3MdZgpniMzPCfowT0kw"

# COMMAND ----------

# DBTITLE 1,ridecollation
# Define the webhook URL for ridecollation
dev_ridecollation_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_ridecollation_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_ridecollation_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_ridecollation_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/b91b3d890ae5445ea364c0738834679b/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=r-KrLf4B9abvpU71HRTRmb2-PBmeTGcA02AArbVwcF0"

# COMMAND ----------

# DBTITLE 1,google_analytics
# Define the webhook URL for google_analytics
dev_google_analytics_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_google_analytics_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_google_analytics_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_google_analytics_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/5348cdb4ac33484dbf75ddb9a2b45a86/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=dI7cFVLtwL8Pu1m8Z9cJrcHJ5GuCuVps6ERGNNdnn5Y"

# COMMAND ----------

# DBTITLE 1,emarsys
dev_emarsys_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_emarsys_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_emarsys_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_emarsys_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/a4b69d68569a491fbdf52bbde24fa362/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=wpxf3VpurS4H3coB1weImDz5rP7yz0PmYRbhnRtIYPI"

# COMMAND ----------

# DBTITLE 1,scv
dev_scv_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_scv_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_scv_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_scv_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/3df1628674394aa386b595b724f1f3af/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=MBiWNYqbF6nIT54mU97YHCn4d9TMinnmbUYHFKV4Cpg"

dev_scv_mdm_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_scv_mdm_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_scv_mdm_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_scv_mdm_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/3df1628674394aa386b595b724f1f3af/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=MBiWNYqbF6nIT54mU97YHCn4d9TMinnmbUYHFKV4Cpg"


# COMMAND ----------

# DBTITLE 1,ycautomation
# Define the webhook URL for yc_automation
dev_ycautomation_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_ycautomation_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_ycautomation_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_ycautomation_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/9a87fb3e8f734cc28a82098c3f3e931c/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=NaIVo7dvtmSLZYmCia-Op2K5FWV_qhnxzU6J8VvLp68"

# COMMAND ----------

# DBTITLE 1,ar_rmcs_ig

dev_ar_ig_drr_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_ar_ig_drr_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_ar_ig_drr_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"


# COMMAND ----------

# DBTITLE 1,ar_rmcs_omni

dev_ar_omni_drr_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_ar_omni_drr_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_ar_omni_drr_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"


# COMMAND ----------

# DBTITLE 1,ipera

dev_ipera_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_ipera_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_ipera_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_ipera_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/2dcd2eaf013a4c8f91eb6caffff6209e/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=EevSKjG86E277RGTCOLdc780r2LS38AQcPMW0fuaiKI"

# COMMAND ----------

# DBTITLE 1,facepassappslogs
dev_facepassappslogs_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_facepassappslogs_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_facepassappslogs_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_facepassappslogs_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/3222621e69644cb4ba1ab52b52f4195e/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=xoRvj1StFLGPzJmY0mOQ8sl3tjEDmx94Ema-ZW-cHuY"

# COMMAND ----------

# DBTITLE 1,facepassreporting
dev_facepassreporting_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_facepassreporting_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_facepassreporting_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_facepassreporting_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/6a1cac6656554a9ca5c6a844a4ea69b6/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=01Az7pSQaUmZy_fqcYIEtcybiC-_RID5Y5-FO8wPi2U"

# COMMAND ----------

# DBTITLE 1,gss
dev_gss_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_gss_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_gss_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_gss_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/2ad64afddf31461b936d73d8b8cda3bc/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=d4HCpvDuQmjHlAseCtw99K_RZ-2o6bg7v2ds3gPNZnM"

# COMMAND ----------
# DBTITLE 1,mdop_fin
dev_mdop_finance_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

uat_mdop_finance_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

preprd_mdop_finance_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_mdop_finance_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/f34301bad47049d096ce0b429d9a4867/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=6PAm331TmNOGAGbQL67i8Vf18Zk1oI32tVuoF3PYUmo"

# COMMAND ----------

# DBTITLE 1,mdop_proc
dev_mdop_procurement_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

uat_mdop_procurement_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

preprd_mdop_procurement_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_mdop_procurement_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/f34301bad47049d096ce0b429d9a4867/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=6PAm331TmNOGAGbQL67i8Vf18Zk1oI32tVuoF3PYUmo"

# COMMAND ----------

# DBTITLE 1,mdop_hr
dev_mdop_hr_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

uat_mdop_hr_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

preprd_mdop_hr_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_mdop_hr_webhook_url = "https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/f34301bad47049d096ce0b429d9a4867/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=6PAm331TmNOGAGbQL67i8Vf18Zk1oI32tVuoF3PYUmo"

# COMMAND ----------

# DBTITLE 1,inventory
dev_inventory_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_inventory_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_inventory_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_inventory_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/9a87fb3e8f734cc28a82098c3f3e931c/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=NaIVo7dvtmSLZYmCia-Op2K5FWV_qhnxzU6J8VvLp68"

# COMMAND ----------

# DBTITLE 1,customer360
dev_customer360_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
uat_customer360_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"
preprd_customer360_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/64f9b954205249f682b0e66caa5d15e8/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=nqmGeh-mPgc4P7WX1C2GJOQHdgshtHSpwvw2pTSLpBY"

prd_customer360_webhook_url ="https://default0b0b76ba8e324c41813065ed5ca435.3c.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/2ad64afddf31461b936d73d8b8cda3bc/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=d4HCpvDuQmjHlAseCtw99K_RZ-2o6bg7v2ds3gPNZnM"
