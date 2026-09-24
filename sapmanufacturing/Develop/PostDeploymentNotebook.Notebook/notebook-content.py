# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "__LAKEHOUSE_ID__",
# META       "default_lakehouse_name": "SAP_Manufacturing_LH",
# META       "default_lakehouse_workspace_id": "__WORKSPACE_ID__"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Post-Deployment - seed data & reframe the model
#
# Click **Run all**. The Lakehouse, GenerateSapData notebook, the
# **SAP_Manufacturing_Model** Direct Lake semantic model and the
# **SAP_Manufacturing_DataAgent** are all deployed for you by fabric-cicd.
#
# This notebook completes the runtime steps that need the *live* lakehouse:
# 1. Discovers the deployed workspace and lakehouse.
# 2. Runs **GenerateSapData** to create the 17 SAP Delta tables.
# 3. Reframes the Direct Lake model so it picks up the freshly written tables.
#
# When it finishes, open **SAP_Manufacturing_DataAgent** and ask e.g. *"How
# many orders are released and how many are in backlog?"* or *"How many
# operations sit at the Laser Balancing bottleneck?"*

# CELL ********************

# --- 1) Discover workspace + lakehouse -------------------------------------
import traceback
import notebookutils
import sempy.fabric as fabric
from pyspark.sql.types import StructType, StructField, StringType

ctx = notebookutils.runtime.context
WORKSPACE_ID = ctx.get("currentWorkspaceId") or ctx.get("workspaceId")
client = fabric.FabricRestClient()
items = client.get(f"/v1/workspaces/{WORKSPACE_ID}/items?type=Lakehouse").json()["value"]
match = [i for i in items if i["displayName"] == "SAP_Manufacturing_LH"]
if not match:
    raise RuntimeError("Lakehouse SAP_Manufacturing_LH not found in workspace.")
LAKEHOUSE_ID = match[0]["id"]
print("Workspace:", WORKSPACE_ID)
print("Lakehouse:", LAKEHOUSE_ID)

_log = []
def _rec(step, status, detail=""):
    _log.append((step, status, str(detail)[:5000]))
    print(step, "->", status)
def _writelog():
    try:
        sch = StructType([StructField("step", StringType()),
                          StructField("status", StringType()),
                          StructField("detail", StringType())])
        p = ("abfss://" + WORKSPACE_ID + "@onelake.dfs.fabric.microsoft.com/"
             + LAKEHOUSE_ID + "/Tables/postdeploy_log")
        spark.createDataFrame(_log or [("none","none","none")], sch) \
            .write.mode("overwrite").option("overwriteSchema", "true").save(p)
    except Exception as e:
        print("log write failed:", e)
_rec("discover", "ok", LAKEHOUSE_ID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- 2) Generate the SAP data ----------------------------------------------
try:
    notebookutils.notebook.run("GenerateSapData", 1800)
    _rec("generate_data", "ok")
except Exception:
    _rec("generate_data", "error", traceback.format_exc()); _writelog(); raise
print("SAP tables generated.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- 3) Reframe the deployed Direct Lake semantic model --------------------
"""Reframe the already-deployed Direct Lake semantic model.

The SemanticModel item is deployed by fabric-cicd (not created here). This module
only refreshes/reframes it at runtime, once the lakehouse tables exist: a fresh
Direct Lake model can't be queried until its framing picks up the tables, and the
lakehouse SQL analytics endpoint metadata sync lags table creation. So we poll a
metadata refresh and retry the dataset refresh with backoff.
"""
import time

MODEL_NAME = "SAP_Manufacturing_Model"


def refresh_semantic_model(ws_id, lh_id):
    """Find the deployed model by name and reframe it. Returns model id."""
    import sempy.fabric as fabric
    client = fabric.FabricRestClient()

    items = client.get("/v1/workspaces/%s/items?type=SemanticModel" % ws_id).json().get("value", [])
    match = [i for i in items if i["displayName"] == MODEL_NAME]
    if not match:
        raise RuntimeError("Semantic model %s not found - was it deployed by fabric-cicd?" % MODEL_NAME)
    model_id = match[0]["id"]

    # SQL endpoint metadata sync lags table creation; discover the endpoint id so we
    # can nudge it before each reframe attempt.
    try:
        props = client.get("/v1/workspaces/%s/lakehouses/%s" % (ws_id, lh_id)).json()
        se_id = props["properties"]["sqlEndpointProperties"]["id"]
    except Exception:
        se_id = None

    def _refresh_metadata():
        if se_id:
            try:
                client.post("/v1/workspaces/%s/sqlEndpoints/%s/refreshMetadata?preview=true"
                            % (ws_id, se_id), json={})
            except Exception as e:
                print("SQL endpoint metadata refresh skipped:", e)

    pbi = fabric.PowerBIRestClient()
    last_err = None
    for _attempt in range(12):
        _refresh_metadata()
        time.sleep(20)
        try:
            pbi.post("/v1.0/myorg/groups/%s/datasets/%s/refreshes" % (ws_id, model_id),
                     json={"type": "full"})
        except Exception as e:
            last_err = str(e)
            continue
        for _ in range(40):
            v = pbi.get("/v1.0/myorg/groups/%s/datasets/%s/refreshes?$top=1"
                        % (ws_id, model_id)).json()["value"]
            if v:
                status = v[0]["status"]
                if status == "Completed":
                    return model_id
                if status == "Failed":
                    last_err = v[0].get("serviceExceptionJson", "Failed")
                    break
            time.sleep(8)
    raise RuntimeError("Model refresh failed: " + str(last_err)[:800])


try:
    model_id = refresh_semantic_model(WORKSPACE_ID, LAKEHOUSE_ID)
    _rec("model_refresh", "ok", model_id)
except Exception:
    _rec("model_refresh", "error", traceback.format_exc()); _writelog(); raise
_writelog()
print("Semantic model reframed:", model_id)
print("POST_DEPLOY_DONE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
