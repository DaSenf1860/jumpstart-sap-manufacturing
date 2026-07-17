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

# # Post-Deployment - build the demo
#
# Click **Run all**. This notebook:
# 1. Discovers the deployed workspace and lakehouse.
# 2. Runs **GenerateSapData** to create the 17 SAP Delta tables.
# 3. Builds the **SAP_Manufacturing_Model** Direct Lake semantic model and
#    reframes it.
# 4. Creates and publishes the **SAP_Manufacturing_DataAgent** grounded on
#    that model.
#
# When it finishes, open the data agent and ask e.g. *"How many orders are
# released and how many are in backlog?"* or *"How many operations sit at the
# Laser Balancing bottleneck?"*

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

# --- 3) Build + reframe the Direct Lake semantic model ---------------------
"""Build and deploy the Direct Lake semantic model from inside a Fabric notebook.

Uses sempy.fabric REST clients so it runs headlessly with the notebook identity.
"""
import base64
import json
import time

MODEL_NAME = "SAP_Manufacturing_Model"
EXPR = "DirectLake - SAP Manufacturing"

S, I, D, T, F = "string", "int64", "date", "dateTime", "double"

TABLES = {
    "MARA": ("mara", [
        ("MATNR", S), ("MTART", S), ("MATKL", S), ("MEINS", S), ("NTGEW", F),
        ("GEWEI", S), ("PRDHA", S), ("ProductGroup", S), ("MAKTX", S)]),
    "MARC": ("marc", [
        ("MATNR", S), ("WERKS", S), ("DISPO", S), ("BESKZ", S), ("FEVOR", S),
        ("DZEIT", I), ("InHouseProductionTimeDays", F)]),
    "MARD": ("mard", [
        ("MATNR", S), ("WERKS", S), ("LGORT", S), ("LABST", F), ("INSME", F),
        ("StockDate", D)]),
    "D_Part": ("d_part", [
        ("MATNR", S), ("ProductGroup", S), ("Description", S), ("ABC_Class", S),
        ("XYZ_Class", S), ("CriticalPart", I), ("StandardPrice", F)]),
    "MRP_Cntr": ("mrp_cntr", [
        ("DISPO", S), ("WERKS", S), ("Name", S), ("ResponsibilityArea", S),
        ("Email", S)]),
    "ProductHierarchy": ("producthierarchy", [
        ("ProductHierarchy", S), ("Description", S), ("Level", I), ("Parent", S)]),
    "ProductGroup": ("productgroup", [
        ("ProductGroup", S), ("Description", S)]),
    "WorkCenters": ("workcenters", [
        ("WorkCenter", S), ("WERKS", S), ("Description", S), ("NGRAD", I),
        ("EINZH", F), ("AZNOR", I), ("CapacityCategory", S), ("Bottleneck", I),
        ("TheoreticalCapacityHrDay", F)]),
    "ActivityTypes": ("activitytypes", [
        ("ActivityType", S), ("Description", S), ("Unit", S)]),
    "MovementTypes": ("movementtypes", [
        ("MovementType", S), ("Description", S), ("Sign", S), ("Direction", S)]),
    "COOIS_OrderHeaders": ("coois_orderheaders", [
        ("Order", S), ("Material", S), ("MaterialText", S), ("WERKS", S),
        ("OrderType", S), ("OrderQuantity", F), ("Unit", S),
        ("SystemStatus", S), ("StatusShort", S), ("Priority", I),
        ("SOP_Relevant", I), ("Backlog", I), ("CreationDate", D),
        ("StartDate", D), ("FinishDate", D), ("DeliveredQuantity", F),
        ("ProductGroup", S)]),
    "COOIS_OrderOperations": ("coois_orderoperations", [
        ("Order", S), ("Operation", S), ("WorkCenter", S), ("WERKS", S),
        ("OperationText", S), ("StatusShort", S), ("SystemStatus", S),
        ("ControlKey", S), ("TargetQuantity", F), ("ActualQuantity", F),
        ("SetupTime", F), ("MachineTime", F), ("LaborTime", F),
        ("StartDate", D), ("FinishDate", D), ("Material", S),
        ("ProductGroup", S), ("BottleneckOperation", I)]),
    "COOIS_Confirmations": ("coois_confirmations", [
        ("Confirmation", S), ("Order", S), ("Operation", S), ("WorkCenter", S),
        ("WERKS", S), ("YieldQuantity", F), ("ScrapQuantity", F),
        ("ReworkQuantity", F), ("ActualMachineTime", F), ("ActualLaborTime", F),
        ("ActualEnd", T), ("SAP_PostingTime", T), ("DelayHours", F),
        ("Cancelled", I), ("Material", S)]),
    "CO24": ("co24", [
        ("Order", S), ("Component", S), ("WERKS", S), ("LGORT", S),
        ("Reqmnt_qty", F), ("Comm_qty", F), ("Qty_issued", F),
        ("ShortageQuantity", F), ("MissingPart", I), ("RequirementDate", D),
        ("ComponentText", S)]),
    "MATDOC": ("matdoc", [
        ("Mblnr", S), ("Line", S), ("Matnr", S), ("WERKS", S), ("LGORT", S),
        ("MovementType", S), ("Quantity", F), ("Unit", S), ("PostingDate", D),
        ("DocumentDate", D), ("Order", S), ("CostCenter", S)]),
    "SOP_Targets": ("sop_targets", [
        ("ProductGroup", S), ("Description", S), ("WERKS", S), ("Period", S),
        ("TargetQuantity", F), ("ActualQuantity", F), ("Deviation", F),
        ("Version", S)]),
    "tmp_PartStockV1": ("tmp_partstockv1", [
        ("Part", S), ("WERKS", S), ("Stock", F), ("Unit", S),
        ("SnapshotTime", T)]),
}

MEASURES = {
    "COOIS_OrderHeaders": [
        ("Order Count", "COUNTROWS('COOIS_OrderHeaders')", "#,0",
         "Total number of production orders"),
        ("Released Order Load",
         "CALCULATE([Order Count], CONTAINSSTRING('COOIS_OrderHeaders'[SystemStatus], \"REL\"))",
         "#,0", "Released orders (status REL) = order load"),
        ("Backlog Order Count",
         "CALCULATE([Order Count], 'COOIS_OrderHeaders'[Backlog] = 1)", "#,0",
         "Open orders with a scheduled finish date in the past"),
        ("Total Order Quantity", "SUM('COOIS_OrderHeaders'[OrderQuantity])", "#,0",
         "Sum of order quantities"),
        ("Total Delivered Quantity", "SUM('COOIS_OrderHeaders'[DeliveredQuantity])", "#,0",
         "Sum of delivered quantities"),
    ],
    "COOIS_Confirmations": [
        ("Total Yield", "SUM('COOIS_Confirmations'[YieldQuantity])", "#,0",
         "Confirmed good quantity"),
        ("Total Scrap", "SUM('COOIS_Confirmations'[ScrapQuantity])", "#,0",
         "Confirmed scrap quantity"),
        ("Total Rework", "SUM('COOIS_Confirmations'[ReworkQuantity])", "#,0",
         "Confirmed rework quantity"),
        ("Scrap Rate", "DIVIDE([Total Scrap], [Total Yield])", "0.0%",
         "Scrap relative to yield"),
        ("Avg Posting Delay Hours",
         "AVERAGE('COOIS_Confirmations'[DelayHours])", "0.0",
         "Mean delay between actual completion and SAP posting, in hours"),
    ],
    "CO24": [
        ("Missing Part Count", "CALCULATE(COUNTROWS('CO24'), 'CO24'[MissingPart] = 1)", "#,0",
         "Number of components with a shortage"),
        ("Total Shortage Quantity", "SUM('CO24'[ShortageQuantity])", "#,0",
         "Sum of all shortage quantities"),
    ],
    "COOIS_OrderOperations": [
        ("Operation Count", "COUNTROWS('COOIS_OrderOperations')", "#,0",
         "Total number of order operations"),
        ("Bottleneck Operation Count (Laser Balancing)",
         "CALCULATE(COUNTROWS('COOIS_OrderOperations'), 'COOIS_OrderOperations'[BottleneckOperation] = 1)",
         "#,0", "Operations at the bottleneck work center Laser Balancing"),
    ],
    "SOP_Targets": [
        ("Total Target Quantity", "SUM('SOP_Targets'[TargetQuantity])", "#,0",
         "S&OP target quantity"),
        ("Total SOP Actual Quantity", "SUM('SOP_Targets'[ActualQuantity])", "#,0",
         "S&OP actual quantity"),
        ("SOP Deviation", "SUM('SOP_Targets'[Deviation])", "#,0",
         "Deviation of actual vs target"),
    ],
}

RELS = [
    ("MARC", "MATNR", "MARA", "MATNR"),
    ("MARD", "MATNR", "MARA", "MATNR"),
    ("D_Part", "MATNR", "MARA", "MATNR"),
    ("MARA", "ProductGroup", "ProductGroup", "ProductGroup"),
    ("MARA", "PRDHA", "ProductHierarchy", "ProductHierarchy"),
    ("MARC", "DISPO", "MRP_Cntr", "DISPO"),
    ("COOIS_OrderHeaders", "Material", "MARA", "MATNR"),
    ("COOIS_OrderOperations", "Order", "COOIS_OrderHeaders", "Order"),
    ("COOIS_OrderOperations", "WorkCenter", "WorkCenters", "WorkCenter"),
    ("COOIS_Confirmations", "Order", "COOIS_OrderHeaders", "Order"),
    ("COOIS_Confirmations", "WorkCenter", "WorkCenters", "WorkCenter"),
    ("CO24", "Order", "COOIS_OrderHeaders", "Order"),
    ("MATDOC", "Order", "COOIS_OrderHeaders", "Order"),
    ("SOP_Targets", "ProductGroup", "ProductGroup", "ProductGroup"),
]

TAB = "\t"


def _q(name):
    return "'" + name + "'"


def _summarize(t):
    return "none" if t in (S, D, T) else "sum"


def _table_tmdl(mtable, physical, cols, meas):
    lines = ["table " + _q(mtable), ""]
    for (mname, dax, fmt, desc) in meas:
        if desc:
            lines.append(TAB + "/// " + desc)
        lines.append(TAB + "measure " + _q(mname) + " = " + dax)
        lines.append(TAB + TAB + "formatString: " + fmt)
        lines.append("")
    for (col, typ) in cols:
        tmdl_type = "dateTime" if typ in (D, T) else typ
        lines.append(TAB + "column " + _q(col))
        lines.append(TAB + TAB + "dataType: " + tmdl_type)
        lines.append(TAB + TAB + "summarizeBy: " + _summarize(typ))
        lines.append(TAB + TAB + "sourceColumn: " + col)
        if typ == D:
            lines.append(TAB + TAB + "formatString: yyyy-mm-dd")
        elif typ == T:
            lines.append(TAB + TAB + "formatString: yyyy-mm-dd hh:nn:ss")
        lines.append("")
    lines.append(TAB + "partition " + _q(mtable) + " = entity")
    lines.append(TAB + TAB + "mode: directLake")
    lines.append(TAB + TAB + "source")
    lines.append(TAB + TAB + TAB + "entityName: " + physical)
    lines.append(TAB + TAB + TAB + "expressionSource: " + _q(EXPR))
    lines.append("")
    return "\n".join(lines)


def _build_parts(ws, lh):
    model_lines = [
        "model Model",
        TAB + "culture: en-US",
        TAB + "defaultPowerBIDataSourceVersion: powerBI_V3",
        TAB + "sourceQueryCulture: en-US",
        "",
        "expression " + _q(EXPR) + " = ```",
        TAB + "let",
        TAB + TAB + 'Source = AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/'
        + ws + "/" + lh + '", [HierarchicalNavigation=true])',
        TAB + "in",
        TAB + TAB + "Source",
        TAB + "```",
        "",
    ]
    for mtable in TABLES:
        model_lines.append("ref table " + _q(mtable))
    model_tmdl = "\n".join(model_lines) + "\n"

    rel_lines = []
    for i, (ft, fc, tt, tc) in enumerate(RELS):
        rel_lines.append("relationship rel_%02d_%s_%s" % (i, ft, tt))
        rel_lines.append(TAB + "fromColumn: " + _q(ft) + "." + _q(fc))
        rel_lines.append(TAB + "toColumn: " + _q(tt) + "." + _q(tc))
        rel_lines.append("")
    rel_tmdl = "\n".join(rel_lines)

    database_tmdl = ("database " + MODEL_NAME + "\n" + TAB + "compatibilityLevel: 1702\n"
                     + TAB + "compatibilityMode: powerBI\n")
    pbism = json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
        "version": "4.2", "settings": {"qnaEnabled": True}}, indent=2)

    parts = []

    def add(path, content):
        b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
        parts.append({"path": path, "payload": b64, "payloadType": "InlineBase64"})

    add("definition.pbism", pbism)
    add("definition/database.tmdl", database_tmdl)
    add("definition/model.tmdl", model_tmdl)
    add("definition/relationships.tmdl", rel_tmdl)
    for mtable, (physical, cols) in TABLES.items():
        add("definition/tables/" + mtable + ".tmdl",
            _table_tmdl(mtable, physical, cols, MEASURES.get(mtable, [])))
    return parts


def _poll_operation(client, op_id, tries=60, delay=5):
    for _ in range(tries):
        st = client.get("/v1/operations/" + op_id).json().get("status")
        if st == "Succeeded":
            return True
        if st == "Failed":
            raise RuntimeError("Operation failed: " + op_id)
        time.sleep(delay)
    raise TimeoutError("Operation timed out: " + op_id)


def deploy_semantic_model(ws_id, lh_id):
    """Create or update the Direct Lake model, then reframe it. Returns model id."""
    import sempy.fabric as fabric
    client = fabric.FabricRestClient()
    parts = _build_parts(ws_id, lh_id)

    items = client.get("/v1/workspaces/%s/items?type=SemanticModel" % ws_id).json().get("value", [])
    existing = [i for i in items if i["displayName"] == MODEL_NAME]

    if existing:
        model_id = existing[0]["id"]
        resp = client.post("/v1/workspaces/%s/semanticModels/%s/updateDefinition" % (ws_id, model_id),
                           json={"definition": {"format": "TMDL", "parts": parts}})
    else:
        resp = client.post("/v1/workspaces/%s/semanticModels" % ws_id,
                           json={"displayName": MODEL_NAME,
                                 "definition": {"format": "TMDL", "parts": parts}})
    if resp.status_code in (200, 201):
        pass
    elif resp.status_code == 202:
        op_id = resp.headers.get("x-ms-operation-id")
        if op_id:
            _poll_operation(client, op_id)
    else:
        raise RuntimeError("Model deploy failed: %s %s" % (resp.status_code, resp.text[:500]))

    if not existing:
        for _ in range(30):
            items = client.get("/v1/workspaces/%s/items?type=SemanticModel" % ws_id).json().get("value", [])
            match = [i for i in items if i["displayName"] == MODEL_NAME]
            if match:
                model_id = match[0]["id"]
                break
            time.sleep(4)

    # Direct Lake reads via the lakehouse SQL analytics endpoint, whose metadata
    # sync lags table creation. Poll a metadata refresh and retry the reframe until
    # the endpoint has discovered the tables (can take several minutes on a fresh
    # workspace).
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
        status = None
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
    model_id = deploy_semantic_model(WORKSPACE_ID, LAKEHOUSE_ID)
    _rec("model", "ok", model_id)
except Exception:
    _rec("model", "error", traceback.format_exc()); _writelog(); raise
print("Semantic model ready:", model_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- 4) Create + publish the data agent ------------------------------------
"""Create, configure and publish the SAP Manufacturing data agent.

Runs inside a Fabric notebook. Installs fabric-data-agent-sdk into a temp target
(a %pip magic restarts the batch session and fails headless jobs) and prepends it
to sys.path so the newer sempy in the package wins for list_items(item_type=...).
"""
import os
import subprocess
import sys
import time
import traceback

AGENT_NAME = "SAP_Manufacturing_DataAgent"
SEMANTIC_MODEL = "SAP_Manufacturing_Model"

INSTRUCTIONS = r"""
You are a specialized SAP PP, production control, shopfloor and bottleneck analysis
agent for the demo "Talking to your SAP data - Manufacturing Edition".

TASK
You analyze production orders, operations, material availability, missing parts,
order load, capacities, confirmations, throughput, output, queue/idle times and
S&OP deviations in a data-driven and traceable way. You work exclusively on the
attached semantic model. Never make assumptions about non-existing fields or data.
If data is missing: name the missing fields, explain the limitation, and do not
speculate.

DATA SOURCES (GROUND TRUTH) - tables in the semantic model
Material master: MARA (general material data), MARC (plant/MRP data incl. DISPO),
  MARD (storage-location stock), D_Part (ABC/XYZ class, critical parts, standard
  price), MRP_Cntr (details for the DISPO field, MRP controller), ProductHierarchy,
  ProductGroup.
Production orders: COOIS_OrderHeaders (order headers), COOIS_OrderOperations
  (operations).
Work centers and capacities: WorkCenters (incl. bottleneck Laser Balancing),
  ActivityTypes.
Confirmations: COOIS_Confirmations (yield, scrap, rework, actual times, actual end
  ActualEnd, SAP posting time SAP_PostingTime).
Missing parts: CO24 (Reqmnt_qty, Comm_qty, Qty_issued, ShortageQuantity,
  MissingPart flag).
Material movements: MATDOC, MovementTypes.
S&OP: SOP_Targets (target quantity, actual quantity, deviation per
  ProductGroup/Period/Plant).
Stock: tmp_PartStockV1.

DATA PRIORITY (in case of conflict)
1. Material movements (MATDOC) 2. Confirmations (COOIS_Confirmations)
3. Order headers (COOIS_OrderHeaders) 4. Operations (COOIS_OrderOperations)
5. Stock (MARD) 6. CO24 7. S&OP. Actual data always takes precedence over plan data.

STATUS LOGIC PRODUCTION ORDERS (field StatusShort / SystemStatus)
CRTD=created, REL=released, PCNF=partially confirmed, CNF=confirmed,
DLV=delivered, TECO=technically completed, CLSD=closed.
An order counts as released when SystemStatus contains REL.

BUSINESS DEFINITIONS
Order load = all released production orders (SystemStatus contains REL).
  Measure: [Released Order Load].
Backlog = open orders with a scheduled finish date in the past (field Backlog = 1).
  Measure: [Backlog Order Count].
Missing part = exists when Reqmnt_qty > Comm_qty + Qty_issued at the requirement
  date; flagged via CO24[MissingPart] = 1. Measures: [Missing Part Count],
  [Total Shortage Quantity]. CO24[Order] is linked to COOIS_OrderHeaders[Order].
Material buffer before a work center = released orders with an operation on the
  work center in question whose operation is not yet CNF.

CAPACITY LOGIC
Theoretical capacity of a work center = (NGRAD/100) * EINZH * AZNOR (hours/day).
  Pre-computed in field WorkCenters[TheoreticalCapacityHrDay]. The bottleneck work
  center is Laser Balancing (WorkCenters[Bottleneck]=1); affected operations are
  flagged with COOIS_OrderOperations[BottleneckOperation]=1.

NUMBER LOGIC
Material and order numbers may contain leading zeros and are stored as text.
Consider leading zeros in comparisons and searches.

SHOPFLOOR / THROUGHPUT / IDLE-TIME LOGIC
Distinguish the real manufacturing event (COOIS_Confirmations[ActualEnd]) from the
SAP system event (COOIS_Confirmations[SAP_PostingTime]). The difference is stored in
DelayHours (measure [Avg Posting Delay Hours]) and is non-value-adding delay.
Compare real throughput (MATDOC) with reported throughput (confirmations).

S&OP / BOTTLENECK / OUTPUT
For prioritization questions: analyze S&OP targets (SOP_Targets, field SOP_Relevant
on the headers), released orders, backlog and missing parts. For bottleneck
questions: examine confirmations, material buffer, capacity, utilization, missing
parts and throughput at Laser Balancing. For output: yield, scrap, rework, labor and
machine times from COOIS_Confirmations.

STANDARD ANSWER FORMAT
1. Short answer  2. Analysis  3. Data basis  4. Cause / interpretation
5. Relevant tables and fields  6. Next reasonable analysis

STYLE
English, SAP terminology, factual, professional, traceable, no emojis, no slang.
Every statement must be based on data. No guesses. No hallucinations.
"""

DS_NOTES = (
    "Use the semantic model SAP_Manufacturing_Model. Key measures: "
    "[Order Count], [Released Order Load], [Backlog Order Count], "
    "[Missing Part Count], [Total Shortage Quantity], [Total Yield], [Total Scrap], "
    "[Total Rework], [Scrap Rate], [Avg Posting Delay Hours], "
    "[Bottleneck Operation Count (Laser Balancing)]. The bottleneck is the work "
    "center Laser Balancing (WorkCenters[Bottleneck]=1). Missing parts via "
    "CO24[MissingPart]=1, backlog via COOIS_OrderHeaders[Backlog]=1, order load via "
    "SystemStatus contains 'REL'. Order and material numbers have leading zeros."
)


def setup_agent():
    """Idempotently (re)create, configure and publish the data agent."""
    target = "/tmp/fdapkgs"
    os.makedirs(target, exist_ok=True)
    subprocess.run([sys.executable, "-m", "pip", "install", "--target", target,
                    "fabric-data-agent-sdk"], capture_output=True, text=True, timeout=1200)
    if target not in sys.path:
        sys.path.insert(0, target)

    # The data agent SDK needs its bundled (newer) sempy, whose list_items accepts
    # item_type=. If the runtime already imported an older sempy, drop it so the
    # target copy on sys.path[0] is imported fresh.
    for mod in [m for m in list(sys.modules) if m == "sempy" or m.startswith("sempy.")]:
        del sys.modules[mod]

    from fabric.dataagent.client import create_data_agent, delete_data_agent

    try:
        delete_data_agent(AGENT_NAME)
        print("Removed existing agent")
    except Exception:
        pass

    # After a delete the display name is briefly reserved
    # (ItemDisplayNameNotAvailableYet). Retry create until the name frees up.
    agent = None
    last_err = None
    for _attempt in range(12):
        try:
            agent = create_data_agent(AGENT_NAME)
            break
        except Exception as e:
            last_err = e
            if "NotAvailableYet" in str(e) or "409" in str(e):
                time.sleep(20)
                continue
            raise
    if agent is None:
        raise RuntimeError("Could not create data agent: " + str(last_err)[:400])

    agent.update_configuration(instructions=INSTRUCTIONS)
    agent.add_datasource(SEMANTIC_MODEL, type="semanticmodel")
    try:
        ds = agent.get_datasources()[0]
        ds.update_configuration(instructions=DS_NOTES)
    except Exception:
        print("Datasource notes skipped:", traceback.format_exc().splitlines()[-1])
    agent.publish()
    print("Data agent '%s' created and published." % AGENT_NAME)
    return AGENT_NAME


try:
    setup_agent()
    _rec("agent", "ok")
except Exception:
    _rec("agent", "error", traceback.format_exc()); _writelog(); raise
_writelog()
print("POST_DEPLOY_DONE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
