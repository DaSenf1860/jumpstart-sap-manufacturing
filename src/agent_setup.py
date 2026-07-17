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
