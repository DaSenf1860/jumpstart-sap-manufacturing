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
