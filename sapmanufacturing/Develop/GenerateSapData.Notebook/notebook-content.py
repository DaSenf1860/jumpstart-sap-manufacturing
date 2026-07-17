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

# # Generate SAP Manufacturing Data
#
# Writes 17 synthetic SAP tables (material master, production orders,
# operations, confirmations, missing parts, movements, S&OP) into the
# bound default lakehouse. Called by the PostDeploymentNotebook.

# CELL ********************

import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    TimestampType, DateType, LongType,
)

spark = SparkSession.builder.getOrCreate()
random.seed(42)

TODAY = datetime(2026, 7, 15, 7, 30, 0)

def pad(n, width=18):
    return str(n).zfill(width)

def d(days=0, hours=0):
    return TODAY + timedelta(days=days, hours=hours)

_ROLE_DESC = {
    "TURN": "CNC Turning", "MILL": "CNC Milling", "WIND": "Winding",
    "LASER": "Laser Balancing", "ASSY": "Assembly", "PAINT": "Painting",
    "INSP": "Final Inspection",
}
def role_desc(role):
    return _ROLE_DESC.get(role, role)

def write(name, rows, schema):
    idx_double = [i for i, f in enumerate(schema.fields)
                  if isinstance(f.dataType, DoubleType)]
    idx_int = [i for i, f in enumerate(schema.fields)
               if isinstance(f.dataType, (IntegerType, LongType))]
    coerced = []
    for r in rows:
        r = list(r)
        for i in idx_double:
            if r[i] is not None:
                r[i] = float(r[i])
        for i in idx_int:
            if r[i] is not None:
                r[i] = int(r[i])
        coerced.append(tuple(r))
    df = spark.createDataFrame(coerced, schema)
    (df.write.mode("overwrite").format("delta")
       .option("overwriteSchema", "true").saveAsTable(name.lower()))
    print(f"{name}: {df.count()} rows")

# ============================================================
# 1. REFERENCE / MASTER
# ============================================================
plants = {"1000": "Plant Hamburg", "2000": "Plant Munich"}

product_groups = [
    ("PG10", "Rotors"),
    ("PG20", "Shafts"),
    ("PG30", "Housings"),
    ("PG40", "Electric Motors"),
    ("PG50", "Bearing Shields"),
]
sch_pg = StructType([
    StructField("ProductGroup", StringType()),
    StructField("Description", StringType()),
])
write("ProductGroup", product_groups, sch_pg)

prod_hier = [
    ("RE00000000", "Rotating Equipment", 1, None),
    ("RE01000000", "Rotors", 2, "RE00000000"),
    ("RE02000000", "Shafts", 2, "RE00000000"),
    ("RE03000000", "Housings", 2, "RE00000000"),
    ("RE04000000", "Electric Motors", 2, "RE00000000"),
    ("RE05000000", "Bearing Shields", 2, "RE00000000"),
]
sch_ph = StructType([
    StructField("ProductHierarchy", StringType()),
    StructField("Description", StringType()),
    StructField("Level", IntegerType()),
    StructField("Parent", StringType()),
])
write("ProductHierarchy", prod_hier, sch_ph)

pg_hier = {"PG10": "RE01000000", "PG20": "RE02000000", "PG30": "RE03000000",
           "PG40": "RE04000000", "PG50": "RE05000000"}

# MRP controller (DISPO details for MARC.DISPO)
mrp_cntr = [
    ("101", "1000", "Miller", "Rotors and Shafts", "miller@demo.local"),
    ("102", "1000", "Smith", "Housings and Bearing Shields", "smith@demo.local"),
    ("103", "1000", "Webb", "Electric Motors", "webb@demo.local"),
    ("201", "2000", "Fisher", "Plant Munich Production", "fisher@demo.local"),
]
sch_mrp = StructType([
    StructField("DISPO", StringType()),
    StructField("WERKS", StringType()),
    StructField("Name", StringType()),
    StructField("ResponsibilityArea", StringType()),
    StructField("Email", StringType()),
])
write("MRP_Cntr", mrp_cntr, sch_mrp)

# Activity types
activity_types = [
    ("MACH", "Machine Time", "HR"),
    ("LABR", "Labor Time", "HR"),
    ("SETUP", "Setup Time", "HR"),
]
sch_la = StructType([
    StructField("ActivityType", StringType()),
    StructField("Description", StringType()),
    StructField("Unit", StringType()),
])
write("ActivityTypes", activity_types, sch_la)

# Movement types
movement_types = [
    ("101", "Goods receipt for production order", "+", "GR"),
    ("102", "Goods receipt reversal", "-", "GR"),
    ("261", "Goods issue for production order", "-", "GI"),
    ("262", "Goods issue reversal", "+", "GI"),
    ("531", "Goods receipt of by-product", "+", "GR"),
    ("311", "Transfer posting storage to storage", "0", "TR"),
]
sch_ba = StructType([
    StructField("MovementType", StringType()),
    StructField("Description", StringType()),
    StructField("Sign", StringType()),
    StructField("Direction", StringType()),
])
write("MovementTypes", movement_types, sch_ba)

# Work centers - includes bottleneck Laser Balancing
# WorkCenter, WERKS, Description, NGRAD(%), EINZH(hr/shift), AZNOR(#capacities),
#   CapacityCategory, Bottleneck
work_centers = [
    ("WC-TURN01", "1000", "CNC Turning", 85, 8.0, 2, "001", 0),
    ("WC-MILL01", "1000", "CNC Milling", 82, 8.0, 2, "001", 0),
    ("WC-WIND01", "1000", "Winding", 80, 8.0, 2, "001", 0),
    ("WC-LASER01", "1000", "Laser Balancing", 95, 8.0, 1, "001", 1),
    ("WC-ASSY01", "1000", "Assembly", 78, 8.0, 3, "001", 0),
    ("WC-PAINT01", "1000", "Painting", 70, 8.0, 1, "001", 0),
    ("WC-INSP01", "1000", "Final Inspection", 90, 8.0, 1, "001", 0),
    ("WC-TURN02", "2000", "CNC Turning", 84, 8.0, 2, "001", 0),
    ("WC-LASER02", "2000", "Laser Balancing", 93, 8.0, 1, "001", 1),
    ("WC-ASSY02", "2000", "Assembly", 76, 8.0, 2, "001", 0),
]
ap_hours = {}
sch_ap = StructType([
    StructField("WorkCenter", StringType()),
    StructField("WERKS", StringType()),
    StructField("Description", StringType()),
    StructField("NGRAD", IntegerType()),
    StructField("EINZH", DoubleType()),
    StructField("AZNOR", IntegerType()),
    StructField("CapacityCategory", StringType()),
    StructField("Bottleneck", IntegerType()),
    StructField("TheoreticalCapacityHrDay", DoubleType()),
])
ap_rows = []
for a in work_centers:
    cap = round(a[3] / 100.0 * a[4] * a[5], 2)
    ap_hours[a[0]] = cap
    ap_rows.append(a + (cap,))
write("WorkCenters", ap_rows, sch_ap)

# ============================================================
# 2. MATERIAL MASTER  MARA / MARC / MARD / D_Part / tmp_PartStockV1
# ============================================================
routings = {
    "PG10": ["TURN", "MILL", "LASER", "INSP"],   # Rotors -> Laser Balancing
    "PG20": ["TURN", "MILL", "INSP"],            # Shafts
    "PG30": ["MILL", "PAINT", "INSP"],           # Housings
    "PG40": ["WIND", "ASSY", "LASER", "INSP"],   # Electric Motors -> Laser Balancing
    "PG50": ["TURN", "INSP"],                    # Bearing Shields
}
pg_meta = {
    "PG10": ("FERT", "Rotor", "PC", 12.5, "RM01"),
    "PG20": ("HALB", "Shaft", "PC", 8.0, "RM02"),
    "PG30": ("HALB", "Housing", "PC", 25.0, "RM03"),
    "PG40": ("FERT", "Electric Motor", "PC", 45.0, "RM04"),
    "PG50": ("HALB", "Bearing Shield", "PC", 3.2, "RM05"),
}

mara_rows, marc_rows, mard_rows, dpart_rows, stock_rows = [], [], [], [], []
materials = []  # (matnr_raw, pg, plant, mtart, meins, name)
mid = 2000010000
for pg, _ in product_groups:
    mtart, name, meins, weight, matkl = pg_meta[pg]
    n_mats = random.randint(10, 13)
    for i in range(n_mats):
        mid += random.randint(1, 5)
        matnr = mid
        plant = "2000" if random.random() < 0.28 else "1000"
        if pg == "PG30":  # housings only in 1000 (has Painting)
            plant = "1000"
        dispo = {"1000": {"PG10": "101", "PG20": "101", "PG30": "102",
                          "PG40": "103", "PG50": "102"},
                 "2000": {k: "201" for k in pg_hier}}[plant][pg]
        beskz = "E" if mtart == "FERT" else "F"  # in-house
        matnr_raw = pad(matnr)
        materials.append((matnr_raw, pg, plant, mtart, meins, name))
        gewicht = round(weight * random.uniform(0.8, 1.2), 2)
        mara_rows.append((
            matnr_raw, mtart, matkl, meins, gewicht, "KG",
            pg_hier[pg], pg, f"{name} Type {chr(65+i)}{i:02d}",
        ))
        marc_rows.append((
            matnr_raw, plant, dispo, beskz, "PP01",
            random.choice([1, 2, 3, 5]),   # lot-size horizon (days)
            round(random.uniform(0.5, 3.0), 1),  # in-house production time (days)
        ))
        total_stock = 0
        for lgort in ["0001", "0002"]:
            qty = random.choice([0, 0, 5, 12, 30, 45, 80, 120])
            total_stock += qty
            mard_rows.append((matnr_raw, plant, lgort, float(qty),
                              float(random.choice([0, 0, 5, 10])),  # QI stock
                              d(days=-random.randint(1, 40)).date()))
        stock_rows.append((matnr_raw, plant, float(total_stock), meins,
                           d(hours=-random.randint(1, 12))))
        dpart_rows.append((
            matnr_raw, pg, f"{name} Type {chr(65+i)}{i:02d}",
            random.choice(["A", "A", "B", "C"]),      # ABC class
            random.choice(["X", "Y", "Z"]),           # XYZ class
            1 if random.random() < 0.35 else 0,       # critical part
            round(random.uniform(50, 2500), 2),       # standard price
        ))

sch_mara = StructType([
    StructField("MATNR", StringType()),
    StructField("MTART", StringType()),
    StructField("MATKL", StringType()),
    StructField("MEINS", StringType()),
    StructField("NTGEW", DoubleType()),
    StructField("GEWEI", StringType()),
    StructField("PRDHA", StringType()),
    StructField("ProductGroup", StringType()),
    StructField("MAKTX", StringType()),
])
write("MARA", mara_rows, sch_mara)

sch_marc = StructType([
    StructField("MATNR", StringType()),
    StructField("WERKS", StringType()),
    StructField("DISPO", StringType()),
    StructField("BESKZ", StringType()),
    StructField("FEVOR", StringType()),
    StructField("DZEIT", IntegerType()),
    StructField("InHouseProductionTimeDays", DoubleType()),
])
write("MARC", marc_rows, sch_marc)

sch_mard = StructType([
    StructField("MATNR", StringType()),
    StructField("WERKS", StringType()),
    StructField("LGORT", StringType()),
    StructField("LABST", DoubleType()),
    StructField("INSME", DoubleType()),
    StructField("StockDate", DateType()),
])
write("MARD", mard_rows, sch_mard)

sch_dpart = StructType([
    StructField("MATNR", StringType()),
    StructField("ProductGroup", StringType()),
    StructField("Description", StringType()),
    StructField("ABC_Class", StringType()),
    StructField("XYZ_Class", StringType()),
    StructField("CriticalPart", IntegerType()),
    StructField("StandardPrice", DoubleType()),
])
write("D_Part", dpart_rows, sch_dpart)

sch_stock = StructType([
    StructField("Part", StringType()),
    StructField("WERKS", StringType()),
    StructField("Stock", DoubleType()),
    StructField("Unit", StringType()),
    StructField("SnapshotTime", TimestampType()),
])
write("tmp_PartStockV1", stock_rows, sch_stock)

# ============================================================
# 3. PRODUCTION ORDERS  COOIS headers + operations
# ============================================================
role_wc = {
    "1000": {"TURN": "WC-TURN01", "MILL": "WC-MILL01", "WIND": "WC-WIND01",
             "LASER": "WC-LASER01", "ASSY": "WC-ASSY01", "PAINT": "WC-PAINT01",
             "INSP": "WC-INSP01"},
    "2000": {"TURN": "WC-TURN02", "MILL": "WC-TURN02", "WIND": "WC-ASSY02",
             "LASER": "WC-LASER02", "ASSY": "WC-ASSY02", "PAINT": "WC-PAINT01",
             "INSP": "WC-LASER02"},
}
fert_materials = [m for m in materials]

status_plan = (
    ["CRTD"] * 10 + ["REL"] * 34 + ["PCNF"] * 20 +
    ["CNF"] * 8 + ["DLV"] * 14 + ["TECO"] * 8 + ["CLSD"] * 6
)

order_headers, order_ops, confirmations = [], [], []
co24_rows, matdoc_rows = [], []

order_no = 1000200000
mblnr = 4900000000
rueck = 100000000
for oi in range(240):
    order_no += random.randint(1, 3)
    matnr_raw, pg, plant, mtart, meins, name = random.choice(fert_materials)
    order = pad(order_no)
    menge = random.choice([5, 10, 12, 20, 25, 40, 50, 80, 100])
    status = random.choice(status_plan)
    erdat = d(days=-random.randint(5, 90))
    start = erdat + timedelta(days=random.randint(1, 5))
    dauer = random.randint(3, 20)
    endtermin = start + timedelta(days=dauer)
    is_backlog = 0
    if status in ("REL", "PCNF", "CRTD") and endtermin < TODAY:
        is_backlog = 1
    if status in ("DLV", "TECO", "CLSD"):
        gut_gel = menge
    elif status == "CNF":
        gut_gel = menge
    elif status == "PCNF":
        gut_gel = int(menge * random.uniform(0.3, 0.7))
    else:
        gut_gel = 0
    sop_relevant = 1 if pg in ("PG10", "PG40") and random.random() < 0.7 else 0
    prio = random.choice([1, 1, 2, 2, 2, 3, 3])
    stat_map = {
        "CRTD": "CRTD", "REL": "REL", "PCNF": "REL PCNF", "CNF": "REL CNF",
        "DLV": "REL CNF DLV", "TECO": "REL CNF DLV TECO",
        "CLSD": "REL CNF DLV TECO CLSD",
    }
    order_headers.append((
        order, matnr_raw, name, plant, "PP01", float(menge), meins,
        stat_map[status], status, prio, sop_relevant, is_backlog,
        erdat.date(), start.date(), endtermin.date(), float(gut_gel),
        pg,
    ))

    route = routings[pg]
    n_ops = len(route)
    if status in ("CNF", "DLV", "TECO", "CLSD"):
        confirmed_ops = n_ops
    elif status == "PCNF":
        confirmed_ops = random.randint(1, n_ops - 1)
    elif status == "REL":
        confirmed_ops = random.randint(0, max(0, n_ops - 2))
    else:
        confirmed_ops = 0

    op_start = start
    for idx, role in enumerate(route):
        operation = f"{(idx+1)*10:04d}"
        wc = role_wc[plant][role]
        setup = round(random.uniform(0.3, 1.5), 2)
        mach = round(menge * random.uniform(0.08, 0.4), 2)
        labor = round(mach * random.uniform(0.4, 0.9), 2)
        op_dauer = max(0.5, (setup + mach) / 8.0)
        op_end = op_start + timedelta(days=op_dauer)
        if idx < confirmed_ops:
            op_status = "CNF"
            op_sys = "REL CNF"
        elif idx == confirmed_ops and status in ("PCNF", "REL") and confirmed_ops < n_ops:
            op_status = "PCNF" if status == "PCNF" else "REL"
            op_sys = "REL PCNF" if status == "PCNF" else "REL"
        elif status == "CRTD":
            op_status = "CRTD"
            op_sys = "CRTD"
        else:
            op_status = "REL"
            op_sys = "REL"
        istmenge = menge if op_status == "CNF" else (
            int(menge * random.uniform(0.3, 0.7)) if op_status == "PCNF" else 0)
        order_ops.append((
            order, operation, wc, plant, role_desc(role), op_status, op_sys,
            "PP01", float(menge), float(istmenge), setup, mach, labor,
            op_start.date(), op_end.date(), matnr_raw, pg,
            1 if role == "LASER" else 0,
        ))

        if op_status in ("CNF", "PCNF"):
            gut = istmenge
            scrap = 0
            rework = 0
            if random.random() < 0.35:
                scrap = max(1, int(gut * random.uniform(0.01, 0.08)))
            if random.random() < 0.25:
                rework = max(1, int(gut * random.uniform(0.01, 0.05)))
            real_ende = op_end + timedelta(hours=random.uniform(-6, 6))
            delay_h = random.choice([0.2, 0.5, 1, 2, 4, 8, 24, 48])
            sap_buchung = real_ende + timedelta(hours=delay_h)
            rueck += 1
            confirmations.append((
                pad(rueck, 12), order, operation, wc, plant,
                float(gut), float(scrap), float(rework),
                round(mach * random.uniform(0.85, 1.25), 2),
                round(labor * random.uniform(0.85, 1.25), 2),
                real_ende, sap_buchung, round(delay_h, 2),
                0, matnr_raw,
            ))
        op_start = op_end + timedelta(hours=random.uniform(2, 60))

    # ---- CO24 missing parts (released orders only) ----
    if status in ("REL", "PCNF"):
        n_comp = random.randint(1, 3)
        for c in range(n_comp):
            comp = random.choice(materials)
            comp_matnr = comp[0]
            reqmnt = round(menge * random.uniform(0.5, 2.0), 1)
            if random.random() < 0.45:
                comm = round(reqmnt * random.uniform(0.0, 0.6), 1)
                issued = round(reqmnt * random.uniform(0.0, 0.3), 1)
            else:
                comm = reqmnt
                issued = round(reqmnt * random.uniform(0.0, 0.5), 1)
            shortage = round(max(0.0, reqmnt - comm - issued), 1)
            bedarf = start + timedelta(days=random.randint(-3, 5))
            co24_rows.append((
                order, comp_matnr, plant, "0001", reqmnt, comm, issued,
                shortage, 1 if shortage > 0 else 0, bedarf.date(), comp[5],
            ))

    # ---- MATDOC movements ----
    if status in ("PCNF", "CNF", "DLV", "TECO", "CLSD"):
        for c in range(random.randint(1, 3)):
            comp = random.choice(materials)
            mblnr += 1
            matdoc_rows.append((
                pad(mblnr, 10), "0001", comp[0], plant, "0001", "261",
                round(menge * random.uniform(0.3, 1.2), 2), comp[4],
                (start + timedelta(days=random.randint(0, 4))).date(),
                (start + timedelta(days=random.randint(0, 4))).date(),
                order, "CC100",
            ))
    if status in ("DLV", "TECO", "CLSD"):
        mblnr += 1
        matdoc_rows.append((
            pad(mblnr, 10), "0001", matnr_raw, plant, "0001", "101",
            float(gut_gel), meins,
            endtermin.date(), endtermin.date(), order, "CC100",
        ))

sch_hdr = StructType([
    StructField("Order", StringType()),
    StructField("Material", StringType()),
    StructField("MaterialText", StringType()),
    StructField("WERKS", StringType()),
    StructField("OrderType", StringType()),
    StructField("OrderQuantity", DoubleType()),
    StructField("Unit", StringType()),
    StructField("SystemStatus", StringType()),
    StructField("StatusShort", StringType()),
    StructField("Priority", IntegerType()),
    StructField("SOP_Relevant", IntegerType()),
    StructField("Backlog", IntegerType()),
    StructField("CreationDate", DateType()),
    StructField("StartDate", DateType()),
    StructField("FinishDate", DateType()),
    StructField("DeliveredQuantity", DoubleType()),
    StructField("ProductGroup", StringType()),
])
write("COOIS_OrderHeaders", order_headers, sch_hdr)

sch_op = StructType([
    StructField("Order", StringType()),
    StructField("Operation", StringType()),
    StructField("WorkCenter", StringType()),
    StructField("WERKS", StringType()),
    StructField("OperationText", StringType()),
    StructField("StatusShort", StringType()),
    StructField("SystemStatus", StringType()),
    StructField("ControlKey", StringType()),
    StructField("TargetQuantity", DoubleType()),
    StructField("ActualQuantity", DoubleType()),
    StructField("SetupTime", DoubleType()),
    StructField("MachineTime", DoubleType()),
    StructField("LaborTime", DoubleType()),
    StructField("StartDate", DateType()),
    StructField("FinishDate", DateType()),
    StructField("Material", StringType()),
    StructField("ProductGroup", StringType()),
    StructField("BottleneckOperation", IntegerType()),
])
write("COOIS_OrderOperations", order_ops, sch_op)

sch_conf = StructType([
    StructField("Confirmation", StringType()),
    StructField("Order", StringType()),
    StructField("Operation", StringType()),
    StructField("WorkCenter", StringType()),
    StructField("WERKS", StringType()),
    StructField("YieldQuantity", DoubleType()),
    StructField("ScrapQuantity", DoubleType()),
    StructField("ReworkQuantity", DoubleType()),
    StructField("ActualMachineTime", DoubleType()),
    StructField("ActualLaborTime", DoubleType()),
    StructField("ActualEnd", TimestampType()),
    StructField("SAP_PostingTime", TimestampType()),
    StructField("DelayHours", DoubleType()),
    StructField("Cancelled", IntegerType()),
    StructField("Material", StringType()),
])
write("COOIS_Confirmations", confirmations, sch_conf)

sch_co24 = StructType([
    StructField("Order", StringType()),
    StructField("Component", StringType()),
    StructField("WERKS", StringType()),
    StructField("LGORT", StringType()),
    StructField("Reqmnt_qty", DoubleType()),
    StructField("Comm_qty", DoubleType()),
    StructField("Qty_issued", DoubleType()),
    StructField("ShortageQuantity", DoubleType()),
    StructField("MissingPart", IntegerType()),
    StructField("RequirementDate", DateType()),
    StructField("ComponentText", StringType()),
])
write("CO24", co24_rows, sch_co24)

sch_md = StructType([
    StructField("Mblnr", StringType()),
    StructField("Line", StringType()),
    StructField("Matnr", StringType()),
    StructField("WERKS", StringType()),
    StructField("LGORT", StringType()),
    StructField("MovementType", StringType()),
    StructField("Quantity", DoubleType()),
    StructField("Unit", StringType()),
    StructField("PostingDate", DateType()),
    StructField("DocumentDate", DateType()),
    StructField("Order", StringType()),
    StructField("CostCenter", StringType()),
])
write("MATDOC", matdoc_rows, sch_md)

# ============================================================
# 4. S&OP TARGETS
# ============================================================
sop_rows = []
months = []
base = datetime(2026, 4, 1)
for k in range(6):
    m = base + timedelta(days=31 * k)
    months.append(f"{m.year}{m.month:02d}")
for pg, pgname in product_groups:
    for per in months:
        for plant in ["1000", "2000"]:
            target = random.choice([120, 180, 240, 300, 360, 450])
            ist = int(target * random.uniform(0.6, 1.15))
            sop_rows.append((pg, pgname, plant, per, float(target),
                             float(ist), float(round(ist - target, 1)), "V01"))
sch_sop = StructType([
    StructField("ProductGroup", StringType()),
    StructField("Description", StringType()),
    StructField("WERKS", StringType()),
    StructField("Period", StringType()),
    StructField("TargetQuantity", DoubleType()),
    StructField("ActualQuantity", DoubleType()),
    StructField("Deviation", DoubleType()),
    StructField("Version", StringType()),
])
write("SOP_Targets", sop_rows, sch_sop)

print("ALL TABLES WRITTEN")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
