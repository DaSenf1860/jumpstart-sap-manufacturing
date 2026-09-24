"""Generate the Fabric jumpstart item tree (fabric-cicd git format).

Assembles a deployable workspace tree under ./sapmanufacturing from the source
modules in ./src. The Lakehouse, GenerateSapData + PostDeployment notebooks, the
**SemanticModel** and the **DataAgent** are all deployed by fabric-cicd from the
git tree. The SemanticModel + DataAgent definitions live as parameterized template
trees under src/ (see extract_definitions.py); their workspace/lakehouse/model
GUIDs are placeholders that parameter.yml resolves at deploy time.

The PostDeploymentNotebook no longer *creates* the model or agent - it only seeds
the SAP data and reframes the (already-deployed) Direct Lake model.

Run:  python build_jumpstart.py
"""
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
OUT = ROOT / "sapmanufacturing"

# Stable logical ids (do NOT regenerate per run -> keeps item identity stable).
LOGICAL_IDS = {
    "SAP_Manufacturing_LH.Lakehouse": "a1f0c0de-0001-4a00-9000-5a70da7a0001",
    "GenerateSapData.Notebook": "a1f0c0de-0002-4a00-9000-5a70da7a0002",
    "PostDeploymentNotebook.Notebook": "a1f0c0de-0003-4a00-9000-5a70da7a0003",
    "SAP_Manufacturing_Model.SemanticModel": "a1f0c0de-0004-4a00-9000-5a70da7a0004",
    "SAP_Manufacturing_DataAgent.DataAgent": "a1f0c0de-0005-4a00-9000-5a70da7a0005",
}

PLATFORM_SCHEMA = ("https://developer.microsoft.com/json-schemas/fabric/"
                   "gitIntegration/platformProperties/2.0.0/schema.json")


def platform(item_type, display, description=""):
    key = "%s.%s" % (display, item_type)
    return json.dumps({
        "$schema": PLATFORM_SCHEMA,
        "metadata": {"type": item_type, "displayName": display,
                     **({"description": description} if description else {})},
        "config": {"version": "2.0", "logicalId": LOGICAL_IDS[key]},
    }, indent=2)


def _nb_header(dependencies=None):
    meta = {"kernel_info": {"name": "synapse_pyspark"}, "dependencies": dependencies or {}}
    meta_json = json.dumps(meta, indent=2)
    meta_block = "\n".join("# META " + line for line in meta_json.splitlines())
    return ('# Fabric notebook source\n\n'
            '# METADATA ********************\n\n'
            + meta_block + '\n')


LAKEHOUSE_DEP = {
    "lakehouse": {
        "default_lakehouse": "__LAKEHOUSE_ID__",
        "default_lakehouse_name": "SAP_Manufacturing_LH",
        "default_lakehouse_workspace_id": "__WORKSPACE_ID__",
    }
}


def _code_cell(src):
    meta = ('\n# METADATA ********************\n\n'
            '# META {\n'
            '# META   "language": "python",\n'
            '# META   "language_group": "synapse_pyspark"\n'
            '# META }\n')
    return '\n# CELL ********************\n\n' + src.rstrip("\n") + '\n' + meta


def _md_cell(md):
    body = "\n".join(("# " + line).rstrip() for line in md.splitlines())
    return '\n# MARKDOWN ********************\n\n' + body + '\n'


def build_notebook(cells, dependencies=None):
    return _nb_header(dependencies) + "".join(cells)


def write_item(rel_dir, item_type, display, description, nb_cells=None, dependencies=None):
    d = OUT / rel_dir / ("%s.%s" % (display, item_type))
    d.mkdir(parents=True, exist_ok=True)
    (d / ".platform").write_text(platform(item_type, display, description), encoding="utf-8")
    if nb_cells is not None:
        (d / "notebook-content.py").write_text(build_notebook(nb_cells, dependencies), encoding="utf-8")
    return d


def write_definition_item(rel_dir, item_type, display, description, src_tree):
    """Emit a definition-backed item (SemanticModel/DataAgent): a .platform plus a
    verbatim copy of the parameterized definition tree under src/."""
    d = OUT / rel_dir / ("%s.%s" % (display, item_type))
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True, exist_ok=True)
    (d / ".platform").write_text(platform(item_type, display, description), encoding="utf-8")
    for f in sorted((SRC / src_tree).rglob("*")):
        if f.is_file():
            rel = f.relative_to(SRC / src_tree)
            dest = d / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(f.read_text(encoding="utf-8"), encoding="utf-8")
    return d


def read_src(name):
    return (SRC / name).read_text(encoding="utf-8")


def main():
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)

    # ---- Lakehouse (definition-less item) ----
    write_item("Lakehouse", "Lakehouse", "SAP_Manufacturing_LH",
               "Synthetic SAP PP / shopfloor manufacturing data (Delta tables).")

    # ---- GenerateSapData notebook ----
    gen_md = ("# Generate SAP Manufacturing Data\n\n"
              "Writes 17 synthetic SAP tables (material master, production orders,\n"
              "operations, confirmations, missing parts, movements, S&OP) into the\n"
              "bound default lakehouse. Called by the PostDeploymentNotebook.")
    write_item("Develop", "Notebook", "GenerateSapData",
               "Synthesize SAP manufacturing Delta tables.",
               nb_cells=[_md_cell(gen_md), _code_cell(read_src("gen_sap_data.py"))],
               dependencies=LAKEHOUSE_DEP)

    # ---- SemanticModel (Direct Lake) - deployed by fabric-cicd ----
    write_definition_item("SemanticModel", "SemanticModel", "SAP_Manufacturing_Model",
                          "Direct Lake model over the SAP lakehouse with SAP PP measures.",
                          "semantic_model")

    # ---- DataAgent - deployed by fabric-cicd, grounded on the model ----
    write_definition_item("DataAgent", "DataAgent", "SAP_Manufacturing_DataAgent",
                          "SAP PP / shopfloor / bottleneck analysis agent on the model.",
                          "data_agent")

    # ---- PostDeploymentNotebook (entry point) - seed data + reframe model ----
    intro = (
        "# Post-Deployment - seed data & reframe the model\n\n"
        "Click **Run all**. The Lakehouse, GenerateSapData notebook, the\n"
        "**SAP_Manufacturing_Model** Direct Lake semantic model and the\n"
        "**SAP_Manufacturing_DataAgent** are all deployed for you by fabric-cicd.\n\n"
        "This notebook completes the runtime steps that need the *live* lakehouse:\n"
        "1. Discovers the deployed workspace and lakehouse.\n"
        "2. Runs **GenerateSapData** to create the 17 SAP Delta tables.\n"
        "3. Reframes the Direct Lake model so it picks up the freshly written tables.\n\n"
        "When it finishes, open **SAP_Manufacturing_DataAgent** and ask e.g. *\"How\n"
        "many orders are released and how many are in backlog?\"* or *\"How many\n"
        "operations sit at the Laser Balancing bottleneck?\"*")

    discover = (
        "# --- 1) Discover workspace + lakehouse -------------------------------------\n"
        "import traceback\n"
        "import notebookutils\n"
        "import sempy.fabric as fabric\n"
        "from pyspark.sql.types import StructType, StructField, StringType\n\n"
        "ctx = notebookutils.runtime.context\n"
        "WORKSPACE_ID = ctx.get(\"currentWorkspaceId\") or ctx.get(\"workspaceId\")\n"
        "client = fabric.FabricRestClient()\n"
        "items = client.get(f\"/v1/workspaces/{WORKSPACE_ID}/items?type=Lakehouse\").json()[\"value\"]\n"
        "match = [i for i in items if i[\"displayName\"] == \"SAP_Manufacturing_LH\"]\n"
        "if not match:\n"
        "    raise RuntimeError(\"Lakehouse SAP_Manufacturing_LH not found in workspace.\")\n"
        "LAKEHOUSE_ID = match[0][\"id\"]\n"
        "print(\"Workspace:\", WORKSPACE_ID)\n"
        "print(\"Lakehouse:\", LAKEHOUSE_ID)\n\n"
        "_log = []\n"
        "def _rec(step, status, detail=\"\"):\n"
        "    _log.append((step, status, str(detail)[:5000]))\n"
        "    print(step, \"->\", status)\n"
        "def _writelog():\n"
        "    try:\n"
        "        sch = StructType([StructField(\"step\", StringType()),\n"
        "                          StructField(\"status\", StringType()),\n"
        "                          StructField(\"detail\", StringType())])\n"
        "        p = (\"abfss://\" + WORKSPACE_ID + \"@onelake.dfs.fabric.microsoft.com/\"\n"
        "             + LAKEHOUSE_ID + \"/Tables/postdeploy_log\")\n"
        "        spark.createDataFrame(_log or [(\"none\",\"none\",\"none\")], sch) \\\n"
        "            .write.mode(\"overwrite\").option(\"overwriteSchema\", \"true\").save(p)\n"
        "    except Exception as e:\n"
        "        print(\"log write failed:\", e)\n"
        "_rec(\"discover\", \"ok\", LAKEHOUSE_ID)")

    run_gen = (
        "# --- 2) Generate the SAP data ----------------------------------------------\n"
        "try:\n"
        "    notebookutils.notebook.run(\"GenerateSapData\", 1800)\n"
        "    _rec(\"generate_data\", \"ok\")\n"
        "except Exception:\n"
        "    _rec(\"generate_data\", \"error\", traceback.format_exc()); _writelog(); raise\n"
        "print(\"SAP tables generated.\")")

    refresh_cell = (
        "# --- 3) Reframe the deployed Direct Lake semantic model --------------------\n"
        + read_src("refresh_model.py")
        + "\n\ntry:\n"
        "    model_id = refresh_semantic_model(WORKSPACE_ID, LAKEHOUSE_ID)\n"
        "    _rec(\"model_refresh\", \"ok\", model_id)\n"
        "except Exception:\n"
        "    _rec(\"model_refresh\", \"error\", traceback.format_exc()); _writelog(); raise\n"
        "_writelog()\n"
        "print(\"Semantic model reframed:\", model_id)\n"
        "print(\"POST_DEPLOY_DONE\")")

    write_item("Develop", "Notebook", "PostDeploymentNotebook",
               "Entry point: seed data and reframe the deployed Direct Lake model.",
               nb_cells=[_md_cell(intro), _code_cell(discover), _code_cell(run_gen),
                         _code_cell(refresh_cell)],
               dependencies=LAKEHOUSE_DEP)

    # ---- parameter.yml: bind deployed ids into notebooks, model and agent ----
    (OUT / "parameter.yml").write_text(PARAMETER_YML, encoding="utf-8")

    # ---- Readme.md (workspace-level) ----
    (OUT / "Readme.md").write_text(WORKSPACE_README, encoding="utf-8")

    print("Generated tree at:", OUT)
    for p in sorted(OUT.rglob("*")):
        if p.is_file():
            print("  ", p.relative_to(OUT))


PARAMETER_YML = """find_replace:
  # --- Notebooks: bind the default lakehouse to the deployed one -------------
  - find_value: "__LAKEHOUSE_ID__"
    replace_value:
      _ALL_: "$items.Lakehouse.SAP_Manufacturing_LH.$id"
    item_type: "Notebook"
  - find_value: "__WORKSPACE_ID__"
    replace_value:
      _ALL_: "$workspace.$id"
    item_type: "Notebook"

  # --- SemanticModel: bind the Direct Lake expression to the deployed lakehouse
  - find_value: "__LAKEHOUSE_ID__"
    replace_value:
      _ALL_: "$items.Lakehouse.SAP_Manufacturing_LH.$id"
    item_type: "SemanticModel"
  - find_value: "__WORKSPACE_ID__"
    replace_value:
      _ALL_: "$workspace.$id"
    item_type: "SemanticModel"

  # --- DataAgent: bind the datasource to the deployed model + workspace -------
  - find_value: "__SEMANTIC_MODEL_ID__"
    replace_value:
      _ALL_: "$items.SemanticModel.SAP_Manufacturing_Model.$id"
    item_type: "DataAgent"
  - find_value: "__WORKSPACE_ID__"
    replace_value:
      _ALL_: "$workspace.$id"
    item_type: "DataAgent"
"""


WORKSPACE_README = """# Talk to your SAP data - Manufacturing Edition

A Fabric jumpstart that lets you ask natural-language questions about SAP
production data. fabric-cicd deploys the whole item tree:

- **SAP_Manufacturing_LH** - a lakehouse for 17 synthetic SAP tables
  (MARA/MARC/MARD, COOIS order headers & operations, confirmations, CO24
  missing parts, MATDOC movements, work centers incl. the *Laser Balancing*
  bottleneck, and S&OP targets).
- **GenerateSapData** - notebook that synthesizes the Delta tables.
- **SAP_Manufacturing_Model** - a Direct Lake semantic model with SAP business
  measures (order load, backlog, missing parts, scrap rate, bottleneck load,
  posting delay). Bound to the deployed lakehouse via parameter.yml.
- **SAP_Manufacturing_DataAgent** - a data agent grounded on the model with SAP
  PP / shopfloor / bottleneck-analysis instructions. Bound to the deployed
  model via parameter.yml.
- **PostDeploymentNotebook** - entry point.

## Getting started

Run the **PostDeploymentNotebook** (Run all). It seeds the SAP data and reframes
the Direct Lake model so it picks up the freshly written tables. Then open the
data agent and try:

- *How many production orders are released, and how many are in backlog?*
- *How many operations sit at the Laser Balancing bottleneck?*
- *Which orders are waiting on missing parts and what is the total shortage?*
- *Which product group has the highest scrap rate?*
- *What is the average posting delay between actual completion and SAP posting?*
"""


if __name__ == "__main__":
    main()
