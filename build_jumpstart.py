"""Generate the Fabric jumpstart item tree (fabric-cicd git format).

Assembles a deployable workspace tree under ./sapmanufacturing from the source
modules in ./src. Notebook definitions use the Fabric notebook-content.py format.

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


def _param_cell(src):
    meta = ('\n# METADATA ********************\n\n'
            '# META {\n'
            '# META   "language": "python",\n'
            '# META   "language_group": "synapse_pyspark"\n'
            '# META }\n')
    return '\n# PARAMETERS CELL ********************\n\n' + src.rstrip("\n") + '\n' + meta


def build_notebook(cells, dependencies=None):
    return _nb_header(dependencies) + "".join(cells)


def write_item(rel_dir, item_type, display, description, nb_cells=None, dependencies=None):
    d = OUT / rel_dir / ("%s.%s" % (display, item_type))
    d.mkdir(parents=True, exist_ok=True)
    (d / ".platform").write_text(platform(item_type, display, description), encoding="utf-8")
    if nb_cells is not None:
        (d / "notebook-content.py").write_text(build_notebook(nb_cells, dependencies), encoding="utf-8")
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

    # ---- PostDeploymentNotebook (entry point) ----
    intro = (
        "# Post-Deployment - build the demo\n\n"
        "Click **Run all**. This notebook:\n"
        "1. Discovers the deployed workspace and lakehouse.\n"
        "2. Runs **GenerateSapData** to create the 17 SAP Delta tables.\n"
        "3. Builds the **SAP_Manufacturing_Model** Direct Lake semantic model and\n"
        "   reframes it.\n"
        "4. Creates and publishes the **SAP_Manufacturing_DataAgent** grounded on\n"
        "   that model.\n\n"
        "When it finishes, open the data agent and ask e.g. *\"How many orders are\n"
        "released and how many are in backlog?\"* or *\"How many operations sit at the\n"
        "Laser Balancing bottleneck?\"*")

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

    model_cell = (
        "# --- 3) Build + reframe the Direct Lake semantic model ---------------------\n"
        + read_src("model_builder.py")
        + "\n\ntry:\n"
        "    model_id = deploy_semantic_model(WORKSPACE_ID, LAKEHOUSE_ID)\n"
        "    _rec(\"model\", \"ok\", model_id)\n"
        "except Exception:\n"
        "    _rec(\"model\", \"error\", traceback.format_exc()); _writelog(); raise\n"
        "print(\"Semantic model ready:\", model_id)")

    agent_cell = (
        "# --- 4) Create + publish the data agent ------------------------------------\n"
        + read_src("agent_setup.py")
        + "\n\ntry:\n"
        "    setup_agent()\n"
        "    _rec(\"agent\", \"ok\")\n"
        "except Exception:\n"
        "    _rec(\"agent\", \"error\", traceback.format_exc()); _writelog(); raise\n"
        "_writelog()\n"
        "print(\"POST_DEPLOY_DONE\")")

    write_item("Develop", "Notebook", "PostDeploymentNotebook",
               "Entry point: seed data, build model, publish the data agent.",
               nb_cells=[_md_cell(intro), _code_cell(discover), _code_cell(run_gen),
                         _code_cell(model_cell), _code_cell(agent_cell)],
               dependencies=LAKEHOUSE_DEP)

    # ---- parameter.yml: bind the deployed lakehouse into GenerateSapData ----
    (OUT / "parameter.yml").write_text(
        "find_replace:\n"
        "  # Bind the GenerateSapData notebook's default lakehouse to the deployed one.\n"
        "  - find_value: \"__LAKEHOUSE_ID__\"\n"
        "    replace_value:\n"
        "      _ALL_: \"$items.Lakehouse.SAP_Manufacturing_LH.$id\"\n"
        "    item_type: \"Notebook\"\n"
        "  - find_value: \"__WORKSPACE_ID__\"\n"
        "    replace_value:\n"
        "      _ALL_: \"$workspace.$id\"\n"
        "    item_type: \"Notebook\"\n", encoding="utf-8")

    # ---- Readme.md (workspace-level) ----
    (OUT / "Readme.md").write_text(WORKSPACE_README, encoding="utf-8")

    print("Generated tree at:", OUT)
    for p in sorted(OUT.rglob("*")):
        if p.is_file():
            print("  ", p.relative_to(OUT))


WORKSPACE_README = """# Talk to your SAP data - Manufacturing Edition

A Fabric jumpstart that lets you ask natural-language questions about SAP
production data. It deploys:

- **SAP_Manufacturing_LH** - a lakehouse with 17 synthetic SAP tables
  (MARA/MARC/MARD, COOIS order headers & operations, confirmations, CO24
  missing parts, MATDOC movements, work centers incl. the *Laser Balancing*
  bottleneck, and S&OP targets).
- **GenerateSapData** - notebook that synthesizes the Delta tables.
- **SAP_Manufacturing_Model** - a Direct Lake semantic model with SAP business
  measures (order load, backlog, missing parts, scrap rate, bottleneck load,
  posting delay).
- **SAP_Manufacturing_DataAgent** - a data agent grounded on the model with SAP
  PP / shopfloor / bottleneck-analysis instructions.

## Getting started

Run the **PostDeploymentNotebook** (Run all). It seeds the data, builds and
reframes the semantic model, and publishes the data agent. Then open the data
agent and try:

- *How many production orders are released, and how many are in backlog?*
- *How many operations sit at the Laser Balancing bottleneck?*
- *Which orders are waiting on missing parts and what is the total shortage?*
- *Which product group has the highest scrap rate?*
- *What is the average posting delay between actual completion and SAP posting?*
"""


if __name__ == "__main__":
    main()
