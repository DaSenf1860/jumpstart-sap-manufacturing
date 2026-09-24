# Talk to your SAP data — Manufacturing Edition (Fabric Jumpstart)

A Microsoft Fabric jumpstart that deploys a **conversational analytics** demo over
synthetic **SAP PP / manufacturing** data. Ask natural-language questions about
production orders, bottlenecks, missing parts, scrap and S&OP — answered by a
grounded Fabric **Data Agent** on top of a **Direct Lake semantic model**.

## What gets deployed

| Item | Type | Purpose |
|------|------|---------|
| `SAP_Manufacturing_LH` | Lakehouse | 17 synthetic SAP Delta tables |
| `GenerateSapData` | Notebook | Synthesizes the tables |
| `SAP_Manufacturing_Model` | Semantic model | Direct Lake model + SAP measures |
| `SAP_Manufacturing_DataAgent` | Data Agent | Grounded SAP PP analysis agent |
| `PostDeploymentNotebook` | Notebook | **Entry point** — seeds data + reframes the model |

All five items are deployed by **fabric-cicd** from the git tree. The semantic
model and data agent carry placeholder ids (`__WORKSPACE_ID__`,
`__LAKEHOUSE_ID__`, `__SEMANTIC_MODEL_ID__`) that `parameter.yml` resolves to the
deployed lakehouse/model at deploy time — so the jumpstart stays portable across
workspaces with no manual id edits. The PostDeploymentNotebook only seeds the SAP
data and reframes the Direct Lake model (which needs the *live* lakehouse tables).

## Data model

Material master (`MARA`, `MARC`, `MARD`, `D_Part`, `MRP_Cntr`, `ProductHierarchy`,
`ProductGroup`), production orders (`COOIS_OrderHeaders`, `COOIS_OrderOperations`),
confirmations (`COOIS_Confirmations`), missing parts (`CO24`), material movements
(`MATDOC`, `MovementTypes`), work centers incl. the **Laser Balancing** bottleneck
(`WorkCenters`, `ActivityTypes`) and S&OP (`SOP_Targets`, `tmp_PartStockV1`).

## Repository layout

```
build_jumpstart.py          # generator — assembles the item tree from ./src
extract_definitions.py      # one-off — captured the SM/DA git defs into ./src
deploy.py                   # standalone fabric-cicd deploy
src/                        # source artifacts assembled into the tree
  gen_sap_data.py           #   data synthesizer (inlined into GenerateSapData)
  refresh_model.py          #   runtime Direct Lake reframe (inlined into PostDeployment)
  semantic_model/           #   parameterized Direct Lake TMDL definition
  data_agent/               #   parameterized data agent definition
sapmanufacturing/           # GENERATED deployable tree (fabric-cicd git format)
  Lakehouse/…  Develop/…  SemanticModel/…  DataAgent/…  parameter.yml  Readme.md
```

Regenerate the tree after editing `src/`:

```bash
python build_jumpstart.py
```

## Deploy

1. `pip install fabric-cicd azure-identity` and `az login`.
2. Set `WORKSPACE_ID` in `deploy.py`.
3. `python deploy.py`.
4. In the workspace, open **PostDeploymentNotebook** → **Run all**.
5. Open **SAP_Manufacturing_DataAgent** and start asking questions:
   - *How many production orders are released, and how many are in backlog?*
   - *How many operations sit at the Laser Balancing bottleneck?*
   - *Which orders are waiting on missing parts and what is the total shortage?*
   - *Which product group has the highest scrap rate?*
   - *What is the average posting delay between actual completion and SAP posting?*

## Requirements

A Fabric capacity (F2+ / Trial) with Data Agents enabled in the tenant.
