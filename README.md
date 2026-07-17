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
| `PostDeploymentNotebook` | Notebook | **Entry point** — orchestrates the build |
| `SAP_Manufacturing_Model` | Semantic model | Direct Lake model + SAP measures (built at runtime) |
| `SAP_Manufacturing_DataAgent` | Data Agent | Grounded SAP PP analysis agent (built at runtime) |

The semantic model and data agent are built by the PostDeploymentNotebook at
runtime (the Direct Lake connection needs the deployed lakehouse id), so the
jumpstart is portable across workspaces with no ID substitution.

## Data model

Material master (`MARA`, `MARC`, `MARD`, `D_Part`, `MRP_Cntr`, `ProductHierarchy`,
`ProductGroup`), production orders (`COOIS_OrderHeaders`, `COOIS_OrderOperations`),
confirmations (`COOIS_Confirmations`), missing parts (`CO24`), material movements
(`MATDOC`, `MovementTypes`), work centers incl. the **Laser Balancing** bottleneck
(`WorkCenters`, `ActivityTypes`) and S&OP (`SOP_Targets`, `tmp_PartStockV1`).

## Repository layout

```
build_jumpstart.py          # generator — assembles the item tree from ./src
deploy.py                   # standalone fabric-cicd deploy
src/                        # source modules inlined into the notebooks
  gen_sap_data.py           #   data synthesizer
  model_builder.py          #   Direct Lake TMDL builder + deploy/refresh
  agent_setup.py            #   data agent create/configure/publish
sapmanufacturing/           # GENERATED deployable tree (fabric-cicd git format)
  Lakehouse/…  Develop/…  parameter.yml  Readme.md
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
