# Talk to your SAP data - Manufacturing Edition

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
