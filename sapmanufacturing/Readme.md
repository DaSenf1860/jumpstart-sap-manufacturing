# Talk to your SAP data - Manufacturing Edition

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
