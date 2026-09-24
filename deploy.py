"""Standalone deploy of the SAP Manufacturing jumpstart with fabric-cicd.

Deploys the whole item tree under ./sapmanufacturing into a target workspace -
lakehouse, notebooks, the Direct Lake **semantic model** and the **data agent**.
parameter.yml binds the model to the deployed lakehouse and the agent to the
deployed model. Afterwards, run the PostDeploymentNotebook to seed the SAP data
and reframe the model.

Prereqs:  pip install fabric-cicd azure-identity ; az login
Set WORKSPACE_ID below, then:  python deploy.py
"""
from pathlib import Path

from azure.identity import AzureCliCredential
from fabric_cicd import FabricWorkspace, publish_all_items

WORKSPACE_ID = "00000000-0000-0000-0000-000000000000"

repo_dir = Path(__file__).resolve().parent / "sapmanufacturing"
workspace = FabricWorkspace(
    workspace_id=WORKSPACE_ID,
    repository_directory=str(repo_dir),
    item_type_in_scope=["Lakehouse", "Notebook", "SemanticModel", "DataAgent"],
    token_credential=AzureCliCredential(),
)

publish_all_items(workspace)
print("Deployed. Now run the 'PostDeploymentNotebook' (Run all) in the workspace.")
