"""Reframe the already-deployed Direct Lake semantic model.

The SemanticModel item is deployed by fabric-cicd (not created here). This module
only refreshes/reframes it at runtime, once the lakehouse tables exist: a fresh
Direct Lake model can't be queried until its framing picks up the tables, and the
lakehouse SQL analytics endpoint metadata sync lags table creation. So we poll a
metadata refresh and retry the dataset refresh with backoff.
"""
import time

MODEL_NAME = "SAP_Manufacturing_Model"


def refresh_semantic_model(ws_id, lh_id):
    """Find the deployed model by name and reframe it. Returns model id."""
    import sempy.fabric as fabric
    client = fabric.FabricRestClient()

    items = client.get("/v1/workspaces/%s/items?type=SemanticModel" % ws_id).json().get("value", [])
    match = [i for i in items if i["displayName"] == MODEL_NAME]
    if not match:
        raise RuntimeError("Semantic model %s not found - was it deployed by fabric-cicd?" % MODEL_NAME)
    model_id = match[0]["id"]

    # SQL endpoint metadata sync lags table creation; discover the endpoint id so we
    # can nudge it before each reframe attempt.
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
