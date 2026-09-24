"""One-off: extract the deployed SemanticModel + DataAgent git definitions
(saved as %TEMP%\model_def.json / agent_def.json via getDefinition) into
parameterized source templates under ./src, replacing workspace/lakehouse/model
GUIDs with placeholders that parameter.yml resolves at deploy time.

Run once:  python extract_definitions.py
"""
import base64
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"

WS = "0923c86c-2c05-4b30-ac68-4cb0e6fe0031"
LH = "d338f94b-5dab-40b3-b867-397f7d28044a"
MODEL = "a240a242-4b4c-4c08-9fd4-e8702a7e80fa"

TMP = Path(os.path.expandvars(r"%TEMP%"))


def _parts(fname):
    d = json.load(open(TMP / fname, encoding="utf-8"))
    for p in d["definition"]["parts"]:
        yield p["path"], base64.b64decode(p["payload"]).decode("utf-8")


def _write(base, path, text):
    out = base / path
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    print("  wrote", out.relative_to(ROOT))


def main():
    # ---- Semantic model -> src/semantic_model/ (skip .platform; generator adds it)
    sm = SRC / "semantic_model"
    if sm.exists():
        import shutil
        shutil.rmtree(sm)
    for path, text in _parts("model_def.json"):
        if path == ".platform":
            continue
        if path.endswith("expressions.tmdl"):
            text = text.replace(WS, "__WORKSPACE_ID__").replace(LH, "__LAKEHOUSE_ID__")
        _write(sm, path, text)

    # ---- Data agent -> src/data_agent/ (skip .platform; generator adds it)
    da = SRC / "data_agent"
    if da.exists():
        import shutil
        shutil.rmtree(da)
    for path, text in _parts("agent_def.json"):
        if path == ".platform":
            continue
        if path.endswith("datasource.json"):
            o = json.loads(text)
            o["artifactId"] = "__SEMANTIC_MODEL_ID__"
            o["workspaceId"] = "__WORKSPACE_ID__"

            # Force every table + column to be selected so the data agent is
            # grounded on the whole model right after deployment. The captured
            # definition has table-level is_selected=False, which leaves the
            # agent with no tables until a human ticks them in the portal.
            def _select_all(elements):
                for el in elements:
                    el["is_selected"] = True
                    if el.get("children"):
                        _select_all(el["children"])
            _select_all(o.get("elements", []))

            text = json.dumps(o, indent=2)
        _write(da, path, text)

    print("done")


if __name__ == "__main__":
    main()
