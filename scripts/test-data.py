#!/usr/bin/env python3
import sys, json, urllib.request

HTTP_PORT = sys.argv[1] if len(sys.argv) > 1 else "3000"
BASE = f"http://localhost:{HTTP_PORT}"

def api_get(path):
    try:
        resp = urllib.request.urlopen(f"{BASE}{path}", timeout=5)
        return json.loads(resp.read())
    except Exception as e:
        print(f"  ❌ Server not running: {e}", file=sys.stderr)
        sys.exit(1)

def api_post(path, data):
    req = urllib.request.Request(
        f"{BASE}{path}",
        data=json.dumps(data).encode(),
        headers={"Content-Type": "application/json"},
    )
    resp = urllib.request.urlopen(req, timeout=60)
    return json.loads(resp.read())

action = sys.argv[2] if len(sys.argv) > 2 else "lifecycle"

if action == "generate":
    scenario = sys.argv[3] if len(sys.argv) > 3 else "iot"
    points = int(sys.argv[4]) if len(sys.argv) > 4 else 60
    r = api_post("/api/test/generate-business-data", {
        "scenario": scenario,
        "points_per_series": points,
        "skip_auto_demote": True,
    })
    d = r["data"]
    print(f"  ✅ {d['total_points']} points, {d['elapsed_secs']:.1f}s")

elif action == "lifecycle":
    d = api_get("/api/lifecycle/status")["data"]
    warm_cfs = [cf["cf_name"] for cf in d["hot_cfs"] if cf.get("demote_eligible") == "warm"]
    cold_cfs = [cf["cf_name"] for cf in d["hot_cfs"] if cf.get("demote_eligible") == "cold"]

    if warm_cfs:
        print(f"  Demoting {len(warm_cfs)} CFs to warm...")
        r = api_post("/api/lifecycle/demote-to-warm", {"cf_names": warm_cfs})
        print(f"  ✅ {len(r['data']['demoted'])} CFs demoted to warm")

    if cold_cfs:
        print(f"  Demoting {len(cold_cfs)} CFs to cold...")
        r = api_post("/api/lifecycle/demote-to-cold", {"cf_names": cold_cfs})
        print(f"  ✅ {len(r['data']['demoted'])} CFs demoted to cold")

    if not warm_cfs and not cold_cfs:
        print("  No CFs eligible for demotion")

elif action == "status":
    d = api_get("/api/lifecycle/status")["data"]
    print(f"  Hot: {len(d['hot_cfs'])}  Warm: {len(d['warm_cfs'])}  Cold: {len(d['cold_cfs'])}")
