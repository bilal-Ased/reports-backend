import json
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Africa/Nairobi")  # use local timezone you want

def human_from_seconds(secs: int) -> str:
    days = secs // 86400
    hours = (secs % 86400) // 3600
    minutes = (secs % 3600) // 60
    return f"{days} Days, {hours} Hrs, {minutes} Mins"

def human_from_created(created_str: str) -> str:
    # expected format: "2025-08-16 08:16:04"
    try:
        dt = datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S")
        dt = dt.replace(tzinfo=TZ)  # interpret created_date in Africa/Nairobi
    except Exception:
        return ""  # unparsable
    now = datetime.now(tz=TZ)
    if now < dt:
        # future date — clamp to zero
        return "0 Days, 0 Hrs, 0 Mins"
    delta = now - dt
    days = delta.days
    hours = delta.seconds // 3600
    minutes = (delta.seconds % 3600) // 60
    return f"{days} Days, {hours} Hrs, {minutes} Mins"

# --- main file handling (keeps your split-by-'},{' approach) ---
text = open("/Users/bilalmughal/Documents/Dev/kati-reports-backend/tickets.json", "r", encoding="utf-8", errors="replace").read()

parts = text.split('},{')
rows = []

for part in parts:
    obj_str = part
    if not obj_str.startswith("{"):
        obj_str = "{" + obj_str
    if not obj_str.endswith("}"):
        obj_str = obj_str + "}"

    try:
        obj = json.loads(obj_str)
    except Exception:
        # try a safer fix: replace unescaped control chars, then try again
        safe = obj_str.encode('utf-8', 'replace').decode('utf-8', 'replace')
        try:
            obj = json.loads(safe)
        except Exception:
            continue

    # Resolution Type extraction (same as before)
    resolution_type = ""
    if obj.get("additional_variables"):
        for item in obj["additional_variables"]:
            if "Resolution Type" in item:
                resolution_type = item["Resolution Type"]
                break
    obj["Resolution Type"] = resolution_type

    # compute human age using created_date if possible, fallback to age secs
    age_human = ""
    cd = obj.get("created_date") or obj.get("date_created") or ""
    if cd:
        age_human = human_from_created(cd)
    else:
        try:
            secs = int(obj.get("age", 0))
            age_human = human_from_seconds(secs)
        except:
            age_human = ""

    obj["age_human"] = age_human
    rows.append(obj)

df = pd.DataFrame(rows)
df.to_csv("tickets.csv", index=False)
print("done:", len(rows), "rows written")
