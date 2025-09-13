from pysqlcipher3 import dbapi2 as sqlite
import datetime, time, sys, traceback

# === 配置项（请按实际修改） ===
db_path = r"D:\路径\到\合并后数据库.db"   # <- 改为你合并后的数据库路径（MicroMsg/MSG 合并后的文件）
chatroom_strs = ["55989503926@chatroom", "55989503926"]  # 先按全名，再尝试只用数字
start_date = datetime.datetime(2025,8,15,0,0,0)   # 输入的开始日期
end_date   = datetime.datetime(2025,9,10,23,59,59) # 输入的结束日期
# ================================

def fmt_ts(ts):
    try:
        ts = int(ts)
    except:
        return str(ts)
    if ts > 1e12:
        # ms
        return datetime.datetime.fromtimestamp(ts/1000).isoformat()
    elif ts > 1e9:
        return datetime.datetime.fromtimestamp(ts).isoformat()
    else:
        return str(ts)

print("Opening DB:", db_path)
conn = sqlite.connect(db_path)
c = conn.cursor()

print("\n--- Tables in DB ---")
tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;").fetchall()]
for t in tables:
    print(" ", t)
    
# Try to find Name2ID table (case-insensitive)
name2id_name = None
for t in tables:
    if t.lower() == "name2id":
        name2id_name = t
        break

if name2id_name:
    print(f"\nFound Name2ID table: {name2id_name}")
    try:
        cnt = c.execute(f"SELECT COUNT(*) FROM {name2id_name};").fetchone()[0]
        print(" Name2ID count:", cnt)
        print(" Sample rows (up to 20):")
        for row in c.execute(f"SELECT * FROM {name2id_name} LIMIT 20;"):
            print("  ", row)
    except Exception as e:
        print(" Could not read Name2ID:", e)
else:
    print("\nNo Name2ID table found.")

# Search Contact for chatroom patterns
print("\n--- Search Contact for chatroom-like entries ---")
contact_exists = any(t.lower()=="contact" for t in tables)
if contact_exists:
    try:
        q = "SELECT Username, Alias, Nickname FROM Contact WHERE Username LIKE ? OR Alias LIKE ? OR Nickname LIKE ? LIMIT 200;"
        for s in chatroom_strs:
            print(f"\nSearching Contact for pattern: {s}")
            for r in c.execute(q, (f"%{s}%", f"%{s}%", f"%{s}%")):
                print(" ", r)
    except Exception as e:
        print(" Contact query failed:", e)
else:
    print(" Contact table not present.")

# Find candidate message tables (names containing 'msg'/'chat'/'message')
msg_tables = [t for t in tables if any(x in t.lower() for x in ('msg','chat','message'))]
print("\nCandidate message tables:", msg_tables)

# For each candidate table, try to detect a time field and talker-like field
start_ts_s = int(start_date.timestamp())
end_ts_s = int(end_date.timestamp())
start_ts_ms = start_ts_s * 1000
end_ts_ms = end_ts_s * 1000

for t in msg_tables:
    print(f"\n--- Checking table {t} ---")
    # print table schema if possible
    try:
        schema = c.execute("SELECT sql FROM sqlite_master WHERE name=?;", (t,)).fetchone()
        print(" schema:", schema[0] if schema else "(no schema)")
    except:
        pass

    # Try to detect talker/create time columns
    sample_cols = []
    try:
        cols = [c2[1] for c2 in c.execute(f"PRAGMA table_info('{t}');").fetchall()]
        print(" columns:", cols)
    except Exception as e:
        print(" PRAGMA failed:", e)
        cols = []

    possible_talker_cols = [col for col in cols if 'talk' in col.lower() or 'username' in col.lower() or 'from' in col.lower()]
    possible_time_cols = [col for col in cols if any(x in col.lower() for x in ('time','create','createtime','msgtime'))]
    print(" possible_talker_cols:", possible_talker_cols)
    print(" possible_time_cols:", possible_time_cols)

    # For each chatroom pattern, count matches and show latest few rows
    for s in chatroom_strs:
        found = False
        for tk in (possible_talker_cols or ['talker','username']):
            try:
                qcount = f"SELECT COUNT(*) FROM {t} WHERE {tk} LIKE ?;"
                cnt = c.execute(qcount, (f"%{s}%",)).fetchone()[0]
                if cnt>0:
                    found = True
                    print(f" Found {cnt} rows in {t} where {tk} LIKE '%{s}%'")
                    # show up to 10 rows with time/content if fields exist
                    time_col = possible_time_cols[0] if possible_time_cols else None
                    select_cols = f"{tk}" + (f", {time_col}" if time_col else "") + ", rowid"
                    rows = c.execute(f"SELECT {select_cols} FROM {t} WHERE {tk} LIKE ? ORDER BY {time_col if time_col else 'rowid'} DESC LIMIT 10;", (f"%{s}%",)).fetchall()
                    for r in rows:
                        if time_col:
                            print("  ", r, " -> time:", fmt_ts(r[1]))
                        else:
                            print("  ", r)
            except Exception:
                # ignore per-column errors
                pass
        if not found:
            print(f" No rows found in {t} matching '{s}'")

# Try direct Name2ID lookup if table exists and we have a full chatroom string
if name2id_name:
    for fullname in chatroom_strs:
        try:
            r = c.execute(f"SELECT * FROM {name2id_name} WHERE Name LIKE ? LIMIT 10;", (f"%{fullname}%",)).fetchall()
            print("\nName2ID matches for", fullname, ":", r)
        except Exception as e:
            print("Name2ID lookup error:", e)

conn.close()
print("\n--- Diagnostic complete ---")