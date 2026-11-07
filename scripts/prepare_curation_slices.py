import sqlite3
import json
import os

db = sqlite3.connect('data/sessions.db')
c = db.cursor()

slices = {
    "dsh_and_general": [
        "344f8d6a-1e97-484f-8a75-fe1e1df51fd9", # dsh mobile remote access
        "c2c694ee-93de-4514-8097-30822c762498", # dsh IM / service
        "session_4734342c7f7085c7433820c0c9b9f2f1", # dsh web restart / port conflict
        "session_ce490f933f71b4a9c07a646214efb97c", # zcode remote access
        "session_a8cd72b7bf77402a3d40e58764cf7fc6", # raspberry pi network check
        "62f73d47-b55d-4230-9c07-8cd2c28d93cc"  # dsh aep-pki
    ],
    "opnsense_mihomo": [
        "session_070c1eb43c7e99749b0a45d28b9414ab",
        "session_1d1b3f9cbfcd59f0394a2afa94bd906e",
        "session_2dd58820dd0556ead067933d4478f650",
        "session_a7d1271d44b025dd95c929cda70bf610",
        "session_9823e5dad741ce9c668b5b2d53705def",
        "sess_679b107c-430b-4668-9a51-fa74a4d1e34c",
        "sess_c40b3cbb-d1d6-4a41-b8af-380e3175e427",
        "sess_8d932ebf-e6b8-4755-bf65-14bfc3af4c1e",
        "sess_7b86b4db-b404-4d86-b749-3e5ebe62b636",
        "sess_af01d870-0298-42a5-b13b-e853fac7e9ed"
    ],
    "vikey_crypto": [
        "session_166ccd0e869ebde42697f681150ede68", # vikey sm3 correct length
        "session_c0910c88c9e049dcc0d72c17cfa513a7", # vikey sm2
        "sess_4daa64aa-0b66-45c6-aa26-9fb077b8f390", # vikey test
        "session_d96a13a4c4ecc17b6281e28a7ee61e8d"  # vikey-1接入 aep-pki
    ],
    "aep_pki_acme": [
        "session_4d685f220e222ef2879b0416d6cbdef0", # acme1
        "session_c420126af911c7dbeffe140f229ac27a", # project status
        "sess_dc842f4b-54d8-468d-9aca-b19d33f77997",
        "sess_dba1cd50-cbc3-41ae-af38-22b883c30309",
        "sess_49fb67d1-2568-4d80-a699-8af8df49c97d",
        "sess_dba839a8-b4fa-40a4-92c8-88f18850eed5"
    ]
}

os.makedirs('data/curation_slices', exist_ok=True)

for slice_name, sids in slices.items():
    slice_data = []
    for sid in sids:
        c.execute("SELECT session_id, agent_id, project_id, started_at, ended_at FROM raw_sessions WHERE session_id = ?", (sid,))
        s_row = c.fetchone()
        if not s_row:
            continue
        c.execute("SELECT message_id, role, content, sequence, created_at FROM raw_messages WHERE session_id = ? ORDER BY sequence ASC", (sid,))
        m_rows = c.fetchall()
        msgs = []
        for m in m_rows:
            msgs.append({
                "message_id": m[0],
                "role": m[1],
                "content": m[2],
                "sequence": m[3],
                "created_at": m[4]
            })
        slice_data.append({
            "session_id": s_row[0],
            "agent_id": s_row[1],
            "project_id": s_row[2],
            "started_at": s_row[3],
            "ended_at": s_row[4],
            "messages": msgs
        })
    out_path = f"data/curation_slices/{slice_name}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(slice_data, f, ensure_ascii=False, indent=2)
    print(f"Slice {slice_name}: {len(slice_data)} sessions saved to {out_path} (size: {os.path.getsize(out_path)} bytes)")
