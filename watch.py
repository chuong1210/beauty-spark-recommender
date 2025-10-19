import json

with open("file.jsonl", "r", encoding="utf-8") as f:
    for i, line in enumerate(f):
        if i >= 10:  # chỉ in 10 dòng đầu
            break
        record = json.loads(line)
        print(record)
