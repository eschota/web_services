"""Blind packets for an agent judge, and merge of its verdicts into judge.py's JSONL format.

  python blind_judge.py make  results.jsonl images.json packets.json mapping.json
  python blind_judge.py merge packets.json mapping.json verdicts.jsonl out.judge.jsonl
"""
import json, random, sys, time

def make(results, images, packets_out, mapping_out, pairs=(("A", "B"), ("A", "C")), rep=1, seed=7):
    urls = {i["id"]: i["url"] for i in json.load(open(images, encoding="utf-8"))["images"]}
    rows = {}
    for line in open(results, encoding="utf-8"):
        r = json.loads(line)
        if r.get("status") == "completed" and r.get("mplus_ok"):
            rows[(r["config"], r["image_id"], r["repeat"])] = r
    rng = random.Random(seed)
    packets, mapping = [], {}
    for base, cand in pairs:
        for image_id in sorted(urls):
            left, right = rows.get((base, image_id, rep)), rows.get((cand, image_id, rep))
            if not left or not right:
                continue
            first, second = (right, left) if rng.random() < 0.5 else (left, right)
            pid = "p%03d" % (len(packets) + 1)
            c = lambda r: {"title": r.get("title", ""), "description": r.get("description", ""),
                           "keywords": r.get("keywords") or []}
            packets.append({"id": pid, "image_url": urls[image_id], "candidate_1": c(first), "candidate_2": c(second)})
            mapping[pid] = {"pair": "%s-%s" % (base, cand), "image_id": image_id, "repeat": rep,
                            "side_1": first["config"], "side_2": second["config"]}
    # shuffle packet order so pairs are interleaved
    rng.shuffle(packets)
    json.dump(packets, open(packets_out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    json.dump(mapping, open(mapping_out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(len(packets), "packets")

def merge(packets, mapping, verdicts, out):
    m = json.load(open(mapping, encoding="utf-8"))
    n = 0
    with open(out, "w", encoding="utf-8") as fh:
        for line in open(verdicts, encoding="utf-8"):
            if not line.strip():
                continue
            v = json.loads(line)
            meta = m[v["id"]]
            rec = dict(meta, judged_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                       judge_model="claude-opus-5-5 (agent, blind packets)")
            rec.update({k: v.get(k) for k in ("winner", "score_1", "score_2", "wrong_keywords_1", "wrong_keywords_2", "reason")})
            w = v.get("winner")
            rec["winner_config"] = {"1": meta["side_1"], "2": meta["side_2"]}.get(w, "tie" if w == "tie" else None)
            for side in ("1", "2"):
                cfg = meta["side_" + side]
                rec["score_" + cfg] = v.get("score_" + side)
                rec["wrong_keywords_" + cfg] = v.get("wrong_keywords_" + side)
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n += 1
    print(n, "verdicts ->", out)

if __name__ == "__main__":
    if sys.argv[1] == "make":
        make(*sys.argv[2:6])
    else:
        merge(*sys.argv[2:6])
