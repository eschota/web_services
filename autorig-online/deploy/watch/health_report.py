#!/usr/bin/env python3
"""AutoRig health report for the 3-hourly watch (owner, 2026-10-16: «каждые 3 часа … проверял все ли в порядке и
докладывал в телеграм результаты работы, конверсию, и нет ли багов с оплатой … смотрел логи платежки, задач, если их
нет то поднимал тревогу»).

Runs on the VPS as root (``sudo python3 /srv/autorig/tools/health_report.py [--json] [--hours 3]``), read-only:
the task DB (sqlite, mode=ro), the funnel JSONL, the journals of autorig-storage / autorig-mt, the fleet API, the
defect reports, systemd units, the disk and the Gumroad product pages. Prints a text report with a verdict line
(OK / WARN / ALARM) and the reasons; ``--json`` prints the same as JSON. It changes nothing.

Thresholds (from the 7-day history measured 2026-10-16: 89–122 tasks a day, median gap 7.7 min, p90 36 min, the
longest night gap 8.5 h):
* ALARM: no task created for ``--no-task-alarm-hours`` (3 h); the 24 h count under 40 % of the 7-day daily mean;
  autorig-storage / autorig-mt / nginx not active; new tasks paused; a payment webhook line with "failed";
  the Gumroad product page of a sold product answering 404.
* WARN: the 24 h count under 70 % of the mean; stuck processing tasks older than 2 h; error share over 15 %;
  free disk under the reserve + 20 GB; a checkout intent older than 3 h never used while a purchase arrived;
  defect reports waiting.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import statistics
import subprocess
import sys
import urllib.request
from pathlib import Path

DB = "file:/srv/autorig/data/db/autorig.db?mode=ro"
FUNNEL = Path("/srv/autorig/data/var/funnel")
FLEET = "http://127.0.0.1:8255/api/fleet"
DEFECTS = "http://127.0.0.1:8251/api/mt/defect-reports?status=new"
UNITS = ("autorig-storage", "autorig-mt", "autorig-storage-renderfin", "autorig-storage-telegram", "nginx",
         "autorig-fleet", "autorig-devbot")
GUMROAD_PRODUCTS = ("autorig-unlimited-monthly", "autorig-5-tasks")
SUB_KEYS = ("autorig-unlimited-monthly", "autorig-unlimited")


def q(cur, sql, *args):
    return list(cur.execute(sql, args))


def one(cur, sql, *args):
    row = q(cur, sql, *args)
    return row[0] if row else None


def http(url, timeout=8):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:                      # noqa: F821 - urllib.error is loaded by request
        return e.code, b""
    except Exception as e:                                   # noqa: BLE001
        return None, str(e).encode()


def journal(unit, since, pattern=None):
    cmd = ["journalctl", "-u", unit, "--since", since, "--no-pager", "-o", "short-iso"]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=60).stdout
    except Exception as e:                                   # noqa: BLE001
        return [f"journalctl failed: {e}"]
    lines = out.splitlines()
    if pattern:
        pats = [p.lower() for p in pattern]
        lines = [ln for ln in lines if any(p in ln.lower() for p in pats)]
    return lines


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=3.0, help="the report window (hours)")
    ap.add_argument("--no-task-alarm-hours", type=float, default=3.0)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    now = dt.datetime.utcnow()
    R = {"generated_utc": now.strftime("%Y-%m-%d %H:%M"), "window_hours": a.hours, "alarms": [], "warnings": []}
    alarm, warn = R["alarms"].append, R["warnings"].append

    # ------------------------------------------------------------------ tasks
    c = sqlite3.connect(DB, uri=True)
    cur = c.cursor()
    T = R["tasks"] = {}
    T["window"] = dict(zip(("created", "done", "needs_review", "error", "processing"), one(cur,
        "select count(*), sum(status='done'), sum(status='needs_review'), sum(status='error'), sum(status='processing') "
        "from tasks where created_at >= datetime('now', ?)", f"-{a.hours} hour")))
    T["last_24h"] = dict(zip(("created", "done", "needs_review", "error", "processing", "anon", "users"), one(cur,
        "select count(*), sum(status='done'), sum(status='needs_review'), sum(status='error'), sum(status='processing'), "
        "sum(owner_type='anon'), sum(owner_type='user') from tasks where created_at >= datetime('now','-24 hour')")))
    per_day = q(cur, "select date(created_at), count(*) from tasks where created_at >= date('now','-7 day') "
                     "and date(created_at) < date('now') group by 1 order by 1")
    T["per_day_7d"] = per_day
    mean7 = statistics.mean([n for _, n in per_day]) if per_day else 0
    T["daily_mean_7d"] = round(mean7, 1)
    last_at, age_h = one(cur, "select max(created_at), (julianday('now')-julianday(max(created_at)))*24 from tasks")
    T["last_created_utc"], T["last_created_age_h"] = (last_at or "")[:19], round(age_h or 0, 2)
    stuck_all = q(cur, "select id, round((julianday('now')-julianday(created_at))*24,1), coalesce(progress_page,''), "
                       "coalesce(input_url,''), coalesce(v3_category,'') from tasks where status='processing' "
                       "and created_at < datetime('now','-2 hour') order by created_at limit 30")
    # The vehicle conveyor stops at the isolate stage until the owner accepts the parts in the viewer (owner order
    # 2026-10-15): those tasks stay "processing" by design and are listed apart, never a warning.
    vehicle = [(i, h) for i, h, p, u, cat in stuck_all if cat == "vehicle" or "vehicle_corpus" in u]
    stuck = [(i, h, p) for i, h, p, u, cat in stuck_all if not (cat == "vehicle" or "vehicle_corpus" in u)]
    T["stuck_processing"] = [{"task": i, "age_h": h, "page": p} for i, h, p in stuck]
    T["vehicle_waiting_for_owner"] = [{"task": i, "age_h": h} for i, h in vehicle]
    T["errors_24h"] = [{"error": e, "n": n} for e, n in q(cur,
        "select substr(coalesce(error_message,''),1,120), count(*) from tasks where status='error' "
        "and created_at >= datetime('now','-24 hour') group by 1 order by 2 desc limit 6")]
    T["by_category_24h"] = q(cur, "select coalesce(v3_category,'?'), count(*) from tasks "
                                  "where created_at >= datetime('now','-24 hour') group by 1 order by 2 desc")
    # task-rate verdicts
    if age_h is not None and age_h >= a.no_task_alarm_hours:
        alarm(f"no new task for {age_h:.1f} h (last {T['last_created_utc']} UTC); 7-day median gap 8 min, p90 36 min")
    n24 = T["last_24h"]["created"] or 0
    if mean7 and n24 < 0.4 * mean7:
        alarm(f"only {n24} tasks in 24 h vs the 7-day mean {mean7:.0f}/day")
    elif mean7 and n24 < 0.7 * mean7:
        warn(f"{n24} tasks in 24 h vs the 7-day mean {mean7:.0f}/day")
    if stuck:
        warn(f"{len(stuck)} task(s) still processing after 2 h (oldest {stuck[0][1]} h)")
    err = T["last_24h"]["error"] or 0
    if n24 and err / n24 > 0.15:
        warn(f"error share 24 h {err}/{n24} = {100 * err / n24:.0f} %")

    # --------------------------------------------------------------- payments
    P = R["payments"] = {}
    for label, since in (("window", f"-{a.hours} hour"), ("24h", "-24 hour"), ("7d", "-7 day"), ("30d", "-30 day")):
        n, cents, ref, test = one(cur, "select count(*), coalesce(sum(price),0), coalesce(sum(refunded),0), "
                                       "coalesce(sum(test),0) from gumroad_purchases where created_at >= datetime('now', ?)", since)
        P[f"purchases_{label}"] = {"n": n, "usd": round((cents or 0) / 100, 2), "refunded": ref, "test": test}
    P["purchases_recent"] = [{"at": t[:16], "product": p, "usd": round((c_ or 0) / 100, 2), "refunded": r, "credited": cr}
                             for t, p, c_, r, cr in q(cur, "select created_at, product_permalink, price, refunded, credited "
                                                           "from gumroad_purchases order by created_at desc limit 5")]
    P["by_product_30d"] = q(cur, "select product_permalink, count(*), coalesce(sum(price),0)/100.0 from gumroad_purchases "
                                 "where created_at >= date('now','-30 day') group by 1 order by 2 desc")
    for label, since in (("window", f"-{a.hours} hour"), ("24h", "-24 hour"), ("7d", "-7 day")):
        n, used, with_task = one(cur, "select count(*), sum(used_at is not null), sum(task_id is not null) "
                                      "from purchase_checkout_intents where created_at >= datetime('now', ?)", since)
        P[f"checkout_intents_{label}"] = {"n": n, "used": used or 0, "with_task": with_task or 0}
    P["intents_by_product_7d"] = q(cur, "select product_permalink, count(*) from purchase_checkout_intents "
                                        "where created_at >= date('now','-7 day') group by 1 order by 2 desc")
    P["active_subscriptions"] = one(cur, "select count(*) from users where autorig_subscription_period_end > datetime('now') "
                                         "and lower(coalesce(autorig_subscription_status,'')) in ('active','trialing','cancelled','past_due','canceled')")[0]
    P["task_unlocks_24h"] = one(cur, "select count(*) from task_file_purchases where file_index is null "
                                     "and created_at >= datetime('now','-24 hour')")[0]
    P["task_unlocks_7d"] = one(cur, "select count(*) from task_file_purchases where file_index is null "
                                    "and created_at >= datetime('now','-7 day')")[0]
    P["credits_on_accounts"] = one(cur, "select count(*), coalesce(sum(balance_credits),0) from users where balance_credits > 0")
    i7, p7 = P["checkout_intents_7d"]["n"] or 0, P["purchases_7d"]["n"] or 0
    P["conversion_7d_intent_to_purchase_pct"] = round(100 * p7 / i7, 1) if i7 else None
    bad = q(cur, "select auto_unlock_status, count(*) from purchase_checkout_intents where created_at >= date('now','-7 day') "
                 "and auto_unlock_status is not null group by 1")
    P["auto_unlock_status_7d"] = bad
    for st, n in bad:
        if st in ("insufficient_credits", "task_not_found", "invalid_task_id"):
            warn(f"auto-unlock after purchase: {st} × {n} in 7 d")
    c.close()

    # ------------------------------------------------------------------ funnel
    F = R["funnel"] = {}
    for day in (now.date(), now.date() - dt.timedelta(days=1)):
        counts = {}
        p = FUNNEL / f"{day}.jsonl"
        if p.exists():
            for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines():
                try:
                    rec = json.loads(ln)
                    ev = rec.get("e") or rec.get("event") or rec.get("name")      # funnel.py writes {"t","e","s","v",...}
                except ValueError:
                    continue
                counts[ev] = counts.get(ev, 0) + 1
        F[str(day)] = counts
    today = F[str(now.date())]
    if today.get("paywall_view"):
        F["today_paywall_to_checkout_pct"] = round(100 * today.get("checkout_start", 0) / today["paywall_view"], 1)

    # ------------------------------------------------------------ journals
    J = R["journal"] = {}
    since = f"-{int(a.hours * 60)}min"
    pay = journal("autorig-storage", since, ["[Gumroad]", "[Checkout]", "gumroad", "subscription"])
    J["payment_lines_window"] = pay[-15:]
    J["payment_lines_n"] = len(pay)
    pay_bad = [ln for ln in pay if any(k in ln.lower() for k in ("failed", "error", "traceback", "refused", "invalid"))]
    if pay_bad:
        alarm(f"payment log: {len(pay_bad)} failure line(s) in {a.hours:g} h: {pay_bad[-1][-160:]}")
    for unit in ("autorig-storage", "autorig-mt"):
        tb = journal(unit, since, ["traceback", "critical", "oom", "killed"])
        J[f"{unit}_traceback_lines"] = len(tb)
        J[f"{unit}_traceback_tail"] = [ln[-200:] for ln in tb[-4:]]
        if len(tb) > 20:
            warn(f"{unit}: {len(tb)} traceback/critical lines in {a.hours:g} h")

    # ------------------------------------------------------------ services, disk, fleet
    S = R["services"] = {}
    for u in UNITS:
        try:
            S[u] = subprocess.run(["systemctl", "is-active", u], capture_output=True, text=True, timeout=10).stdout.strip()
        except Exception as e:                               # noqa: BLE001
            S[u] = f"? {e}"
    for u in ("autorig-storage", "autorig-mt", "nginx"):
        if S.get(u) != "active":
            alarm(f"unit {u} is {S.get(u)}")
    st, body = http(FLEET)
    if st == 200:
        try:
            fl = json.loads(body)
            v = fl.get("vps_object") or {}
            R["fleet"] = {"release": v.get("release_string"), "disk_free_gb": (v.get("disk_object") or {}).get("free_gb"),
                          "new_tasks_paused": v.get("new_tasks_paused_bool"), "below_reserve": v.get("below_reserve_bool"),
                          "queues": fl.get("queues_object"), "summary": {k: fl.get("summary_object", {}).get(k)
                          for k in ("autorig_capable_array", "ai_ready_array", "low_disk_array")},
                          "boxes": [(b.get("id") or b.get("name"), b.get("state_string")) for b in fl.get("boxes_array") or []]}
            if v.get("new_tasks_paused_bool"):
                alarm("new tasks are PAUSED on the VPS")
            free = (v.get("disk_object") or {}).get("free_gb")
            reserve = v.get("artifact_cache_reserve_gb_float") or 10
            if isinstance(free, (int, float)) and free < reserve + 20:
                warn(f"free disk {free} GB (reserve {reserve} GB)")
            if not (fl.get("summary_object", {}).get("ai_ready_array")):
                warn("no AI node ready (Vision answers fall back to the free router)")
        except ValueError:
            R["fleet"] = {"error": "bad json"}
    else:
        R["fleet"] = {"error": f"fleet api {st}"}
        warn(f"fleet API not answering ({st})")
    st, body = http(DEFECTS)
    if st == 200:
        try:
            d = json.loads(body)
            items = d if isinstance(d, list) else (d.get("items") or d.get("reports") or [])
            R["defect_reports_new"] = len(items)
            if items:
                warn(f"{len(items)} defect report(s) waiting (buttons in Telegram)")
        except ValueError:
            R["defect_reports_new"] = None
    try:
        sv = os.statvfs("/srv/autorig")
        R["disk_free_gb"] = round(sv.f_bavail * sv.f_frsize / 1e9, 1)
    except OSError:
        pass

    # ------------------------------------------------------------ Gumroad product pages
    G = R["gumroad_products"] = {}
    for key in GUMROAD_PRODUCTS:
        st, _ = http(f"https://u3d.gumroad.com/l/{key}", timeout=15)
        G[key] = st
        if st == 404:
            alarm(f"Gumroad product page /l/{key} answers 404: the product is not created, its checkout button is dead")
        elif st not in (200, 301, 302, 303):
            warn(f"Gumroad product page /l/{key} answered {st}")

    R["verdict"] = "ALARM" if R["alarms"] else ("WARN" if R["warnings"] else "OK")
    if a.json:
        print(json.dumps(R, ensure_ascii=False, indent=1, default=str))
        return
    w = T["window"]; l24 = T["last_24h"]
    print(f"AutoRig health {R['generated_utc']} UTC · window {a.hours:g} h · verdict {R['verdict']}")
    for x in R["alarms"]:
        print(f"  ALARM: {x}")
    for x in R["warnings"]:
        print(f"  WARN: {x}")
    print(f"tasks: window created {w['created']} (done {w['done']}, review {w['needs_review']}, error {w['error']}, "
          f"processing {w['processing']}); 24 h created {l24['created']} (anon {l24['anon']}, users {l24['users']}; done {l24['done']}, "
          f"review {l24['needs_review']}, error {l24['error']}); 7-day mean {mean7:.0f}/day; last task {T['last_created_age_h']} h ago")
    print(f"  per day 7d: {', '.join(f'{d[5:]}={n}' for d, n in per_day)}")
    print(f"  categories 24 h: {', '.join(f'{k}={n}' for k, n in T['by_category_24h'])}")
    if T["vehicle_waiting_for_owner"]:
        print(f"  vehicle tasks waiting for the owner's part decision (by design): {len(T['vehicle_waiting_for_owner'])}")
    if T["stuck_processing"]:
        print(f"  stuck processing: {', '.join(s['task'][:8] + ' ' + str(s['age_h']) + 'h' for s in T['stuck_processing'])}")
    for e in T["errors_24h"]:
        print(f"  error ×{e['n']}: {e['error']}")
    print(f"payments: window {P['purchases_window']['n']} · 24 h {P['purchases_24h']['n']} (${P['purchases_24h']['usd']}) · "
          f"7 d {P['purchases_7d']['n']} (${P['purchases_7d']['usd']}) · 30 d {P['purchases_30d']['n']} (${P['purchases_30d']['usd']}, "
          f"refunded {P['purchases_30d']['refunded']}); active subscriptions {P['active_subscriptions']}; "
          f"task unlocks 24 h {P['task_unlocks_24h']} / 7 d {P['task_unlocks_7d']}")
    print(f"  checkout intents: window {P['checkout_intents_window']['n']} · 24 h {P['checkout_intents_24h']['n']} · 7 d {i7} "
          f"(used {P['checkout_intents_7d']['used']}); conversion 7 d intent→purchase {P['conversion_7d_intent_to_purchase_pct']} %; "
          f"intents by product 7 d: {P['intents_by_product_7d']}")
    print(f"  by product 30 d: {P['by_product_30d']}; last purchases: {[(x['at'], x['product'], x['usd']) for x in P['purchases_recent'][:3]]}")
    print(f"  accounts with credits: {P['credits_on_accounts']}; auto-unlock statuses 7 d: {bad}")
    print(f"funnel today: {today}; yesterday: {F[str(now.date() - dt.timedelta(days=1))]}")
    print(f"journal: payment lines {J['payment_lines_n']}; tracebacks storage {J['autorig-storage_traceback_lines']} / "
          f"mt {J['autorig-mt_traceback_lines']}")
    for ln in J["payment_lines_window"][-6:]:
        print(f"  {ln[-180:]}")
    print(f"services: {', '.join(f'{k}={v}' for k, v in S.items())}")
    fl = R.get("fleet") or {}
    print(f"vps: release {fl.get('release')}, disk free {R.get('disk_free_gb')} GB, new tasks paused {fl.get('new_tasks_paused')}, "
          f"defect reports new {R.get('defect_reports_new')}; fleet ai_ready {fl.get('summary', {}).get('ai_ready_array') if isinstance(fl.get('summary'), dict) else '?'}; "
          f"boxes {fl.get('boxes')}")
    print(f"gumroad product pages: {G}")


if __name__ == "__main__":
    sys.exit(main())
