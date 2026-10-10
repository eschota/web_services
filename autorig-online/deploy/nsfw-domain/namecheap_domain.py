#!/usr/bin/env python3
"""Namecheap helper for the AutoRig adult domain (owner order 2026-10-11). Runs on the VPS as root.

    namecheap_domain.py check [domain ...]        availability, premium flags, price (read-only)
    namecheap_domain.py dns <domain> [ip]         A records @ and www -> the VPS (merges other records)
    namecheap_domain.py register <domain> --confirm <domain> [--years 1]
                                                  BUYS the domain. Only with the owner's explicit yes.

Credentials: NAMECHEAP_API_USER / NAMECHEAP_API_KEY (/srv/autorig/secrets/backend.env).
Nothing secret is printed. The API answers only from the whitelisted IP of this box.
"""
from __future__ import annotations

import argparse
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

ENV_FILE = "/srv/autorig/secrets/backend.env"
API = "https://api.namecheap.com/xml.response"
VPS_IP = "37.187.57.177"
PROTECTED = {"autorig.online", "qwertystock.com", "freestock.online", "microstock.plus"}
DEFAULT_CANDIDATES = ["autorig.red", "autorigonline.red", "autorigred.com", "autorig18.com",
                      "autorig.vip", "autorig.xxx", "autorig.adult"]


def _env():
    out = {}
    with open(ENV_FILE, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line.startswith("NAMECHEAP_") and "=" in line:
                k, v = line.split("=", 1)
                out[k] = v.strip().strip('"').strip("'")
    return out


def _tag(el):
    return el.tag.split("}", 1)[-1]


def call(command, method="GET", **params):
    env = _env()
    base = {"ApiUser": env["NAMECHEAP_API_USER"], "ApiKey": env["NAMECHEAP_API_KEY"],
            "UserName": env.get("NAMECHEAP_USERNAME") or env["NAMECHEAP_API_USER"],
            "ClientIp": VPS_IP, "Command": command}
    base.update({k: str(v) for k, v in params.items()})
    data = urllib.parse.urlencode(base)
    if method == "POST":
        req = urllib.request.Request(API, data=data.encode(), method="POST")
    else:
        req = urllib.request.Request(API + "?" + data)
    with urllib.request.urlopen(req, timeout=90) as resp:
        root = ET.fromstring(resp.read())
    errors = [f"{e.get('Number')}: {e.text}" for e in root.iter() if _tag(e) == "Error"]
    if root.get("Status") != "OK" or errors:
        raise SystemExit(f"{command} failed: {'; '.join(errors) or root.get('Status')}")
    return root


def prices(tld):
    root = call("namecheap.users.getPricing", ProductType="DOMAIN", ProductName=tld)
    out = {}
    for cat in root.iter():
        if _tag(cat) != "ProductCategory":
            continue
        for p in cat.iter():
            if _tag(p) == "Price" and p.get("Duration") == "1":
                out[cat.get("Name")] = (float(p.get("YourPrice") or 0), float(p.get("YourAdditonalCost") or 0))
    return out


def cmd_check(domains):
    domains = domains or DEFAULT_CANDIDATES
    root = call("namecheap.domains.check", DomainList=",".join(domains))
    cache = {}
    print(f"{'domain':22} {'free':5} {'premium':7} {'1st year':>9} {'renewal':>9}")
    for el in root.iter():
        if _tag(el) != "DomainCheckResult":
            continue
        name = el.get("Domain")
        tld = name.split(".", 1)[1]
        if tld not in cache:
            cache[tld] = prices(tld)
        reg, fee = cache[tld].get("register", (0, 0))
        ren, _ = cache[tld].get("renew", (0, 0))
        premium = el.get("IsPremiumName") == "true"
        if premium:
            reg = float(el.get("PremiumRegistrationPrice") or reg)
            ren = float(el.get("PremiumRenewalPrice") or ren)
        print(f"{name:22} {el.get('Available'):5} {str(premium):7} {reg + fee:9.2f} {ren + fee:9.2f}  USD")


def _split(domain):
    domain = domain.strip().lower().rstrip(".")
    if domain in PROTECTED or any(domain.endswith("." + p) for p in PROTECTED):
        raise SystemExit(f"refusing to touch {domain}: not the adult domain")
    sld, tld = domain.split(".", 1)
    return domain, sld, tld


def cmd_dns(domain, ip):
    domain, sld, tld = _split(domain)
    root = call("namecheap.domains.dns.getHosts", SLD=sld, TLD=tld)
    hosts = []
    for el in root.iter():
        if _tag(el) == "host" and el.get("Name") is not None:
            hosts.append({"Name": el.get("Name"), "Type": el.get("Type"), "Address": el.get("Address"),
                          "MXPref": el.get("MXPref") or "10", "TTL": el.get("TTL") or "1800"})
    keep = [h for h in hosts if not (h["Name"] in ("@", "www") and h["Type"] in ("A", "AAAA", "CNAME", "URL", "URL301", "FRAME"))]
    keep += [{"Name": "@", "Type": "A", "Address": ip, "MXPref": "10", "TTL": "300"},
             {"Name": "www", "Type": "A", "Address": ip, "MXPref": "10", "TTL": "300"}]
    params = {"SLD": sld, "TLD": tld}
    for i, h in enumerate(keep, 1):
        params.update({f"HostName{i}": h["Name"], f"RecordType{i}": h["Type"], f"Address{i}": h["Address"],
                       f"MXPref{i}": h["MXPref"], f"TTL{i}": h["TTL"]})
    call("namecheap.domains.dns.setHosts", method="POST", **params)
    print(f"{domain}: @ and www -> {ip} ({len(keep)} records)")


def cmd_register(domain, confirm, years):
    domain, sld, tld = _split(domain)
    if confirm != domain:
        raise SystemExit("register needs --confirm <the same domain>; it spends money")
    root = call("namecheap.domains.check", DomainList=domain)
    res = next(el for el in root.iter() if _tag(el) == "DomainCheckResult")
    if res.get("Available") != "true":
        raise SystemExit(f"{domain} is not available")
    if res.get("IsPremiumName") == "true":
        raise SystemExit(f"{domain} is a premium name; not buying it from a script")
    root = call("namecheap.users.address.getList")
    ids = [(e.get("AddressId"), e.get("IsDefault")) for e in root.iter() if _tag(e) == "List"]
    if not ids:
        raise SystemExit("no saved address in the Namecheap account")
    aid = next((a for a, d in ids if d == "true"), ids[0][0])
    info = call("namecheap.users.address.getInfo", AddressId=aid)
    addr = {_tag(e): (e.text or "").strip() for e in info.iter()}
    contact = {"FirstName": addr.get("FirstName"), "LastName": addr.get("LastName"),
               "Address1": addr.get("Address1"), "City": addr.get("City"),
               "StateProvince": addr.get("StateProvince") or addr.get("StateProvinceChoice") or "NA",
               "PostalCode": addr.get("Zip"), "Country": addr.get("Country"),
               "Phone": addr.get("Phone"), "EmailAddress": addr.get("EmailAddress"),
               "OrganizationName": addr.get("Organization") or ""}
    params = {"DomainName": domain, "Years": years, "AddFreeWhoisguard": "yes", "WGEnabled": "yes"}
    for role in ("Registrant", "Tech", "Admin", "AuxBilling"):
        for k, v in contact.items():
            if v:
                params[role + k] = v
    root = call("namecheap.domains.create", method="POST", **params)
    res = next(el for el in root.iter() if _tag(el) == "DomainCreateResult")
    print(f"{domain}: registered={res.get('Registered')} charged={res.get('ChargedAmount')} "
          f"order={res.get('OrderID')} whoisguard={res.get('WhoisguardEnable')}")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check")
    c.add_argument("domains", nargs="*")
    d = sub.add_parser("dns")
    d.add_argument("domain")
    d.add_argument("ip", nargs="?", default=VPS_IP)
    r = sub.add_parser("register")
    r.add_argument("domain")
    r.add_argument("--confirm", required=True)
    r.add_argument("--years", type=int, default=1)
    a = ap.parse_args()
    if a.cmd == "check":
        cmd_check(a.domains)
    elif a.cmd == "dns":
        cmd_dns(a.domain, a.ip)
    else:
        cmd_register(a.domain, a.confirm, a.years)


if __name__ == "__main__":
    sys.exit(main())
