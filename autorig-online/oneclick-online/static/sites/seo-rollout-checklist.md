# Multilang SEO Rollout Checklist

## 1) Crawlability checks
- Open and verify `200 OK` for:
  - `https://3dsmaxtounity.com/sitemap.xml`
  - `https://3dsmaxtounreal.com/sitemap.xml`
  - `https://3dsmaxtounity.com/ru/`, `/zh-cn/`, `/es/`
  - `https://3dsmaxtounreal.com/ru/`, `/zh-cn/`, `/es/`
- Confirm `robots.txt` points to the correct sitemap on each domain.

## 2) Source checks per page
- Ensure each localized page has:
  - self-canonical on the same language URL
  - `hreflang` set for `en`, `ru`, `zh-CN`, `es`, `x-default`
  - internal navigation links staying in the same language namespace

## 3) Search Console / Bing submission
- Google Search Console:
  - Submit `https://3dsmaxtounity.com/sitemap.xml`
  - Submit `https://3dsmaxtounreal.com/sitemap.xml`
  - Request indexing for:
    - `/`, `/ru/`, `/zh-cn/`, `/es/` on both domains
- Bing Webmaster Tools:
  - Submit both sitemaps
  - Trigger URL inspection for main language roots

## 4) Monitoring window (2-4 weeks)
- Track:
  - indexed pages count by language
  - impressions/clicks split by locale
  - crawl anomalies and alternate-page issues
- Watch for:
  - wrong canonical target
  - missing return hreflang
  - mixed-language internal linking

## 5) Expected success criteria
- Sitemaps are accepted and processed.
- Localized URLs are indexed for all 4 language variants.
- No critical `hreflang` or canonical conflicts in webmaster reports.
