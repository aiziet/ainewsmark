import os, time, hashlib, datetime as dt
import feedparser, requests
from urllib.parse import urlparse
from simhash import Simhash
from sentence_transformers import SentenceTransformer
import psycopg2, psycopg2.extras
import yaml

print("Starting ingestion job…")

PG_CONN = os.environ["PG_CONN"]
MODEL = SentenceTransformer("sentence-transformers/paraphrase-multilingual-mpnet-base-v2")

def sha1(x): return hashlib.sha1(x.encode("utf-8","ignore")).digest()
def norm_url(u):
    if not u: return u
    p = urlparse(u); return f"{p.scheme}://{p.netloc}{p.path}".lower()
def simh(text): return Simhash(text or "", f=64).value

def upsert(cur, table, data, conflict_cols):
    cols = ", ".join(data.keys())
    vals = ", ".join(["%s"]*len(data))
    updates = ", ".join([f"{k}=excluded.{k}" for k in data.keys() if k not in conflict_cols])
    cur.execute(f"insert into {table} ({cols}) values ({vals}) "
                f"on conflict ({', '.join(conflict_cols)}) do update set {updates}",
                list(data.values()))

def ingest_rss(cur, s):
    print("RSS:", s["name"])
    d = feedparser.parse(s["rss_url"])
    for e in d.entries[:50]:
        url = norm_url(getattr(e,"link",None))
        guid = getattr(e,"id",url) or url
        title = getattr(e,"title","") or ""
        summary = getattr(e,"summary","") or ""
        published = getattr(e,"published_parsed",None)
        published_at = (dt.datetime.fromtimestamp(time.mktime(published), dt.timezone.utc)
                        if published else dt.datetime.now(dt.timezone.utc))
        text = f"{title}\n{summary}"
        emb = MODEL.encode([text], normalize_embeddings=True)[0].tolist()
        upsert(cur, "items", {
          "source_id": s["_id"], "source_item_id": guid,
          "url": url, "canonical_url": url, "title": title, "summary": summary,
          "published_at": published_at, "lang": s.get("default_lang","en"), "country": s.get("country"),
          "content_hash": sha1((url or "")+title+summary), "simhash": simh(text), "embedding": emb
        }, ["source_id","source_item_id"])

def main():
    with open("ingest/sources.yaml","r") as f:
        cfg = yaml.safe_load(f)
    with psycopg2.connect(PG_CONN) as con:
        con.set_session(autocommit=True)
        cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        for group, lst in cfg.items():
            for s in lst:
                cur.execute("""
                  insert into sources(key,name,type,country,default_lang,rss_url)
                  values (%s,%s,%s,%s,%s,%s)
                  on conflict(key) do update set name=excluded.name
                  returning id
                """, (s["key"], s["name"], group, s.get("country"), s.get("default_lang"), s.get("rss_url")))
                s["_id"] = cur.fetchone()["id"]
        for s in cfg.get("rss", []):
            ingest_rss(cur, s)

if __name__ == "__main__":
    main()
