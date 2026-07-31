# Copy-paste recipes

Every trap in SKILL.md applies to these — read them first.

## Rate limit — one request every 30 seconds

The service runs on shared academic infrastructure (see SKILL.md). Every recipe below must respect **1 HTTP request per 30 seconds**. That makes request count the thing to optimise:

- **`/pangenomes/count/` before any listing** — it is one request and often makes the listing unnecessary.
- **Filter server-side** (`taxon_name`, `collection_name`, `only_latest_release`) instead of fetching broadly and filtering locally.
- **`limit=100` always**, the maximum: a 2,044-pangenome collection is 21 requests, not 103 — and 21 requests is 10 minutes at this rate, so say so before starting.
- **Download a pangenome once**; re-read the local copy instead of re-fetching.
- Never run several agents against the API in parallel: the limit is global, not per agent.

```bash
API=https://pangbank-api.genoscope.cns.fr   # always -G --data-urlencode: taxon names contain spaces

# Collections and releases (do this once, first)
curl -s "$API/collections/" | jq -r '.[] | .name as $c | .releases[] |
  "\($c)\t\(.version)\tlatest=\(.latest)\t\(.pangenome_count) pangenomes\t\(.taxonomy_source.version)"'

# How many pangenomes for a genus, current releases only?
curl -s -G "$API/pangenomes/count/" \
  --data-urlencode "taxon_name=g__Klebsiella" --data-urlencode "only_latest_release=true"

# One species, collection and release pinned
curl -s -G "$API/pangenomes/" \
  --data-urlencode "taxon_name=s__Klebsiella pneumoniae" \
  --data-urlencode "collection_name=GTDB_refseq" \
  --data-urlencode "only_latest_release=true" \
| jq -r '.[] | "id=\(.id)\t\(.name)\tv\(.collection_release.version)\tgenomes=\(.genome_count)\tfamilies=\(.family_count)\tpers=\(.persistent_family_count) shell=\(.shell_family_count) cloud=\(.cloud_family_count)\tRGP=\(.rgp_count) spots=\(.spot_count) modules=\(.module_count)"'

# Which pangenomes contain this genome? (never infer it from taxonomy)
curl -s -G "$API/pangenomes/" --data-urlencode "genome_name=GCF_000005845.2" \
| jq -r '.[] | "\(.id)\t\(.name)\t\(.collection_release.collection_name) v\(.collection_release.version)"'

# Every member genome of a pangenome, paginated — 21 requests for a 2,000-genome
# species, so ~10 minutes at the rate limit. Announce that before starting.
PID=11587
TOTAL=$(curl -s "$API/pangenomes/$PID" | jq .genome_count)
for off in $(seq 0 100 "$TOTAL"); do
  sleep 30                                              # rate limit: 1 request / 30 s
  curl -s -G "$API/pangenomes/$PID/genomes" \
    --data-urlencode "limit=100" --data-urlencode "offset=$off" \
  | jq -r '.[] | [.Genome_name,.Genes,.Families,.Persistent_families,.Shell_families,
                  .Cloud_families,.RGPs,.Spots,.Modules,.Completeness,.Contamination] | @tsv'
done

# Size check, then resumable download, then integrity check
PID=11587
curl -s -r 0-0 -D - -o /dev/null "$API/pangenomes/$PID/file" | grep -i content-range
curl -L -C - -o pangenome.h5 "$API/pangenomes/$PID/file" && md5sum pangenome.h5   # == file_md5sum
```

```bash
# Same things with the CLI
pangbank list-collections -l | cat                      # TSV when piped, rich table on a TTY
pangbank search-pangenomes -c GTDB_refseq -l -t g__Vibrio --exact-match > vibrio.tsv
pangbank search-pangenomes -l -g GCF_000005845.2        # which pangenome holds this genome
pangbank get-pangenome 12767 --download --outdir ./pg
# ⚠ a `--download` into a directory that already holds a pangenome whose md5 no longer
# matches `file_md5sum` DELETES that file first, before trying to replace it. Annotated
# pangenomes (`ppanggolin metadata` rewrites in place) must not sit there under the original name.

# Canonical reproducible download (the form used in PanGBank's own published use case):
# pin the release explicitly rather than taking "whatever is latest today".
pangbank search-pangenomes \
    --collection GTDB_refseq \
    --taxon "s__Acinetobacter baumannii" \
    --release-version 2.0.0 \
    --download
# -> pangbank/GTDB_refseq_s__Acinetobacter_baumannii_id10832.h5

ppanggolin info -p ./pangbank/GTDB_refseq_s__Acinetobacter_baumannii_id10832.h5 --content
```

The CLI cannot report a file size and the API record carries no size field, so range-check `/pangenomes/{pid}/file` and state the size before any `--download` (trap 7).

**Pinning: `-l` and `--release-version` answer different questions.** `-l/--latest-only` means "latest *at run time*" — correct for "what is current", but **not reproducible**: rerun it after the next release and you silently get a different pangenome with different ids. `--release-version` pins an explicit release and is what published work should use. Passing neither returns every release at once (trap 1). The CLI applies `--release-version` client-side, which is why it works where the API parameter of the same name does not.

```python
# Bulk survey with the SDK: every pangenome of a collection at its latest release
from pangbank_api.sdk import PanGBankClient
import pandas as pd

with PanGBankClient(timeout=60) as client:
    n = client.pangenomes.count(collection_name="GTDB_refseq", only_latest_release=True)
    rows, off = [], 0
    while off < n:                                        # limit is capped at 100
        time.sleep(30)                                    # rate limit: 1 request / 30 s
        rows += client.pangenomes.list(collection_name="GTDB_refseq",
                                       only_latest_release=True, limit=100, offset=off)
        off += 100
    df = pd.DataFrame([{
        "id": p.id, "name": p.name, "genomes": p.genome_count, "families": p.family_count,
        "persistent_fraction": p.persistent_fraction, "cloud_fraction": p.cloud_fraction,
        "all_genome_fluidity": p.all_genome_fluidity, "rgps": p.rgp_count, "spots": p.spot_count,
    } for p in rows])
```

---

