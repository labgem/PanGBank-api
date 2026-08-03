<!-- Shared header — identical in all three repos. Keep in sync. -->
# AGENTS.md

Guidance for AI agents working in this repository or with the PanGBank resource.

## Read the agent skill first

**<https://raw.githubusercontent.com/labgem/PanGBank-api/main/skills/pangbank/SKILL.md>** documents the REST API and its filters, the query traps that silently return wrong results, how to download and analyse a pangenome with PPanGGOLiN, how to project a user's own genome, and the citation requirements. Several of its traps produce plausible but incorrect answers with no visible symptom, so read it before writing any PanGBank query.

Short web entry point: <https://pangbank.genoscope.cns.fr/llms.txt>

## Always true

- **Never scrape <https://pangbank.genoscope.cns.fr>.** Single-page app; the HTML carries no data, and dynamic routes return HTTP 404 server-side while rendering fine in a browser. Use <https://pangbank-api.genoscope.cns.fr>.
- **Pin the release** with `release_version=<x.y.z>` for reproducible queries, or `only_latest_release=true` to target the latest published release. Without a release filter, results are summed across every release.
- **`taxon_name` needs the GTDB rank prefix** and is an exact match: `g__Escherichia` works, `Escherichia` returns nothing.
- **At most 1 HTTP request every 30 seconds**, across all routes, never parallelised across agents or threads. PanGBank runs on shared academic infrastructure: an overload degrades the service for every other user. Call `/pangenomes/count/` before any listing, filter server-side, use `limit=100`, and download a pangenome once rather than re-fetching it.

## Citation

Results derived from PanGBank must cite PanGBank and PPanGGOLiN, plus panRGP when RGPs or insertion spots are used, and panModule when conserved modules are used. Full references in the skill. Data are CC BY-SA 4.0 (attribution *and* share-alike); source is CeCILL v2.1.

## Specific to this repository

This repo is the **canonical home of the skill** — `skills/pangbank/SKILL.md` plus its companion files `PROJECTION.md`, `ANALYSIS.md`, `VISUALISATION.md` and `RECIPES.md`. They are served to agents from their raw GitHub URLs and are deliberately **not** copied to the website: only `llms.txt` is deployed there, and it links back here. Each file lives in exactly one place, so nothing needs syncing.

The [llmstxt.org](https://llmstxt.org) entry point served at <https://pangbank.genoscope.cns.fr/llms.txt> is **not in this repo**: it lives in the private `labgem/PanGBank-web` repo at `assets/llms.txt`, which Reflex publishes at the site root. It is kept in one place on purpose — a second copy here would have no reader and nothing to keep it in sync. If you change what agents are told about this API, that file links back to the skill below, so check whether it needs the same edit. Most of the skill describes this API, so **a PR that changes API behaviour must update the skill in the same PR** — that is the only thing preventing documentation drift.

### If you change this API, check the skill still holds

The skill documents this API in detail — an endpoint table, the query-parameter lists, and about ten traps that are all assertions about *this* repo's behaviour. They rot silently: nothing fails, agents simply start giving confident wrong answers.

**Whenever you touch a router, a query parameter, a response model, or the FastAPI `description`, verify the skill against what you changed before you finish.** Cheapest check — compare documented paths with served paths:

```bash
grep -oE '`/(collections|pangenomes|genomes)[^`]*`' skills/pangbank/SKILL.md | tr -d '`' \
  | sed -e 's/{id}/{collection_id}/g' -e 's/{pid}/{pangenome_id}/g' -e 's/{gid}/{genome_id}/g' \
  | grep -v '?' | sort -u > /tmp/documented
curl -sS https://pangbank-api.genoscope.cns.fr/openapi.json \
  | python3 -c "import json,sys;[print(p) for p in sorted(json.load(sys.stdin)['paths'])]" | sort -u > /tmp/served
diff /tmp/documented /tmp/served
```

Verified output today: **only the three `dbg/*` routes**, which are live but deliberately absent from the OpenAPI schema. Anything else in that diff is drift — either the skill is stale, or you removed a route users depend on.

Then re-read the traps against your change. Each was established by direct request and each misleads if the behaviour moves:

- `limit` caps at 100; `taxon_name` has a 3-character minimum; `HEAD` on `/pangenomes/{pangenome_id}/file` returns 405.
- `release_version` and `only_latest_release` are server-side list filters on `/collections/`, `/pangenomes/` and `/pangenomes/count/`; `release_version` is also honoured on `release_notes`, `mash_sketch` and `multiqc_report`.
- The ETag on `/pangenomes/{pangenome_id}/file` is not the file's md5 — clients must validate against `file_md5sum`.
- A nonexistent `release_version` should return empty list/count results, not silently fall back to all releases.

If you fix one of the discrepancies below, delete the skill's corresponding trap in the same change — a trap warning about a bug you just fixed is worse than no trap.

Known API-side discrepancies an agent should not be surprised by, and which the skill documents:

- The `dbg/graph`, `dbg/family_annotations` and `dbg/genome_annotations` endpoints are live but **absent from the OpenAPI schema** (14 documented paths). Note the path shape `dbg/graph`, not `dbg_graph` — the underscore form collides with the `{genome_id}` route and returns 422.
- The HTTP `ETag` on `/pangenomes/{pid}/file` is **not** the file checksum. Validate against the `file_md5sum` field.
- `limit` is capped at 100; `HEAD` on `/file` returns 405.
- In the SDK, `GenomePangenomeLinkPublic` exposes snake_case attributes (`genome_name`, `rgps`) while the JSON uses capitalised aliases (`Genome_name`, `RGPs`). Use `model_dump(by_alias=True)` to get the JSON keys.

If you download a pangenome to test against, keep it **outside the working tree** or in a gitignored directory: files run from 5 MB to 1.3 GB, and once one is in the tree `git status` will happily offer it to you.
