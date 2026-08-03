---
name: pangbank
description: Obtain and analyse precomputed microbial pangenomes from PanGBank (https://pangbank.genoscope.cns.fr), the Genoscope/LABGeM collection of PPanGGOLiN pangenomes covering >4,600 prokaryotic species. Use this skill to find the right pangenome for a species or for a user-supplied genome, download it, project a new genome onto it, integrate the user's own annotations or metadata, and produce the standard analyses and visualisations (Proksee/CGView maps, U-shaped plot, presence/absence heatmaps, spot figures, pangenome graphs in Gephi). Trigger whenever a task involves a reference pangenome, core/accessory genome structure, regions of genome plasticity, insertion spots, conserved modules, or placing a newly sequenced isolate in a pangenomic context.
---

# PanGBank

PanGBank serves **precomputed, species-level pangenomes** built with PPanGGOLiN over GTDB species clusters. One species (≥ 15 genomes) = one partitioned pangenome graph, delivered as a single self-contained **HDF5 file** that already contains gene annotations, gene families, the pangenome graph, partitions (persistent/shell/cloud), RGPs, insertion spots, conserved modules, per-genome metadata and precomputed fluidity metrics.

**The point is to skip pangenome construction.** Never rebuild a species pangenome from scratch if PanGBank has it; downloading takes seconds to minutes, building takes hours to days.

---

## Which file to read

This file holds what applies to **every** task: how to reach the data, how to choose a collection, the traps that silently produce wrong answers, how to read the numbers, and what to cite. Read it first, in full.

Procedures live in four companion files — on disk next to this one, or at the same base URL `https://raw.githubusercontent.com/labgem/PanGBank-api/main/skills/pangbank/<FILE>` if you fetched this from the web. Read the one your task needs rather than all four, but **follow any pointer a file gives you**.

| Your task | Read |
|---|---|
| Place a user's own genome in its species pangenome | **`PROJECTION.md`** — the flagship workflow: match by Mash, download, `ppanggolin projection`, interpret the result |
| Extract flat files from a downloaded `.h5`, or integrate your own annotations into it | **`ANALYSIS.md`** — exports, output-file schemas, the family → RGP → spot chain, `ppanggolin metadata` |
| Produce figures | **`VISUALISATION.md`** — Proksee/CGView maps, U-shaped plot, presence/absence heatmaps, spot figures, pangenome graphs in Gephi |
| Choose which pangenome to work on, or survey a whole collection (including under a disk budget) | **`RECIPES.md`** — the paginated survey loop |
| Copy-paste a query | **`RECIPES.md`** — curl, CLI and Python SDK |

## Rate limit — one request every 30 seconds

PanGBank runs on **shared academic infrastructure**: an overload degrades the service for every other user.

> **Never send more than 1 HTTP request per 30 seconds**, across every route: API calls, file downloads, CGView maps. Sleep between calls, and never parallelise requests across agents or threads.

Call `/pangenomes/count/` before any listing, filter server-side, always use `limit=100`, and download a pangenome once rather than re-fetching it. If a task genuinely needs hundreds of requests, say it will take tens of minutes and let the user decide. Techniques in `RECIPES.md`.

## Access rule: use the API, never scrape the website

`https://pangbank.genoscope.cns.fr/` is a Reflex/React single-page app. **The served HTML contains no data.** Dynamic routes such as `/pangenome/12767` return **HTTP 404 from the server** while rendering correctly in a browser (client-side routing after hydration). Never scrape it, and never treat its status code as evidence that a record is missing.

```
REST API   https://pangbank-api.genoscope.cns.fr     # public, no auth, no key, no header
Swagger    https://pangbank-api.genoscope.cns.fr/    # OpenAPI schema at /openapi.json
```

Three clients, all backed by that API — pick per task:

| Route | Install | Best for |
|---|---|---|
| **REST + curl/jq** | — | one-off lookups, exact control, scripting in any language |
| **Python SDK** | `pip install "pangbank-api[sdk]"` | notebooks, typed objects, bulk surveys |
| **CLI** | `pip install pangbank-cli` or `conda install -c bioconda pangbank-cli` | download workflows, and the only route to `match-pangenome` |

PanGBank 2.x `.h5` files are written by **PPanGGOLiN 2.3.0**; 2.2.6 reads them without complaint, but install ≥ 2.3.0 when you can. Per-flag minimum versions are noted in `ANALYSIS.md`.

---

## Data model and choice of collection

```
Collection ──< CollectionRelease ──< Pangenome ──< GenomePangenomeLink >── Genome
 GTDB_refseq      2.0.0, 1.0.0      one GTDB species    per-genome metrics    one assembly
 GTDB_all         2.1.0, 2.0.0,…    × one release       inside THAT pangenome  accession
```

A species exists once **per collection × per release**. Integer ids are **not stable across releases**.

### Which collection — this decides what analyses are possible

| | `GTDB_refseq` (id 1) | `GTDB_all` (id 2) |
|---|---|---|
| Source | RefSeq only | RefSeq + GenBank |
| Retrieved as | **GenBank format — original gene calls and functional annotation preserved** | FASTA — **re-annotated de novo, uniformly** |
| Composition | almost exclusively isolates | ~40 % MAGs/SAGs |
| Scale (latest) | 2,044 pangenomes / 219,611 genomes | 4,679 pangenomes / 382,760 genomes; 55 phyla; ~150 archaeal |
| Choose it for | functional work, anything needing gene names or products | taxonomic breadth, environmental and uncultured lineages |

This is decisive, and it is verifiable on the *same* genome. `GCF_000008865.2` in `GTDB_refseq` pangenome 11587: all 5,281 genes carry a `product`, 3,998 carry a gene name (`thrL`, locus tags `ECs_0001`). The identical genome in `GTDB_all` pangenome 14510: **zero products**, identifiers like `GCF_000008865.2_CDS_0001`.

> **If the question touches gene function, use `GTDB_refseq`.** If it needs coverage of uncultured lineages, use `GTDB_all` and check the MAG/SAG fraction before interpreting.

State on 2026-07-29 — **re-read `/collections/`, never trust these figures**; earlier releases stay served and queryable.

| Collection | Latest release | Date | Pangenomes | Genomes | PPanGGOLiN | Taxonomy |
|---|---|---|---|---|---|---|
| GTDB_refseq | 2.0.0 | 2026-06-04 | 2,044 | 219,611 | 2.3.0 | GTDB 11-RS232 |
| GTDB_all | 2.1.0 | 2026-07-15 | 4,679 | 382,760 | 2.3.0 | GTDB 11-RS232 |

### How the pangenomes were built (this constrains interpretation)

A Nextflow/nf-core pipeline (`PanGBank-wf`): **(i)** quality filter — completeness = max(CheckM1, CheckM2); genomes < 70 % dropped, species dropped when the GTDB representative is < 85 %; **(ii)** **merged split GTDB species** sharing a base name (`s__Proteus_vulgaris`, `_B`, `_C`) when mean Skani ANI ≥ 95 % and AF ≥ 50 % — these carry `has_multiple_species` (*Merged sp* on the site); **(iii)** **dereplication above 2,000 genomes** (Mash → QuickTree NJ tree → 2,000 clusters → highest-completeness representative, reference genomes reinstated); **(iv)** `ppanggolin all`, then `metrics --genome_fluidity`, `info --content`, `write_metadata`.

Consequences: a count near 2,000 genomes means **dereplicated, not exhaustive** (only 24 species in `GTDB_refseq`, 27 in `GTDB_all`); both collections are dominated by pangenomes of < 50 genomes; and **flat files, Rtab matrices and GEXF graphs are not precomputed** — only the `.h5`, per-genome Proksee maps, statistics and a MultiQC report. You generate the rest from the `.h5` yourself (exports in `ANALYSIS.md`; figures in `VISUALISATION.md`).

---

## Endpoints

| Method | Path | Returns |
|---|---|---|
| GET | `/collections/` | collections with all releases |
| GET | `/collections/{id}` | one collection |
| GET | `/collections/{id}/release_notes` | changelog, `text/plain` — **read before comparing releases** |
| GET | `/collections/{id}/multiqc_report` | build QC report (`text/html`, ~8 MB) |
| GET | `/collections/{id}/mash_sketch` | Mash sketch of the release, built from persistent-family sequences |
| GET | `/pangenomes/` | pangenome list (paginated) |
| GET | `/pangenomes/count/` | **bare integer**, same filters, no pagination |
| GET | `/pangenomes/{pid}` | one pangenome, all metrics |
| GET | `/pangenomes/{pid}/file` | the **PPanGGOLiN HDF5** |
| GET | `/pangenomes/{pid}/genomes` | member genomes + per-genome metrics (paginated) |
| GET | `/pangenomes/{pid}/{gid}` | one genome's metrics inside that pangenome |
| GET | `/pangenomes/{pid}/{gid}/cgview_map` | CGView.js JSON map |
| GET | `/genomes/` | genomes (paginated) |
| GET | `/genomes/{gid}` | one genome: CheckM/CheckM2, GC, N50/L50, **all** taxonomies, statuses |

Filters on `/pangenomes/` and `/pangenomes/count/`: `taxon_name`, `substring_taxon_match`, `pangenome_name`, `genome_name`, `collection_name`, `collection_id`, `only_latest_release`, `release_version`, `offset`, `limit` (≤ 100). On `/genomes/`: `taxon_name`, `substring_taxon_match`, `genome_name`, `offset`, `limit`. On `/pangenomes/{pid}/genomes`: `genome_name`, `offset`, `limit`.

There is **no filter on metrics** — fetch, then filter locally.
## Traps — every one verified by direct request

**1. Without `only_latest_release=true`, results sum across releases.** `/pangenomes/count/` alone → 17,523; with the flag → 6,723. "5 *E. coli* pangenomes" is meaningless: that is 5 (collection × release) pairs. Always pin the release dimension, and say how in your answer.

**1b. `release_version` now works as a server-side list filter.** On `/pangenomes/`, `/pangenomes/count/` and `/collections/`, use `release_version` to pin an explicit release during query time. A nonexistent version returns no result (`[]` / `0`) instead of silently falling back to all releases.

The same parameter **does** work on the per-release artifact endpoints, where it selects which release's file to serve: `/collections/{id}/release_notes` returns different content per version, and `/collections/{id}/mash_sketch` returns 14.5 MB for v1.0.0 versus 16.7 MB for v2.0.0. Both default to the latest release when omitted.

**2. `taxon_name` is exact and needs the GTDB rank prefix.** `g__Escherichia` → 36; `Escherichia` → **0**; `Escherichia` with `substring_taxon_match=true` → 47. Prefixes `d__ p__ c__ o__ f__ g__ s__`; species carry a **space**: `s__Escherichia coli`. Minimum 3 characters (else HTTP 422). Searching a higher rank descends the tree: `f__Enterobacteriaceae` + latest → 378.

**3. GTDB taxonomy moves genomes between releases — never infer a genome's pangenome.** `GCF_000005845.2` (*E. coli* K-12 MG1655) is `s__Escherichia coli` under GTDB R226 but `g__G047199095 / s__G047199095 sp047199095` under R232. In `GTDB_refseq` 2.0.0 it therefore sits in pangenome 11651, **not** the *E. coli* one — and `match-pangenome` reproduces this independently, matching K-12 to `s__G047199095_sp047199095` at distance 0.0084. Always resolve a genome with `/pangenomes/?genome_name=<accession>`.

**4. Pangenome name ≠ taxon name.** Taxon `s__Escherichia coli` → pangenome `s__Escherichia_coli_0` (spaces → `_`, numeric suffix from species splitting/merging). `pangenome_name` is an exact match with no substring support. Search organisms via `taxon_name`.

**5. `genome_id` is an internal integer; `genome_name` is the accession.** `GCF_000005845.2` is id 1. `/pangenomes/{pid}/{gid}` expects the integer.

**6. `limit` caps at 100** (422 above), default 20. Paginate with `offset` (stable id order); call `/pangenomes/count/` first.

**7. HDF5 files are large.** *E. coli* `GTDB_refseq` = **1.33 GB**; a *Vibrio* = 231 MB; a small *Mycoplasmoides* = 5 MB. **State the size and get the user's agreement before downloading.** `HEAD` is rejected on `/file` (405) — read `content-range` with `GET -r 0-0`. `accept-ranges: bytes` allows resume. **A `--download` into a directory already holding a pangenome whose md5 no longer matches `file_md5sum` deletes that file before attempting the replacement** — so an annotated pangenome kept under its original name is destroyed, with nothing in its place if the network then fails.

**8. Validate downloads against `file_md5sum`, never against the HTTP `ETag`.** The two differ: for pangenome 12008 the ETag is `00017d43…` while `file_md5sum` is `e77cd293…`, which is what `md5sum` on the downloaded file actually returns. The ETag is a server-side file handle derived from mtime and size, not a content checksum.

**9. CLI flag semantics are inverted relative to the API.** `pangbank search-pangenomes` defaults to *substring* matching (`--exact-match` opts into exact), the opposite of the raw API. And `--latest-only` defaults to **False**, so always pin the release — with `-l` for "what is current", or `--release-version` for reproducible work. Beware too that the CLI's TSV `name` column holds the **deepest taxon name, with spaces**, whereas the API/SDK `name` field holds the **underscored pangenome name** — joining CLI output to API results on `name` silently fails; join on `pangenome_id`.

**10. Three endpoints are live but absent from the OpenAPI schema.** `/openapi.json` documents 14 paths; the De Bruijn graph index is not among them, yet it serves:

| Path | File | Size (example: pid 12767) |
|---|---|---|
| `/pangenomes/{pid}/dbg/graph` | `<name>.dbg` | 36 MB |
| `/pangenomes/{pid}/dbg/family_annotations` | `<name>.row_diff_brwt.annodbg` | 8.9 MB |
| `/pangenomes/{pid}/dbg/genome_annotations` | `<name>_genomes.row_diff_brwt.annodbg` | 45 MB |

These are **MetaGraph** indexes (row-diff BRWT annotations), enabling sequence-level queries against the pangenome — searching reads or a query sequence directly against the graph, with hits resolvable to gene families or to genomes. They are the substrate for metagenomic profiling against PanGBank. Reach them via the SDK's `download_dbg_graph` / `download_dbg_family_annotations` / `download_dbg_genome_annotations`, or by direct URL. Note the path shape is `dbg/graph`, **not** `dbg_graph` — the underscore form collides with the `{genome_id}` route and returns 422. The SDK's `download_graph_tool` has no live counterpart.
## Interpretation guardrails

- **Partitions come from a statistical mixture model**, not frequency thresholds. `Exact_core` and `Soft_core` are the threshold-based measures and are reported separately.
- **Partitions are not transferable to a subset of genomes.** If the user restricts the genome set, recompute from `gene_presence_absence.Rtab` (produce it with `write_pangenome --Rtab`, see `ANALYSIS.md`); do not filter the partition files.
- **Strict core collapses when fragmentary assemblies are present** — one incomplete genome removes a family from the exact core. Report the soft core.
- **Spot membership is locus-based, not content-based** (defined by conserved flanking persistent genes). Never claim two RGPs in the same spot carry the same genes without checking.
- **Per-genome metrics are relative to the queried pangenome**: the same genome differs between `GTDB_refseq` and `GTDB_all`.
- **Raw counts scale with `genome_count` and with sampling.** Compare scale-free quantities and check `has_multiple_species`, `genome_category_counts`, `mean_completeness` and `mean_contamination` before drawing biology. A `GTDB_all` pangenome dominated by MAGs has inflated shell/cloud from partial genomes — and a `GTDB_refseq` one can still be MAG-heavy, so check per pangenome, not per collection. **A `has_multiple_species` pangenome lumps several GTDB species** (merged at ANI ≥ 95 %, AF ≥ 50 %); its fluidity, shell and cloud are inflated accordingly. When comparing taxa, report how many merged pangenomes each group contains and repeat the comparison without them.
- **Know which fraction you are reporting.** `persistent_fraction` / `shell_fraction` / `cloud_fraction` are each partition's share of an **average genome's** family content and sum to exactly 1 (they equal `mean_<part>_families_count_per_genome / average_families_per_genome`). They are **not** `<part>_family_count / family_count`, the partition's share of the pangenome repertoire. For pangenome 12008 the two readings give 0.90 vs 0.68 for persistent and 0.027 vs 0.218 for cloud. Both are legitimate; always say which.
- **Fluidity is not openness.** `all_genome_fluidity` is the mean pairwise gene-content dissimilarity between genomes; openness is the rate at which the family repertoire grows as genomes are added. The API exposes no rarefaction or Heaps parameter, so an openness claim requires `ppanggolin rarefaction` on the `.h5`. If you use fluidity as a proxy, say so and say what it measures — the two can rank taxa in opposite directions.
- **Four fluidity fields — pick deliberately.** `all_genome_fluidity` (all families) is the default for cross-taxon comparison. `accessory_genome_fluidity` covers turnover *within* the shell+cloud pool and can show no difference where `all_genome_fluidity` differs strongly — they answer different questions. `shell_genome_fluidity` / `cloud_genome_fluidity` are per-partition and sum to nothing. Name the field you used.
- **Projection `Completeness` and genome-specific family counts are annotation-method dependent** (`--fasta` de-novo calls versus `--anno` curated calls); RGP, spot and new-spot counts are robust to the choice. Never compare a `--fasta` completeness against an `--anno` one.
- A genome count near 2,000 means the species was **dereplicated**; do not present it as the full public sample.
- Always qualify an identifier with its collection **and** release: "pangenome 11587 (`GTDB_refseq` v2.0.0, GTDB R232)".

Browsable links to give the user (server-side 404 is expected and harmless):

```
https://pangbank.genoscope.cns.fr/pangenome/{pangenome_id}
https://pangbank.genoscope.cns.fr/pangenome/{pangenome_id}/genome/{genome_id}
https://pangbank.genoscope.cns.fr/genome/{genome_id}
https://pangbank.genoscope.cns.fr/collection/{collection_id}/{version}
```

---

## Citation — always tell the user

**Whenever results derived from PanGBank are used in a report, manuscript, thesis or figure, state the citations explicitly and unprompted.** Do not leave it to the user to remember. Cite conditionally, according to what was actually used:

| When | Cite |
|---|---|
| **Always** — any pangenome, any partition, any projection | **PanGBank** (the resource) and **PPanGGOLiN** (Gautreau *et al.* 2020) |
| RGPs, genomic islands or **insertion spots** were used | add **panRGP** (Bazin *et al.* 2020) |
| Conserved **modules** were used | add **panModule** (Bazin *et al.* 2021) |

> **PPanGGOLiN** — Gautreau G, Bazin A, Gachet M, Planel R, Burlot L, Dubois M, Perrin A, Médigue C, Calteau A, Cruveiller S, Matias C, Ambroise C, Rocha EPC, Vallenet D (2020). *PPanGGOLiN: Depicting microbial diversity via a partitioned pangenome graph.* PLOS Computational Biology 16(3): e1007732. doi:[10.1371/journal.pcbi.1007732](https://doi.org/10.1371/journal.pcbi.1007732)
>
> **panRGP** — Bazin A, Gautreau G, Médigue C, Vallenet D, Calteau A (2020). *panRGP: a pangenome-based method to predict genomic islands and explore their diversity.* Bioinformatics 36(Suppl_2): i651–i658. doi:[10.1093/bioinformatics/btaa792](https://doi.org/10.1093/bioinformatics/btaa792)
>
> **panModule** — Bazin A, Gautreau G, Médigue C, Vallenet D, Calteau A (2021). *panModule: detecting conserved modules in the variable regions of a pangenome graph.* bioRxiv. doi:[10.1101/2021.12.06.471380](https://doi.org/10.1101/2021.12.06.471380)
>
> **PanGBank** — Mainguy J, Lemane T, Bazin A, Arnoux J, Gautreau G, Médigue C, Calteau A, Vallenet D. *PanGBank: a comprehensive collection of microbial pangenomes built with PPanGGOLiN.* Ask the user for the current reference (journal, year, DOI) rather than inventing one; cite the resource URL <https://pangbank.genoscope.cns.fr> in the interim.

Also cite the third-party tools actually run — GTDB for the taxonomy, and any external annotator (AMRFinderPlus, eggNOG-mapper, …), Mash, MMseqs2 as applicable.

**Licence.** PanGBank data are distributed under **CC BY-SA 4.0** (as is the GTDB taxonomy it adapts) — attribution *and* share-alike, which propagates to redistributed derivatives. The `PanGBank-wf`, `PanGBank-api` and `PanGBank-cli` source code is CeCILL v2.1, as is PPanGGOLiN.

---

## Resources

Web <https://pangbank.genoscope.cns.fr> · API <https://pangbank-api.genoscope.cns.fr> · tutorials <https://github.com/labgem/PanGBank-tutorial> · PPanGGOLiN <https://ppanggolin.readthedocs.io/>
