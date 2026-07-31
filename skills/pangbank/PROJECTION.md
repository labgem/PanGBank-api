# Flagship workflow — project a user's genome onto a PanGBank pangenome

This is the highest-value thing PanGBank enables: take a newly sequenced isolate and characterise it against 2,000 reference genomes **without rebuilding anything**. Every gene is assigned to a pangenome family (or flagged genome-specific), its RGPs are detected, and those RGPs are matched to the species' known insertion spots.

```bash
# 0. Install. Name every external binary explicitly — projection needs mmseqs2,
#    match-pangenome needs mash, and neither is pulled in by pangbank-cli alone.
conda create -n pangbank -c bioconda -c conda-forge pangbank-cli ppanggolin mash mmseqs2
conda activate pangbank

# 1a. Match only — `--no-download` is the CLI default, so this just names the pangenome.
#     Mash against a sketch built from the persistent families of every pangenome.
pangbank match-pangenome -c GTDB_refseq -i my_isolate.fna --outdir ./pg
#    -> stderr: "Genome 'my_isolate.fna' matches pangenome 's__...' with a distance of 0.0084"
#    -> stdout: the info block, including pangenome_id

# 1b. Size-check that id, tell the user the size, THEN download (the traps in SKILL.md trap 7 — mandatory).
API=https://pangbank-api.genoscope.cns.fr
curl -s -r 0-0 -D - -o /dev/null "$API/pangenomes/<ID>/file" | grep -i content-range
pangbank match-pangenome -c GTDB_refseq -i my_isolate.fna --download --outdir ./pg  # same --outdir
#    -> ./pg/GTDB_refseq_s__<taxon>_id<ID>.h5   (MD5-verified against the API's file_md5sum)
# Optional guard: make the reference copy read-only. Run `ppanggolin info` FIRST —
# PPanGGOLiN 2.2.6 opens the file r+ even to read it, so `info` fails under 444
# (verified). `write_pangenome` works fine under 444. And `cp` preserves the mode,
# so use `cp --no-preserve=mode` for any copy you intend to mutate. Full
# classification of read-only vs mutating subcommands in ANALYSIS.md.
chmod 444 ./pg/*.h5

# ⚠ Re-running any `--download` into this directory DELETES a file whose md5 no
# longer matches `file_md5sum` — and it deletes it BEFORE attempting the
# replacement, so a network failure at that moment leaves you with nothing.
# Since `ppanggolin metadata` rewrites a pangenome in place, an annotated
# pangenome left under its original name in `./pg` is destroyed by a re-download.
# Keep annotated copies under a different name, or in a different directory.

# 2. Project the genome onto it
ppanggolin projection \
    -p ./pg/GTDB_refseq_s__<taxon>_id<ID>.h5 \
    --fasta my_isolate.fna -n my_isolate \
    --gff --proksee --table \
    -o projection_out -c 8
```

Give `--anno my_isolate.gbff` instead of `--fasta` when the genome is already annotated (GBFF/GFF); the annotation then takes precedence and its gene calls are preserved. **Prefer `--anno` whenever an annotation exists, above all against `GTDB_refseq`, whose members carry the original NCBI gene calls** — mixing de-novo calls with curated ones moves the headline numbers: the same isolate on the same pangenome gives 98.41 % completeness and 16 genome-specific families via `--fasta`, but 96.65 % and 33 via `--anno`. Structural results (RGPs, spots, new spots) are unaffected. Always state which route you used. `--fast` aligns against family representatives only — faster, slightly less sensitive.

**Outputs** — everything lands in `projection_out/<genome_name>/` (only `summary_projection.tsv` is top-level):

| File | What it answers |
|---|---|
| `projection_summary.yaml` | the headline: completeness vs the pangenome, gene/family counts per partition, genome-specific families, RGPs, spots, modules, `New_spots` |
| `gene_to_gene_family.tsv` | which pangenome family each of the genome's genes belongs to |
| `specific_genes.tsv` | genes with no match anywhere in the pangenome — the genuinely novel content |
| `sequences_partition_projection.tsv` | per-sequence partition assignment (no-hit sequences are forced to cloud) |
| `regions_of_genomic_plasticity.tsv` | the genome's RGPs, with coordinates |
| `input_genome_rgp_to_spot.tsv` | which known spot each RGP inserts into, or `new_spot_N` — **every projected RGP resolves to one or the other**, unlike member genomes, where an RGP with non-conserved borders gets no spot at all |
| `new_spots_summary.tsv` | insertion loci not previously seen in the species |
| `modules_in_input_genome.tsv` | conserved modules present |
| `<genome>.gff`, `<genome>.tsv` | annotated genome carrying partition/family/RGP/spot attributes |
| `<genome>_proksee.json` | circular map, drag-and-drop into <https://proksee.ca> |

**Worked example, run end to end.** A *Mycoplasmoides gallisepticum* isolate (`GCA_054790465.1`) absent from the release, projected onto pangenome 12008 (48 genomes) in ~15 s on 4 CPUs: **98.41 % completeness**, 758 genes, 711 families, 16 genome-specific families, 13 RGPs — 9 at known spots, **4 at new spots**. A verdict of that shape is the deliverable — but only after the percentile comparison below; the raw counts alone do not support it.

**Is it typical? Measure it, do not assert it.** `GET /pangenomes/{pid}/genomes?limit=100` returns per-genome metrics whose JSON keys are the very names used in `projection_summary.yaml` (`RGPs`, `Spots`, `Persistent_families`, `Cloud_families`, `Completeness`, `Exact_core_families`, `Contigs`), so the projected genome can be placed field by field as a percentile of the member distribution. Report percentiles plus the member min–median–max, not bare counts. **Subtract `New_spots` from `Spots` before comparing**: a projected genome's `Spots` counts loci that members cannot have by construction, so the raw figure inflates the rank — on the worked example, 13 raw spots reads as "above every member" (member max 9), while the like-for-like 9 known spots simply ties the maximum. Report `New_spots` separately; it is the interesting number anyway. Two caveats: compare `Completeness` only against members annotated the same way (see `--anno` above), and note the isolate's `Contigs` against the members' — `RGPs` and `Spots` both rise with assembly contiguity.

**Requirements and traps**

- **MMseqs2 is mandatory** for projection (the alignment step) — failure is `FileNotFoundError: Command 'mmseqs' not found`. Mash is mandatory for `match-pangenome`.
- **Invoke `ppanggolin` through the activated environment's `PATH`, not by absolute path.** It shells out to sibling binaries (`mmseqs`, `aragorn`, `cmscan`) resolved from `PATH`; calling `/path/to/env/bin/ppanggolin` directly makes them invisible and the run dies mid-way.
- `match-pangenome` handles **one genome per invocation**, hardcodes `mash dist -p 1 -d 0.05` (single-threaded, 5 % max distance, neither configurable), and downloads the collection sketch on first use (~16 MB for `GTDB_refseq`, ~38 MB for `GTDB_all`), caching it and re-validating against `mash_sketch_md5sum`. Beyond that distance you get "no match" — the species is simply not in the collection. Read the returned distance, do not just accept the name: Mash distance approximates 1 − ANI, so the hardcoded 0.05 ceiling sits essentially on the ~95 % ANI species boundary. A hit at 0.005 is a confident same-species call; one at 0.045 is a borderline neighbour and should be reported as such.
- If the user already knows the species, skip Mash and search by taxon (the traps in SKILL.md). `match-pangenome` earns its keep when the taxonomy is unknown, uncertain, or — as GTDB reclassification regularly causes — *wrong*.

---

