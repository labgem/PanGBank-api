# What is inside the `.h5`, and what to run on it

Start with the cheap inventory — it prints genome/family/partition counts, fluidity, RGPs, spots and modules, and tells you which PPanGGOLiN version wrote the file:

```bash
ppanggolin info -p pangenome.h5 --content      # -s status, -a the exact build parameters, -m metadata sources
```

`info -a` is worth a look: it reports the thresholds PanGBank actually used (clustering identity 0.8 / coverage 0.8, partition beta 2.5 seed 42, RGP min_length 3000 min_score 4, spot set_size 3, module size 3 jaccard 0.85), which is what makes results comparable across the whole collection.

> **Read-only vs mutating.** `info`, `write_pangenome`, `write_genomes`, `write_metadata`, `draw` and `fasta` leave the file's content unchanged. Careful though: *leaving the content unchanged* is not *opening read-only* — `ppanggolin info` still opens the HDF5 in `r+` mode under 2.2.6 and therefore **fails with `PermissionError` on a `chmod 444` file** (verified), whereas `write_pangenome` succeeds. Run `info` before setting the file read-only. But `metadata`, `partition`, `rgp`, `spot`, `module`, `rarefaction` and `metrics --recompute_metrics` **write back into the `.h5` in place** — verified by md5 before/after. That breaks byte-identity with the API's `file_md5sum`. Work on a copy whenever you need a reproducible artifact, and never re-run `partition`/`rgp`/`spot`/`module`: those results are already in the file, and recomputing them is both expensive and destructive. Concrete guard: `chmod 444 pangenome.h5` on the download — a mutating command then fails cleanly with `PermissionError: file ... exists but it can not be written` instead of silently rewriting it.

Then export flat files. Every flag is independent; ask for what you need:

```bash
ppanggolin write_pangenome -p pangenome.h5 -o flat -f -c 8 \
    --stats --partitions --families_tsv --Rtab --csv \
    --regions --regions_families --spots --borders --modules --spot_modules
```

(`--regions_families` requires PPanGGOLiN ≥ 2.2.6. It is what produces `rgp_families.tsv`, without which the RGP → family joins below are impossible.)

| Output | Contents |
|---|---|
| `gene_presence_absence.Rtab` | binary matrix, **rows = families, columns = genomes** — the input for custom heatmaps and for recomputing a core on a subset |
| `matrix.csv` | Roary-style matrix |
| `gene_families.tsv` | **4 columns, no header**: family, gene ID, local identifier, `F` when the gene is a fragment (a naive `pandas.read_csv` with a header will silently mis-parse) |
| `partitions/*.txt` | one family ID per line: `persistent`, `shell`, `cloud`, plus `exact_core`, `exact_accessory`, `soft_core`, `soft_accessory`, and one file per shell subpartition (`S1.txt`, …) |
| `regions_of_genomic_plasticity.tsv` | RGPs: `region, genome, contig, genes, first_gene, last_gene, start, stop, length, coordinates, score, contigBorder, wholeContig` |
| `rgp_families.tsv` (`--regions_families`) | which gene families each RGP contains — the join key between RGPs and family-level annotation |
| `spots.tsv` | `spot_id, rgp_id` — **only the RGPs that were assigned to a spot.** An RGP whose flanking regions are not conserved gets no spot at all. The spot-less share varies enormously by species: 12 % of RGPs in pangenome 12008, but a clear majority in more fragmented pangenomes. Count them, never assume the mapping is total. |
| `summarize_spots.tsv` | `spot, nb_rgp, nb_families, nb_unique_family_sets, mean_nb_genes, …` |
| `spot_borders.tsv`, `border_protein_genes.fasta` | the flanking persistent families that define each spot |
| `functional_modules.tsv`, `modules_summary.tsv`, `modules_in_genomes.tsv`, `modules_spots.tsv` | panModule outputs |
| `genomes_statistics.tsv` | per-genome metrics; **header preceded by `#soft_core=…` comment lines that break naive parsing** |

> **Documentation trap:** `--regions`' help text claims it writes `plastic_regions.tsv`. It does not — the file is `regions_of_genomic_plasticity.tsv` in both 2.2.6 and 2.3.0 (the help string is stale). Do not send a user hunting for `plastic_regions.tsv`.

Sequence exports, for feeding external annotation tools:

```bash
ppanggolin fasta -p pangenome.h5 -o seq --prot_families all --compress   # seq/all_protein_families.faa.gz
ppanggolin fasta -p pangenome.h5 -o seq --genes all                      # nucleotide genes
```

Other useful subcommands: `metrics --genome_fluidity`; `write_genomes --gff --table --proksee` (per-genome files with pangenome annotations, `--genomes` to restrict); `msa` (family alignments — expensive); `context` (search the genomic neighbourhood of a gene of interest); `rgp_cluster` (cluster RGPs by shared gene content, emitting `gexf`/`graphml`); `rarefaction` (Heaps'-law openness curve — expensive, and the **only** route to a statement about openness; the API's fluidity is *not* a substitute, see the guardrails section of SKILL.md).

**Version compatibility:** PanGBank 2.x files are written by PPanGGOLiN **2.3.0**. PPanGGOLiN 2.2.6 reads them without complaint (verified: `info --content` reproduces the API metrics exactly), but install ≥ 2.3.0 when you can.

---

---

# Bringing the user's own data

**Annotate the gene families, then push the annotations into the pangenome.** Annotating at family level rather than per genome means one hit maps to one family instead of being recomputed across thousands of genomes.

```bash
# 1. one representative protein per family
ppanggolin fasta -p pangenome.h5 -o fam --prot_families all --compress -f

# 2. annotate with any external tool (AMRFinderPlus, eggNOG-mapper, abricate, InterProScan…)
amrfinder -p fam/all_protein_families.faa.gz --plus --threads 8 -o amrfinder_result.tsv

# 3. normalise the header: PPanGGOLiN needs a column named after the --assign target
sed -i 's/%/Prct/g; 1s/ /_/g; 1s/\bProtein_id\b/families/' amrfinder_result.tsv

# 4. embed into a COPY — `metadata` rewrites the file in place (the read-only/mutating note above)
cp pangenome.h5 pangenome_annotated.h5
ppanggolin metadata -p pangenome_annotated.h5 --metadata amrfinder_result.tsv \
    --source amrfinder --assign families
```

From then on the annotations **propagate automatically**: `write_genomes --gff --add_metadata` emits per-CDS `family_amrfinder_*` attributes, the tile plot can display them (`draw --add_metadata`), and `projection` carries them onto a newly projected genome — so a user's isolate inherits the species-wide annotation effort. `--assign` also accepts genomes, genes, RGPs, spots and modules, so the same mechanism attaches clinical metadata, isolation sources or phenotypes to genomes.

The official use case (*Acinetobacter baumannii*, `GTDB_refseq`, 2,002 genomes) follows exactly this path: 123 AMR-associated families (79 cloud, 39 shell, only 5 persistent — an accessory resistome), then RGP/spot aggregation identifying three hotspots present in > 500 genomes with > 40 % of their RGPs carrying AMR genes, spot_47 being the AbaR1 resistance island locus. A canine isolate projected onto that pangenome carries a compact 22 kb insertion at the same spot_47, with a Jaccard index of 1.0 against six human clinical strains — a One Health result obtained without building anything.

Useful joins: `gene_families.tsv` (gene ↔ family) → `rgp_families.tsv` (family ↔ RGP) → `spots.tsv` (RGP ↔ spot) → `functional_modules.tsv` (family ↔ module).

> **Join RGP → spot with a LEFT join, never an inner join.** Not every RGP belongs to a spot (see the `spots.tsv` row in the outputs table above), so an inner join silently discards part — sometimes most — of the plasticity. Label the unmatched rows `no_spot`, compute spot-level fractions against the **full** RGP set, and state how many RGPs are spot-less. `rgp_families.tsv` also repeats a family across rows when it has several gene copies in one RGP, so deduplicate on (rgp, family) before counting. Note that `projection` behaves differently: there every RGP is resolved either to a known spot or to a `new_spot_N`. To compare gene *content* rather than locus, compute a Jaccard index between RGP family sets — spot membership is defined by flanking genes, so two RGPs at one spot may share nothing.

**Close the loop back to biology.** Once a Jaccard comparison names the pangenome genomes most similar to the user's isolate, resolve those accessions to their sample metadata — host, isolation source, geography — with the NCBI Datasets CLI (`datasets summary genome accession <ACC>`), and to their PanGBank pages via `https://pangbank.genoscope.cns.fr/pangenome/{pid}/genome/{gid}` (get `{gid}` from `/pangenomes/{pid}/genomes?genome_name=<ACC>`). That final step is what turns "these six RGPs are identical" into "an identical resistance island circulates between a canine isolate and human respiratory isolates on three continents".

---

