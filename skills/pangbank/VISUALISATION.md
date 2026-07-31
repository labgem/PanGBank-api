# Visualisation

### Proksee / CGView — circular genome maps

Two routes, same format:

- **Ready-made, no download:** `GET /pangenomes/{pid}/{gid}/cgview_map` returns CGView.js v1.5.0 JSON — tracks `Gene` (outside), `RGP` and `Module` (inside); legend `persistent / shell / cloud / RNA / RGP / module_N`; each feature carries `meta.ID`, `meta.family`, and (in `GTDB_refseq`) `product` and gene `name`. The website exposes the same file with an "open in Proksee" button.
- **Your own genome:** `ppanggolin projection --proksee` (`PROJECTION.md`) or `ppanggolin write_genomes --proksee` emits `<genome>_proksee.json` in the identical structure.

Either file drags straight into <https://proksee.ca>. To add a custom track (e.g. an AMR ring), append to `cgview.tracks`, add a legend item, and duplicate the relevant features with a new `source` — the JSON is plain and editable.

### U-shaped plot and tile plot (presence/absence heatmap)

```bash
ppanggolin draw -p pangenome.h5 -o figs -f --ucurve --tile_plot --add_dendrogram
```

Produces `figs/Ushaped_plot.html` and `figs/tile_plot.html` — self-contained interactive **Plotly** pages. The U-curve shows families binned by the number of genomes carrying them, coloured by partition; the characteristic U (a persistent peak on the right, a cloud peak on the left, a shell trough between) is the visual signature of an open pangenome. `--add_dendrogram` clusters genomes on the tile plot, `--nocloud` drops cloud families, `--soft_core` sets the threshold.

**Scaling:** for a small pangenome (48 genomes × 918 families) these files are already 4.4 MB and 5.6 MB. At PanGBank scale (2,000 genomes × 59,000 families) the tile plot is unusable. Subset first, or build your own heatmap:

First produce the matrix — no command earlier in this file does it (schemas in `ANALYSIS.md`):

```bash
ppanggolin write_pangenome -p pangenome.h5 -o flat -f --Rtab --partitions
```

```python
import pandas as pd
from scipy.cluster.hierarchy import linkage, leaves_list
from scipy.spatial.distance import pdist
import seaborn as sns, matplotlib.pyplot as plt

m = (pd.read_csv("flat/gene_presence_absence.Rtab", sep="\t", index_col=0) > 0).astype(int)
freq = m.mean(axis=1)
acc = m[(freq > 0.05) & (freq < 0.95)]                       # accessory only: drop core and singletons
g = leaves_list(linkage(pdist(acc.T.values, metric="jaccard"), method="average"))
f = leaves_list(linkage(pdist(acc.values,   metric="jaccard"), method="average"))
sns.heatmap(acc.iloc[f, g], cmap="Blues", cbar=False, xticklabels=False, yticklabels=False)
plt.savefig("accessory_heatmap.png", dpi=150, bbox_inches="tight")
```

### Spot figures

```bash
ppanggolin draw -p pangenome.h5 -o figs --draw_spots --spots all   # or: --spots synteny, or spot_47
```

Per spot: an interactive `spot_N.html` aligning every RGP inserted at that locus, a `spot_N.gexf` subgraph, and `spot_N_identical_rgps.tsv`. This is the figure that shows what different strains have parked at the same chromosomal address.

### Linear view of one region (pyGenomeViz)

Proksee answers "where is it on the chromosome"; a linear gene-arrow plot answers "what is *in* this island, gene by gene". Export a genome's GFF carrying pangenome annotations, then draw the window of interest:

```bash
ppanggolin write_genomes -p pangenome.h5 --genomes GCF_000069245.1 --gff --add_metadata -o genome_out
```

```python
from pygenomeviz.parser import Gff                      # pip install pygenomeviz
gff = Gff("genome_out/gff/GCF_000069245.1.gff", target_seqid="NZ_CP...")
# each CDS carries partition=, family=, RGP/spot attributes, and any metadata you embedded (ANALYSIS.md)
# colour by the partition attribute; outline features whose family_<source>_* keys are present
```

This is how PanGBank's published use case renders the ~86 kb AbaR1 resistance island at spot_47: genes coloured orange/green/blue by partition, AMR-carrying genes outlined in red. It is the right figure whenever the question is about the *content* of one RGP or spot rather than about genome-wide structure.

### Pangenome graph in Gephi

```bash
ppanggolin write_pangenome -p pangenome.h5 -o flat --light_gexf     # flat/pangenomeGraph_light.gexf
```

**Always use `--light_gexf` at PanGBank scale.** The full `--gexf` adds one attribute per (family, genome) pair to every node *and* edge: for *E. coli* (59,165 families × 2,002 genomes) that is on the order of 10⁸ attribute values — tens of GB of XML that Gephi will not open. The light graph for the same pangenome is ~59 k nodes / 153 k edges, which Gephi handles.

Node attributes available for styling: `partition`, `subpartition`, `partition_exact`, `partition_soft`, `nb_genes`, `nb_genomes`, `name`, `product`, `type`, `length_avg`, `length_med`, `spot`, `module`. `viz:color` is preset per partition (persistent orange, shell green, cloud light blue) and `viz:size` = genome count. Edge weight = number of genomes sharing that adjacency.

Recipe: open as **undirected**; raise Gephi's heap (`-Xmx8g`) for 10⁵ nodes; layout with **ForceAtlas 2** (enable Approximate Repulsion, LinLog mode on, and **set Edge Weight Influence to 0** — otherwise the persistent backbone collapses to a point and hides all accessory structure), or **Yifan Hu Proportional** first for ≥ 50 k nodes; colour nodes by `partition`, size by `nb_genomes`, then partition/filter by `module` or `spot`. What you are looking for: a dense persistent backbone forming the chromosomal thread, with shell/cloud bubbles hanging off it at the insertion spots.

> **Parser trap:** PPanGGOLiN writes `xmlns="https://www.gexf.net/1.2draft"` with **https**, whereas the GEXF spec uses `http://`. Gephi tolerates it; **NetworkX does not** (`NetworkXError: No <graph> element in GEXF file`). Fix before loading in Python:
> ```bash
> sed 's|https://www.gexf.net|http://www.gexf.net|g' flat/pangenomeGraph_light.gexf > graph_fixed.gexf
> ```
> Cytoscape needs the `gexf-app` plugin. The per-spot `spot_N.gexf` files use the correct namespace and load in NetworkX directly.

---

