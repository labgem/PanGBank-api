# PanGBank SDK

Python client for the PanGBank API, with synchronous and asynchronous
variants. Built on [httpx](https://www.python-httpx.org/) and
[pydantic](https://docs.pydantic.dev/) models.

## Installation

```bash
pip install pangbank-api[sdk]
```

The SDK requires `httpx` to be installed; importing `pangbank_api.sdk`
without it raises an `ImportError` with installation instructions.

## Quick start

### Synchronous

```python
from pangbank_api.sdk import PanGBankClient

with PanGBankClient(base_url="https://pangbank-api.genoscope.cns.fr") as client:
    collections = client.collections.list()
    genomes = client.genomes.list(taxon_name="Escherichia coli")
    pangenome = client.pangenomes.get(pangenome_id=1)
```

### Asynchronous

```python
import asyncio
from pangbank_api.sdk import AsyncPanGBankClient

async def main():
    async with AsyncPanGBankClient(base_url="https://pangbank-api.genoscope.cns.fr") as client:
        collections = await client.collections.list()
        genomes = await client.genomes.list(taxon_name="Escherichia coli")
        pangenome = await client.pangenomes.get(pangenome_id=1)

asyncio.run(main())
```

Both clients can also be constructed without a context manager; call
`client.close()` (or `await client.close()`) when done.

## Client construction

`PanGBankClient` and `AsyncPanGBankClient` take the same arguments:

| Argument   | Type                                   | Description                                                                                                 |
| ---------- | --------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `base_url` | `str \| None`                          | Base URL of the PanGBank API. Required unless `client` is provided.                                          |
| `timeout`  | `float` (default `10.0`)               | Request timeout in seconds, used when a new `httpx.Client`/`AsyncClient` is created from `base_url`.          |
| `client`   | `httpx.Client \| httpx.AsyncClient \| None` | A pre-configured httpx client to reuse instead of creating one. Its lifecycle is not managed (`close` is a no-op). |

Raises `ValueError` if neither `base_url` nor `client` is given.

Each client exposes three resources, mirroring the API's routers:

- `client.collections`
- `client.genomes`
- `client.pangenomes`

Every method below exists on both the sync (`client.<resource>.<method>(...)`)
and async (`await client.<resource>.<method>(...)`) resource classes with an
identical signature.

## Errors

All SDK errors derive from `PanGBankAPIError` (itself an `Exception`), with
`status_code` and `response` (`httpx.Response | None`) attributes:

- **`PanGBankNotFoundError`** — the API returned `404` for a requested
  resource (e.g. `collections.get`, `genomes.get`, `pangenomes.get`,
  `pangenomes.get_genome` with an unknown id).
- **`PanGBankConnectionError`** — the underlying HTTP request could not be
  completed (DNS failure, timeout, connection refused, etc.). `status_code`
  and `response` are always `None`.
- **`PanGBankAPIError`** — any other non-2xx response.

```python
from pangbank_api.sdk import PanGBankNotFoundError

try:
    client.genomes.get(genome_id=999999)
except PanGBankNotFoundError as exc:
    print(exc.status_code, str(exc))
```

## Collections

`client.collections` / `client.collections` (async: `AsyncCollectionsResource`)

| Method | Description |
| --- | --- |
| `list(collection_name=None, collection_id=None, only_latest_release=None)` | List collections, optionally filtered by name or id. If `only_latest_release` is `True`, each collection's releases are restricted to the latest one. Returns `list[CollectionPublicWithReleases]`. |
| `get(collection_id, only_latest_release=None)` | Fetch a single collection by id. Returns `CollectionPublicWithReleases`. |
| `download_mash_sketch(collection_id, dest=None)` | Download the collection's Mash sketch file. |
| `download_index_info(collection_id, dest=None)` | Download the collection's index info file. |
| `download_index_pangenomes(collection_id, dest=None)` | Download the collection's pangenomes index file. |
| `download_index_genomes(collection_id, dest=None)` | Download the collection's genomes index file. |

All `download_*` methods accept an optional `dest: str | Path`. If given,
the response is streamed directly to that file and the `Path` is returned;
otherwise the full content is buffered in memory and returned as `bytes`.

```python
client.collections.download_mash_sketch(collection_id=1, dest="sketch.msh")
data = client.collections.download_index_info(collection_id=1)  # bytes
```

## Genomes

`client.genomes` (async: `AsyncGenomesResource`)

| Method | Description |
| --- | --- |
| `list(genome_name=None, taxon_name=None, substring_taxon_match=False, offset=0, limit=20)` | List genomes, optionally filtered by name or taxon. `substring_taxon_match=True` matches `taxon_name` as a substring instead of requiring an exact match. Returns `list[GenomePublic]`. |
| `get(genome_id)` | Fetch a single genome by id. Returns `GenomePublic`. |

## Pangenomes

`client.pangenomes` (async: `AsyncPangenomesResource`)

| Method | Description |
| --- | --- |
| `list(collection_name=None, collection_id=None, only_latest_release=None, taxon_name=None, substring_taxon_match=False, genome_name=None, pangenome_name=None, offset=0, limit=20)` | List pangenomes, filtered by any combination of owning collection, taxon, containing genome, or pangenome name. Returns `list[PangenomePublic]`. |
| `get(pangenome_id)` | Fetch a single pangenome by id. Returns `PangenomePublic`. |
| `count(collection_name=None, collection_id=None, only_latest_release=None, taxon_name=None, substring_taxon_match=False, genome_name=None, pangenome_name=None)` | Count pangenomes matching the same filters as `list` (no pagination). Returns `int`. |
| `list_genomes(pangenome_id, genome_name=None, offset=0, limit=20)` | List the genomes belonging to a pangenome. Returns `list[GenomePangenomeLinkPublic]`. |
| `get_genome(pangenome_id, genome_id)` | Fetch the link between a pangenome and one of its genomes. Returns `GenomePangenomeLinkPublic`. |
| `download_file(pangenome_id, dest=None)` | Download the pangenome file. |
| `download_cgview_map(pangenome_id, genome_id, dest=None)` | Download the CGView map for a genome within a pangenome. |
| `download_dbg_graph(pangenome_id, dest=None)` | Download the pangenome's De Bruijn graph file. |
| `download_graph_tool(pangenome_id, dest=None)` | Download the graph-tool representation of the pangenome. |
| `download_dbg_family_annotations(pangenome_id, dest=None)` | Download the De Bruijn graph's gene family annotations. |
| `download_dbg_genome_annotations(pangenome_id, dest=None)` | Download the De Bruijn graph's genome annotations. |

`download_*` methods follow the same `dest` convention as the collections
resource (write-to-file vs. return `bytes`).

```python
client.pangenomes.download_dbg_graph(pangenome_id=1, dest="graph.gfa")
count = client.pangenomes.count(taxon_name="Escherichia", substring_taxon_match=True)
```

## Module layout

| File | Contents |
| --- | --- |
| `client.py` | `PanGBankClient` (sync). |
| `async_client.py` | `AsyncPanGBankClient` (async). |
| `transport.py` | `SyncTransport` / `AsyncTransport` — httpx wrappers handling request dispatch, error translation, and streaming downloads. |
| `exceptions.py` | `PanGBankAPIError`, `PanGBankNotFoundError`, `PanGBankConnectionError`. |
| `resources/collections.py` | `CollectionsResource` / `AsyncCollectionsResource`. |
| `resources/genomes.py` | `GenomesResource` / `AsyncGenomesResource`. |
| `resources/pangenomes.py` | `PangenomesResource` / `AsyncPangenomesResource`. |
