# OGC Building Block — Portolan Record Extension

`portolan-record/` is an [OGC Building Block](https://ogcincubator.github.io/bblocks-docs/):
a JSON Schema (`schema.json`) + JSON-LD context (`context.jsonld`) + metadata (`bblock.json`)
that defines the namespaced `portolan:*` properties this catalog adds to an **OGC API - Records**
record / **STAC** item — cloud-native access (Apache Iceberg endpoint + table) and provenance.

It **depends on** `ogc.api.records.v1.0.record-core`: Portolan records are record-core records
*plus* this extension. `additionalProperties` is `true`, so the extension never breaks the host
record. The JSON-LD context gives semantic uplift (a path to RDF / GeoDCAT).

## How it's used here
- The catalog's [`records/`](../records/) documents carry these `portolan:*` properties.
- CI (`tools/validate.py` via `.github/workflows/validate.yml`) validates every record/item
  against this `schema.json` — so the building block is *enforced*, not just declared.

## Registering it with OGC
To publish in the OGC Building Blocks Register, the register's tooling auto-detects
`schema.json` and `context.jsonld` from a bblock directory (see
[bblock-template](https://github.com/opengeospatial/bblock-template)). For real registration this
block should live in a dedicated Portolan bblocks repository wired to the bblocks postprocessing
action; it lives here for now as the working, validated reference.

> Note: the vocabulary namespace in `context.jsonld`
> (`https://portolan-sdi.github.io/def/portolan#`) is a **placeholder** to be finalized when the
> Portolan definitions server / register home is decided.
