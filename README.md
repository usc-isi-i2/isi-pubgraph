# Schema

The dataset is formatted as follows:

```bash
.
├── authors
│   ├── quals
│   │   ├── partition-00
│   │   │   ├── part-0.tsv.gz
│   │   │   ├── ...
│   │   │   └── part-0.tsv.gz
│   │   ├── ...
│   │   └── partition-XX
│   │       ├── part-0.tsv.gz
│   │       ├── ...
│   │       └── part-Y.tsv.gz
│   └── triples
│       ├── partition-00
│       │   ├── part-0.tsv.gz
│       │   ├── ...
│       │   └── part-Y.tsv.gz
│       ├── ...
│       └── partition-XX
│           ├── part-0.tsv.gz
│           ├── ...
│           └── part-Y.tsv.gz
├── concepts
├── institutions
├── publishers
├── sources
└── works
    ├── quals
    ├── triples
    └── embeddings
```

Each top-level directory contains the data extracted from the respective entity types in OpenAlex, e.g., [authors](https://docs.openalex.org/api-entities/authors/author-object).
Inside each top-level directory there are two directories containing the triples and qualifiers as described in [KGTK data model](https://kgtk.readthedocs.io/en/latest/data_model/).
Each partition is further split into smaller compressed parts for easier data handling.
Additionally, the `works` directory includes an `embeddings` subdirectory that holds the indices and embeddings extracted from the articles' `title + abstract` using [SciNCL](https://huggingface.co/malteos/scincl).
