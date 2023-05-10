[![CC BY-SA 4.0][cc-by-sa-shield]][cc-by-sa]

# Schema

## PubGraph

After extraction, the PubGraph data is formatted as follows:

```bash
PubGraph
├── authors
│   ├── quals
│   │   ├── partition-00
│   │   │   ├── part-0000.parquet
│   │   │   ├── ...
│   │   │   └── part-YYYY.parquet
│   │   ├── ...
│   │   └── partition-XX
│   │       ├── part-0000.parquet
│   │       ├── ...
│   │       └── part-YYYY.parquet
│   └── triples
│       ├── partition-00
│       │   ├── part-0000.parquet
│       │   ├── ...
│       │   └── part-YYYY.parquet
│       ├── ...
│       └── partition-XX
│           ├── part-0000.parquet
│           ├── ...
│           └── part-YYYY.parquet
├── concepts
├── institutions
├── publishers
├── sources
└── works
    ├── quals
    ├── triples
    └── extra
        ├── corpus_ids.parquet
        ├── wikidata_qnodes.parquet
        ├── community_ids.parquet
        └── embeddings
           ├── partition-00
           │   ├── index.tsv
           │   ├── part-0000.npz
           │   ├── ...
           │   └── part-YYYY.npz
           ├── ...
           └── partition-XX
               ├── index.tsv
               ├── part-0000.npz
               ├── ...
               └── part-YYYY.npz
```

Each top-level directory contains the data extracted from the respective entity types in OpenAlex, e.g., [authors](https://docs.openalex.org/api-entities/authors/author-object).
Inside each top-level directory there are two directories containing the triples and qualifiers as described in [KGTK data model](https://kgtk.readthedocs.io/en/latest/data_model/).
Each partition is further split into smaller compressed parts for easier data handling.
Additionally, the `works` directory includes an `extra` subdirectory that holds the additional information, i.e., Corpus IDs, Wikidata IDs, Wikidata Qnodes, Community IDs, and Embeddings, included as part of the PubGraph.

## PG-X

After extraction, the PG-X data is formatted as follows:

```bash
PG-X
├── PG-1M
│   ├── ent2id.tsv
│   ├── rel2id.tsv
│   ├── test_data_test_setting.tsv
│   ├── test_data_valid_setting.tsv
│   ├── train_data_test_setting.tsv
│   └── train_data_valid_setting.tsv
├── PG-10M
│   ├── ent2id.tsv
│   ├── rel2id.tsv
│   ├── test_data_test_setting.tsv
│   ├── test_data_valid_setting.tsv
│   ├── train_data_test_setting.tsv
│   └── train_data_valid_setting.tsv
└── PG-Full
    ├── ent2id.tsv
    ├── rel2id.tsv
    ├── test_data_test_setting.tsv
    ├── test_data_valid_setting.tsv
    ├── train_data_test_setting.tsv
    └── train_data_valid_setting.tsv
```

# License
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License][cc-by-sa].

[![CC BY-SA 4.0][cc-by-sa-image]][cc-by-sa]

[cc-by-sa]: http://creativecommons.org/licenses/by-sa/4.0/
[cc-by-sa-image]: https://licensebuttons.net/l/by-sa/4.0/88x31.png
[cc-by-sa-shield]: https://img.shields.io/badge/License-CC%20BY--SA%204.0-lightgrey.svg
