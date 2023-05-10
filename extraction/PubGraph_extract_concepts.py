import glob
import json
import os
import uuid
from multiprocessing import Process

import pandas as pd


def extract(fns, world_size, rank, write_dir):
    os.makedirs(f"{write_dir}/triples/partition-{rank}", exist_ok=True)
    os.makedirs(f"{write_dir}/quals/partition-{rank}", exist_ok=True)

    triples = []
    quals = []
    for i, fn in enumerate(fns):
        if (i % world_size) != rank:
            continue
        with open(fn[1], "r") as fr:
            for l in fr:
                concept = json.loads(l)
                concept_id = concept["id"]

                if concept.get("ancestors") is not None:
                    for ancestor in concept["ancestors"]:
                        if ancestor.get("id") is None:
                            continue

                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, concept_id, "P4900", ancestor["id"]])

                if concept.get("cited_by_count") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, concept_id, "P_total_cited_by_count", concept["cited_by_count"]])

                if concept.get("counts_by_year") is not None:
                    for count_by_year in concept["counts_by_year"]:
                        if count_by_year.get("cited_by_count") is None:
                            continue

                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, concept_id, "P_cited_by_count", count_by_year["cited_by_count"]])

                        if count_by_year.get("year") is None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P585", count_by_year["year"]])

                        if count_by_year.get("works_count") is None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P3740", count_by_year["works_count"]])

                if concept.get("created_date") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, concept_id, "P571", concept["created_date"]])

                if concept.get("description") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, concept_id, "P7535", concept["description"]])

                if concept.get("display_name") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, concept_id, "P2561", concept["display_name"]])

                if concept.get("ids") is not None:
                    ids = concept["ids"]

                    if ids.get("wikipedia") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, concept_id, "P4656", ids["wikipedia"]])

                    if ids.get("mag") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, concept_id, "P6366", ids["mag"]])

                    if ids.get("umls_cui") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, concept_id, "P2892", ids["umls_cui"]])

                    if ids.get("umls_aui") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, concept_id, "P_umls_aui", ids["umls_aui"]])

                if concept.get("international") is not None:
                    for wikidata_language_code, display_name in concept["international"].items():
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, concept_id, "P4970", display_name])

                        qual_id = str(uuid.uuid4())
                        quals.append([qual_id, triplet_id, "P9753", wikidata_language_code])

                if concept.get("level") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, concept_id, "P1545", concept["level"]])

                if concept.get("related_concepts") is not None:
                    for related_concept in concept["related_concepts"]:
                        if related_concept.get("id") is None:
                            continue

                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, concept_id, "P921", related_concept["id"]])

                        if related_concept.get("score") is None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P4271", related_concept["score"]])

                if concept.get("summary_stats") is not None:
                    summary_stats = concept["summary_stats"]

                    if summary_stats.get("2yr_mean_citedness") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, concept_id, "P_impact_factor", summary_stats["2yr_mean_citedness"]])

                    if summary_stats.get("h_index") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, concept_id, "P_h_index", summary_stats["h_index"]])

                    if summary_stats.get("i10_index") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, concept_id, "P_i10_index", summary_stats["i10_index"]])

                if concept.get("updated_date") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, concept_id, "P5017", concept["updated_date"]])

                if concept.get("wikidata") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, concept_id, "P_wikidata", concept["wikidata"]])

                if concept.get("works_count") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, concept_id, "P3740", concept["works_count"]])

                triplet_id = str(uuid.uuid4())
                triples.append([triplet_id, concept_id, "P31", "Q115949945"])

        print(f"{fn} done.")

    triples_df = pd.DataFrame(triples, columns=["id", "node1", "label", "node2"])
    triples_df.astype(str).to_parquet(f"{write_dir}/triples/partition-{rank}/part-0.parquet", index=False, compression=None)
    del triples_df

    quals_df = pd.DataFrame(quals, columns=["id", "node1", "label", "node2"])
    quals_df.astype(str).to_parquet(f"{write_dir}/quals/partition-{rank}/part-0.parquet", index=False, compression=None)
    del quals_df

if __name__ == "__main__":
    read_path = "openalex-snapshot-040923/data/concepts/*/*"
    write_dir = "openalex-snapshot-040923/processed/concepts"

    fns = sorted([(os.path.getsize(fn), fn) for fn in glob.glob(read_path)], key=lambda x: -x[0])

    world_size = 1
    ps = []
    for rank in range(world_size):
        p = Process(target=extract, args=(fns, world_size, rank, write_dir))
        ps.append(p)
        p.start()

    for p in ps:
        p.join()
