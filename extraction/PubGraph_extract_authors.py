import glob
import json
import os
import uuid
from multiprocessing import Process

import pandas as pd


def extract(fns, world_size, rank, write_dir):
    os.makedirs(f"{write_dir}/triples/partition-{rank:02d}", exist_ok=True)
    os.makedirs(f"{write_dir}/quals/partition-{rank:02d}", exist_ok=True)

    part = 0
    for i, fn in enumerate(fns):
        if (i % world_size) != rank:
            continue
        triples = []
        quals = []
        with open(fn[1], "r") as fr:
            for l in fr:
                author = json.loads(l)
                author_id = author["id"]

                if author.get("cited_by_count") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, author_id, "P_total_cited_by_count", author["cited_by_count"]])

                if author.get("counts_by_year") is not None:
                    for count_by_year in author["counts_by_year"]:
                        if count_by_year.get("cited_by_count") is None:
                            continue

                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, author_id, "P_cited_by_count", count_by_year["cited_by_count"]])

                        if count_by_year.get("year") is None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P585", count_by_year["year"]])

                        if count_by_year.get("works_count") is None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P3740", count_by_year["works_count"]])

                if author.get("created_date") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, author_id, "P571", author["created_date"]])

                if author.get("display_name") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, author_id, "P2561", author["display_name"]])

                if author.get("display_name_alternatives") is not None:
                    for display_name_alternative in author["display_name_alternatives"]:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, author_id, "P4970", display_name_alternative])

                if author.get("ids") is not None:
                    ids = author["ids"]

                    if ids.get("scopus") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, author_id, "P1153", ids["scopus"]])

                    if ids.get("mag") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, author_id, "P6366", ids["mag"]])

                    if ids.get("twitter") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, author_id, "P2002", ids["twitter"]])

                    if ids.get("wikipedia") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, author_id, "P4656", ids["wikipedia"]])

                if author.get("last_known_institution") is not None and author["last_known_institution"].get("id") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, author_id, "P1416", author["last_known_institution"]["id"]])

                if author.get("orcid") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, author_id, "P496", author["orcid"]])

                if author.get("summary_stats") is not None:
                    summary_stats = author["summary_stats"]

                    if summary_stats.get("2yr_mean_citedness") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, author_id, "P_impact_factor", summary_stats["2yr_mean_citedness"]])

                    if summary_stats.get("h_index") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, author_id, "P_h_index", summary_stats["h_index"]])

                    if summary_stats.get("i10_index") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, author_id, "P_i10_index", summary_stats["i10_index"]])

                if author.get("updated_date") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, author_id, "P5017", author["updated_date"]])

                if author.get("works_count") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, author_id, "P3740", author["works_count"]])

                if author.get("x_concepts") is not None:
                    for x_concept in author["x_concepts"]:
                        if x_concept.get("id") is None:
                            continue

                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, author_id, "P921", x_concept["id"]])

                        if x_concept.get("score") is None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P4271", x_concept["score"]])

                triplet_id = str(uuid.uuid4())
                triples.append([triplet_id, author_id, "P31", "Q482980"])

        triples_df = pd.DataFrame(triples, columns=["id", "node1", "label", "node2"])
        triples_df.astype(str).to_parquet(f"{write_dir}/triples/partition-{rank:02d}/part-{part}.parquet", index=False, compression=None)
        del triples_df

        quals_df = pd.DataFrame(quals, columns=["id", "node1", "label", "node2"])
        quals_df.astype(str).to_parquet(f"{write_dir}/quals/partition-{rank:02d}/part-{part}.parquet", index=False, compression=None)
        del quals_df

        part += 1

        print(f"{fn} done.")

if __name__ == "__main__":
    read_path = "openalex-snapshot-040923/data/authors/*/*"
    write_dir = "openalex-snapshot-040923/processed/authors"

    fns = sorted([(os.path.getsize(fn), fn) for fn in glob.glob(read_path)], key=lambda x: -x[0])

    world_size = 36
    ps = []
    for rank in range(world_size):
        p = Process(target=extract, args=(fns, world_size, rank, write_dir))
        ps.append(p)
        p.start()

    for p in ps:
        p.join()
