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
                publisher = json.loads(l)
                publisher_id = publisher["id"]

                if publisher.get("alternate_titles") is not None:
                    for alternate_title in publisher["alternate_titles"]:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, publisher_id, "P4970", alternate_title])

                if publisher.get("cited_by_count") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, publisher_id, "P_total_cited_by_count", publisher["cited_by_count"]])

                if publisher.get("country_codes") is not None:
                    for country_code in publisher["country_codes"]:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, publisher_id, "P297", country_code])

                if publisher.get("counts_by_year") is not None:
                    for count_by_year in publisher["counts_by_year"]:
                        if count_by_year.get("cited_by_count") is None:
                            continue

                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, publisher_id, "P_cited_by_count", count_by_year["cited_by_count"]])

                        if count_by_year.get("year") is None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P585", count_by_year["year"]])

                        if count_by_year.get("works_count") is None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P3740", count_by_year["works_count"]])

                if publisher.get("created_date") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, publisher_id, "P571", publisher["created_date"]])

                if publisher.get("display_name") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, publisher_id, "P2561", publisher["display_name"]])

                if publisher.get("hierarchy_level") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, publisher_id, "P1545", publisher["hierarchy_level"]])

                if publisher.get("ids") is not None:
                    ids = publisher["ids"]

                    if ids.get("ror") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, publisher_id, "P6782", ids["ror"]])

                    if ids.get("wikidata") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, publisher_id, "P_wikidata", ids["wikidata"]])

                if publisher.get("parent_publisher") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, publisher_id, "P749", publisher["parent_publisher"]])

                if publisher.get("summary_stats") is not None:
                    summary_stats = publisher["summary_stats"]

                    if summary_stats.get("2yr_mean_citedness") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, publisher_id, "P_impact_factor", summary_stats["2yr_mean_citedness"]])

                    if summary_stats.get("h_index") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, publisher_id, "P_h_index", summary_stats["h_index"]])

                    if summary_stats.get("i10_index") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, publisher_id, "P_i10_index", summary_stats["i10_index"]])

                if publisher.get("updated_date") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, publisher_id, "P5017", publisher["updated_date"]])

                if publisher.get("works_count") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, publisher_id, "P3740", publisher["works_count"]])

                triplet_id = str(uuid.uuid4())
                triples.append([triplet_id, publisher_id, "P31", "Q2085381"])

        print(f"{fn} done.")

    triples_df = pd.DataFrame(triples, columns=["id", "node1", "label", "node2"])
    triples_df.astype(str).to_parquet(f"{write_dir}/triples/partition-{rank}/part-0.parquet", index=False, compression=None)
    del triples_df

    quals_df = pd.DataFrame(quals, columns=["id", "node1", "label", "node2"])
    quals_df.astype(str).to_parquet(f"{write_dir}/quals/partition-{rank}/part-0.parquet", index=False, compression=None)
    del quals_df

if __name__ == "__main__":
    read_path = "openalex-snapshot-040923/data/publishers/*/*"
    write_dir = "openalex-snapshot-040923/processed/publishers"

    fns = sorted([(os.path.getsize(fn), fn) for fn in glob.glob(read_path)], key=lambda x: -x[0])

    world_size = 1
    ps = []
    for rank in range(world_size):
        p = Process(target=extract, args=(fns, world_size, rank, write_dir))
        ps.append(p)
        p.start()

    for p in ps:
        p.join()
