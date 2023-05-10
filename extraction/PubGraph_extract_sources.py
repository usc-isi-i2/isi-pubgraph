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
                source = json.loads(l)
                source_id = source["id"]

                if source.get("abbreviated_title") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, source_id, "P1813", source["abbreviated_title"]])

                if source.get("alternate_titles") is not None:
                    for alternate_title in source["alternate_titles"]:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, source_id, "P1476", alternate_title])

                if source.get("apc_usd") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, source_id, "P2555", source["apc_usd"]])

                if source.get("cited_by_count") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, source_id, "P_total_cited_by_count", source["cited_by_count"]])

                if source.get("country_code") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, source_id, "P297", source["country_code"]])

                if source.get("counts_by_year") is not None:
                    for count_by_year in source["counts_by_year"]:
                        if count_by_year.get("cited_by_count") is None:
                            continue

                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, source_id, "P_cited_by_count", count_by_year["cited_by_count"]])

                        if count_by_year.get("year") is None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P585", count_by_year["year"]])

                        if count_by_year.get("works_count") is None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P3740", count_by_year["works_count"]])

                if source.get("created_date") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, source_id, "P571", source["created_date"]])

                if source.get("display_name") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, source_id, "P2561", source["display_name"]])

                if source.get("homepage_url") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, source_id, "P856", source["homepage_url"]])

                if source.get("host_organization") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, source_id, "P749", source["host_organization"]])

                if source.get("ids") is not None:
                    ids = source["ids"]

                    if ids.get("mag") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, source_id, "P6366", ids["mag"]])

                    if ids.get("fatcat") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, source_id, "P8608", ids["fatcat"]])

                    if ids.get("wikidata") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, source_id, "P_wikidata", ids["wikidata"]])

                if source.get("is_in_doaj") is not None and source["is_in_doaj"]:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, source_id, "P31", "Q1227538"])

                if source.get("is_oa") is not None and source["is_oa"]:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, source_id, "P31", "Q232932"])

                if source.get("issn_l") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, source_id, "P7363", source["issn_l"]])

                if source.get("issn") is not None:
                    for _issn in source["issn"]:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, source_id, "P236", _issn])

                if source.get("summary_stats") is not None:
                    summary_stats = source["summary_stats"]

                    if summary_stats.get("2yr_mean_citedness") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, source_id, "P_impact_factor", summary_stats["2yr_mean_citedness"]])

                    if summary_stats.get("h_index") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, source_id, "P_h_index", summary_stats["h_index"]])

                    if summary_stats.get("i10_index") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, source_id, "P_i10_index", summary_stats["i10_index"]])

                if source.get("type") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, source_id, "P31", source["type"]])

                if source.get("updated_date") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, source_id, "P5017", source["updated_date"]])

                if source.get("works_count") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, source_id, "P3740", source["works_count"]])

                if source.get("x_concepts") is not None:
                    for x_concept in source["x_concepts"]:
                        if x_concept.get("id") is None:
                            continue

                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, source_id, "P921", x_concept["id"]])

                        if x_concept.get("score") is None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P4271", x_concept["score"]])

                triplet_id = str(uuid.uuid4())
                triples.append([triplet_id, source_id, "P31", "Q1711593"])

        print(f"{fn} done.")

    triples_df = pd.DataFrame(triples, columns=["id", "node1", "label", "node2"])
    triples_df.astype(str).to_parquet(f"{write_dir}/triples/partition-{rank}/part-0.parquet", index=False, compression=None)
    del triples_df

    quals_df = pd.DataFrame(quals, columns=["id", "node1", "label", "node2"])
    quals_df.astype(str).to_parquet(f"{write_dir}/quals/partition-{rank}/part-0.parquet", index=False, compression=None)
    del quals_df

if __name__ == "__main__":
    read_path = "openalex-snapshot-040923/data/sources/*/*"
    write_dir = "openalex-snapshot-040923/processed/sources"

    fns = sorted([(os.path.getsize(fn), fn) for fn in glob.glob(read_path)], key=lambda x: -x[0])

    world_size = 1
    ps = []
    for rank in range(world_size):
        p = Process(target=extract, args=(fns, world_size, rank, write_dir))
        ps.append(p)
        p.start()

    for p in ps:
        p.join()
