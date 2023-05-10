import glob
import json
import os
import uuid
from multiprocessing import Process

import pandas as pd
# from latlon import LatLon


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
                institution = json.loads(l)
                institution_id = institution["id"]

                if institution.get("associated_institutions") is not None:
                    for associated_institution in institution["associated_institutions"]:
                        if associated_institution.get("id") is None:
                            continue

                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, institution_id, "P1416", associated_institution["id"]])

                        if associated_institution.get("relationship") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P1039", associated_institution["relationship"]])

                if institution.get("cited_by_count") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, institution_id, "P_total_cited_by_count", institution["cited_by_count"]])

                if institution.get("country_code") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, institution_id, "P297", institution["country_code"]])

                if institution.get("counts_by_year") is not None:
                    for count_by_year in institution["counts_by_year"]:
                        if count_by_year.get("cited_by_count") is None:
                            continue

                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, institution_id, "P_cited_by_count", count_by_year["cited_by_count"]])

                        if count_by_year.get("year") is None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P585", count_by_year["year"]])

                        if count_by_year.get("works_count") is None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P3740", count_by_year["works_count"]])

                if institution.get("created_date") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, institution_id, "P571", institution["created_date"]])

                if institution.get("display_name") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, institution_id, "P2561", institution["display_name"]])

                if institution.get("display_name_acronyms") is not None:
                    for display_name_acronym in institution["display_name_acronyms"]:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, institution_id, "P1813", display_name_acronym])

                if institution.get("display_name_alternatives") is not None:
                    for display_name_alternative in institution["display_name_alternatives"]:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, institution_id, "P4970", display_name_alternative])

                if institution.get("geo") is not None:
                    geo = institution["geo"]

                    # if geo.get("city") is not None:
                    #     triplet_id = str(uuid.uuid4())
                    #     triples.append([triplet_id, institution_id, "P131", geo["city"]])

                    if geo.get("geonames_city_id") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, institution_id, "P1566", geo["geonames_city_id"]])

                    # if geo.get("region") is not None:
                    #     triplet_id = str(uuid.uuid4())
                    #     triples.append([triplet_id, institution_id, "P131", geo["region"]])

                    # if geo.get("country_code") is not None:
                    #     triplet_id = str(uuid.uuid4())
                    #     triples.append([triplet_id, institution_id, "P297", geo["country_code"]])

                    # if geo.get("country") is not None:
                    #     triplet_id = str(uuid.uuid4())
                    #     triples.append([triplet_id, institution_id, "P17", geo["country"]])

                    # if geo.get("latitude") is not None and geo.get("longitude") is not None:
                    #     coordinate = " ".join(LatLon(geo["latitude"], geo["longitude"]).to_string("d%°%m%'%S%\"%H"))

                    #     triplet_id = str(uuid.uuid4())
                    #     triples.append([triplet_id, institution_id, "P625", coordinate])

                if institution.get("homepage_url") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, institution_id, "P856", institution["homepage_url"]])

                if institution.get("ids") is not None:
                    ids = institution["ids"]

                    if ids.get("grid") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, institution_id, "P2427", ids["grid"]])

                    if ids.get("wikipedia") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, institution_id, "P4656", ids["wikipedia"]])

                    if ids.get("wikidata") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, institution_id, "P_wikidata", ids["wikidata"]])

                    if ids.get("mag") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, institution_id, "P6366", ids["mag"]])

                if institution.get("international") is not None:
                    for wikidata_language_code, display_name in institution["international"].items():
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, institution_id, "P4970", display_name])

                        qual_id = str(uuid.uuid4())
                        quals.append([qual_id, triplet_id, "P9753", wikidata_language_code])

                if institution.get("ror") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, institution_id, "P6782", institution["ror"]])

                if institution.get("summary_stats") is not None:
                    summary_stats = institution["summary_stats"]

                    if summary_stats.get("2yr_mean_citedness") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, institution_id, "P_impact_factor", summary_stats["2yr_mean_citedness"]])

                    if summary_stats.get("h_index") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, institution_id, "P_h_index", summary_stats["h_index"]])

                    if summary_stats.get("i10_index") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, institution_id, "P_i10_index", summary_stats["i10_index"]])

                if institution.get("type") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, institution_id, "P31", institution["type"]])

                if institution.get("updated_date") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, institution_id, "P5017", institution["updated_date"]])

                if institution.get("works_count") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, institution_id, "P3740", institution["works_count"]])

                if institution.get("x_concepts") is not None:
                    for x_concept in institution["x_concepts"]:
                        if x_concept.get("id") is None:
                            continue

                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, institution_id, "P921", x_concept["id"]])

                        if x_concept.get("score") is None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P4271", x_concept["score"]])

                triplet_id = str(uuid.uuid4())
                triples.append([triplet_id, institution_id, "P31", "Q178706"])

        print(f"{fn} done.")

    triples_df = pd.DataFrame(triples, columns=["id", "node1", "label", "node2"])
    triples_df.astype(str).to_parquet(f"{write_dir}/triples/partition-{rank}/part-0.parquet", index=False, compression=None)
    del triples_df

    quals_df = pd.DataFrame(quals, columns=["id", "node1", "label", "node2"])
    quals_df.astype(str).to_parquet(f"{write_dir}/quals/partition-{rank}/part-0.parquet", index=False, compression=None)
    del quals_df

if __name__ == "__main__":
    read_path = "openalex-snapshot-040923/data/institutions/*/*"
    write_dir = "openalex-snapshot-040923/processed/institutions"

    fns = sorted([(os.path.getsize(fn), fn) for fn in glob.glob(read_path)], key=lambda x: -x[0])

    world_size = 1
    ps = []
    for rank in range(world_size):
        p = Process(target=extract, args=(fns, world_size, rank, write_dir))
        ps.append(p)
        p.start()

    for p in ps:
        p.join()
