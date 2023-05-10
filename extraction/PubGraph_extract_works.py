import glob
import json
import os
import re
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
                work = json.loads(l)
                work_id = work["id"]

                if work.get("abstract_inverted_index") is not None:
                    abstract_inverted_index = work["abstract_inverted_index"]
                    abstract_index = []
                    for k, v in abstract_inverted_index.items():
                        for p in v:
                            abstract_index.append((k, p))
                    abstract_index = sorted(abstract_index, key=lambda x: x[1])

                    abstract = " ".join(map(lambda x: x[0], abstract_index))
                    abstract = abstract.lower()
                    abstract = re.sub('\s+', ' ', abstract)  # type: ignore

                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, work_id, "P7535", abstract])

                if work.get("authorships") is not None:
                    for authorship in work["authorships"]:
                        if authorship.get("author") is None or authorship["author"].get("id") is None:
                            continue
                        author = authorship["author"]

                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, work_id, "P50", author["id"]])

                        if authorship.get("author_position") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P1545", authorship["author_position"]])

                        if authorship.get("institution") is not None:
                            for institution in authorship["institution"]:
                                if institution.get("id") is None:
                                    continue

                                qual_id = str(uuid.uuid4())
                                quals.append([qual_id, triplet_id, "P1416", institution["id"]])

                        if author.get("is_corresponding") is not None and author["is_corresponding"]:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P31", "Q36988860"])

                if work.get("best_oa_location") is not None:
                    best_oa_location = work["best_oa_location"]
                    if best_oa_location.get("source") is not None and best_oa_location["source"].get("id") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, work_id, "P_best_oa_location", best_oa_location["source"]["id"]])

                        if best_oa_location.get("is_oa") is not None and best_oa_location["is_oa"]:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P31", "Q232932"])

                        if best_oa_location.get("landing_page_url") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P973", best_oa_location["landing_page_url"]])

                        if best_oa_location.get("pdf_url") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P953", best_oa_location["pdf_url"]])

                        if best_oa_location.get("license") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P275", best_oa_location["license"]])

                        if best_oa_location.get("version") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P9767", best_oa_location["version"]])

                if work.get("biblio") is not None:
                    biblio = work["biblio"]

                    if biblio.get("volume") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, work_id, "P478", biblio["volume"]])

                    if biblio.get("issue") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, work_id, "P433", biblio["issue"]])

                    if biblio.get("first_page") is not None or biblio.get("last_page") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, work_id, "P304", f"{biblio['first_page'] or ''}-{biblio['last_page'] or ''}"])

                if work.get("cited_by_count") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, work_id, "P_total_cited_by_count", work["cited_by_count"]])

                if work.get("concepts") is not None:
                    for concept in work["concepts"]:
                        if concept.get("id") is None:
                            continue

                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, work_id, "P921", concept["id"]])

                        if concept.get("score") is None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P4271", concept["score"]])

                if work.get("counts_by_year") is not None:
                    for count_by_year in work["counts_by_year"]:
                        if count_by_year.get("cited_by_count") is None or count_by_year.get("year") is None:
                            continue
                        count_by_year_cited_by_count = count_by_year["cited_by_count"]
                        count_by_year_year = count_by_year["year"]

                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, work_id, "P_cited_by_count", count_by_year_cited_by_count])

                        qual_id = str(uuid.uuid4())
                        quals.append([qual_id, triplet_id, "P585", count_by_year_year])

                if work.get("created_date") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, work_id, "P571", work["created_date"]])

                if work.get("doi") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, work_id, "P356", work["doi"]])

                if work.get("ids") is not None:
                    ids = work["ids"]

                    if ids.get("mag") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, work_id, "P6366", ids["mag"]])

                    if ids.get("pmid") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, work_id, "P698", ids["pmid"]])

                    if ids.get("pmcid") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, work_id, "P932", ids["pmcid"]])

                if work.get("is_paratext") is not None and work["is_paratext"]:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, work_id, "P31", "Q853520"])

                if work.get("is_retracted") is not None and work["is_retracted"]:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, work_id, "P31", "Q45182324"])

                if work.get("locations") is not None:
                    for location in work["locations"]:
                        if location.get("source") is None:
                            continue
                        source = location["source"]

                        if source.get("id") is None:
                            continue
                        source_id = source["id"]

                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, work_id, "P1433", source_id])

                        if location.get("is_oa") is not None and location["is_oa"]:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P31", "Q232932"])

                        if location.get("landing_page_url") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P973", location["landing_page_url"]])

                        if location.get("pdf_url") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P953", location["pdf_url"]])

                        if location.get("license") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P275", location["license"]])

                        if location.get("version") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P9767", location["version"]])

                if work.get("mesh") is not None:
                    for _mesh in work["mesh"]:
                        if _mesh.get("descriptor_ui") is not None or _mesh.get("qualifier_ui") is not None:
                            triplet_id = str(uuid.uuid4())
                            triples.append([triplet_id, work_id, "P9340", f"{_mesh['descriptor_ui'] or ''}{_mesh['qualifier_ui'] or ''}"])

                if work.get("open_access") is not None:
                    open_access = work["open_access"]

                    if open_access.get("is_oa") is not None and open_access["is_oa"]:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, work_id, "P31", "Q232932"])

                        if open_access.get("oa_status") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P6954", open_access["oa_status"]])

                        if open_access.get("oa_url") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P2699", open_access["oa_url"]])

                if work.get("primary_location") is not None:
                    primary_location = work["primary_location"]
                    if primary_location.get("source") is not None and primary_location["source"].get("id") is not None:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, work_id, "P_primary_location", primary_location["source"]["id"]])

                        if primary_location.get("is_oa") is not None and primary_location["is_oa"]:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P31", "Q232932"])

                        if primary_location.get("landing_page_url") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P973", primary_location["landing_page_url"]])

                        if primary_location.get("pdf_url") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P953", primary_location["pdf_url"]])

                        if primary_location.get("license") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P275", primary_location["license"]])

                        if primary_location.get("version") is not None:
                            qual_id = str(uuid.uuid4())
                            quals.append([qual_id, triplet_id, "P9767", primary_location["version"]])

                if work.get("publication_date") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, work_id, "P577", work["publication_date"]])

                if work.get("referenced_works") is not None:
                    for referenced_work in work["referenced_works"]:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, work_id, "P2860", referenced_work])

                if work.get("related_works") is not None:
                    for related_work in work["related_works"]:
                        triplet_id = str(uuid.uuid4())
                        triples.append([triplet_id, work_id, "P_related_work", related_work])

                if work.get("title") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, work_id, "P1476", work["title"]])

                if work.get("type") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, work_id, "P31", work["type"]])

                if work.get("updated_date") is not None:
                    triplet_id = str(uuid.uuid4())
                    triples.append([triplet_id, work_id, "P5017", work["updated_date"]])

                triplet_id = str(uuid.uuid4())
                triples.append([triplet_id, work_id, "P31", "Q13442814"])

        triples_df = pd.DataFrame(triples, columns=["id", "node1", "label", "node2"])
        triples_df.astype(str).to_parquet(f"{write_dir}/triples/partition-{rank:02d}/part-{part}.parquet", index=False, compression=None)
        del triples_df

        quals_df = pd.DataFrame(quals, columns=["id", "node1", "label", "node2"])
        quals_df.astype(str).to_parquet(f"{write_dir}/quals/partition-{rank:02d}/part-{part}.parquet", index=False, compression=None)
        del quals_df

        part += 1

        print(f"{fn} done.")


if __name__ == "__main__":
    read_path = "openalex-snapshot-040923/data/works/*/*"
    write_dir = "openalex-snapshot-040923/processed/works"

    fns = sorted([(os.path.getsize(fn), fn) for fn in glob.glob(read_path)], key=lambda x: -x[0])

    world_size = 48
    ps = []
    for rank in range(world_size):
        p = Process(target=extract, args=(fns, world_size, rank, write_dir))
        ps.append(p)
        p.start()

    for p in ps:
        p.join()
