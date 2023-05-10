import glob
import os
from multiprocessing import Process

import numpy as np
import pandas as pd
import torch as th
from transformers import AutoTokenizer, AutoModel


def extract(fns, world_size, rank, gpu, write_dir):
    os.makedirs(f"{write_dir}/partition-{rank:02d}", exist_ok=True)

    device = f"cuda:{gpu}"

    print(f"rank={rank}: started...")

    _df = []
    for i, fn in enumerate(fns):
        if (i % world_size) != rank:
            continue
        _df.append(pd.read_csv(fn[1], sep="\t", encoding="utf-8"))
    df = pd.concat(_df)
    df.fillna("", inplace=True)

    print(f"rank={rank}: {len(df)} records.")

    tokenizer = AutoTokenizer.from_pretrained("malteos/scincl")
    model = AutoModel.from_pretrained("malteos/scincl").to(device)
    model.eval()

    print(f"rank={rank}: model loaded.")

    batch_size = 256
    n = len(df) // batch_size
    work_id_batches = np.array_split(df["work_id"].values, n)  # type: ignore
    batches = np.array_split((df["title"] + tokenizer.sep_token + df["abstract"]).values, n)  # type: ignore

    print(f"rank={rank}: data is ready with {len(batches)} batches.")

    with open(f"{write_dir}/partition-{rank:02d}/index.tsv", "w") as fw:
        fw.write("\t".join(["work_id", "filename", "row"]) + "\n")
        with th.no_grad():
            for i, (work_id_batch, batch) in enumerate(zip(work_id_batches, batches)):
                inputs = tokenizer(batch.tolist(), padding=True, truncation=True, return_tensors="pt", max_length=512).to(device)
                result = model(**inputs)
                embeddings = result.last_hidden_state[:, 0, :]

                fn = f"partition-{rank:02d}/part-{i:05d}.npz"
                np.savez(f"{write_dir}/{fn}", embeddings.cpu().numpy())
                for j, work_id in enumerate(work_id_batch):
                    fw.write("\t".join([work_id, fn, str(j)]) + "\n")
                if (i + 1) % 10_000 == 0:
                    print(f"rank={rank}: {i + 1}/{n} done.")

    print(f"rank={rank}: done.")


if __name__ == "__main__":
    read_path = "openalex-snapshot-040923/extra/works/abstracts/*.tsv"
    write_dir = "openalex-snapshot-040923/processed/works/embeddings"

    fns = sorted([(os.path.getsize(fn), fn) for fn in glob.glob(read_path)], key=lambda x: -x[0])

    world_size = 12
    for i in range(3):
        ps = []
        for rank in range(i * 4, (i + 1) * 4):
            gpu = rank % 4
            p = Process(target=extract, args=(fns, world_size, rank, gpu, write_dir))
            ps.append(p)
            p.start()

        for p in ps:
            p.join()
