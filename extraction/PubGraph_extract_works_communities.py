from igraph import *
import leidenalg
import datetime
import pickle
import logging
import numpy as np

optimiser_dict = {
    1: {
        "name":"SignificanceVertexPartition",
        "obj":leidenalg.SignificanceVertexPartition
        }
}

max_comm_size = 300000
n_comms = 3000
n_iterations = 10

edges_file = "openalex-snapshot-040923/extra/works/citations/indexed_citations.npy"
vertices_count = 100847204
optimiser_option = 1

optimiser_d = optimiser_dict[optimiser_option]

def logger_setup(path_to_log):
    time =  datetime.datetime.now()
    st_time =  "{}_{}_{}-{}_{}_{}".format(time.year, time.month, time.day, time.hour, time.minute, time.second)
    LOG_PATH = "{}{}-{}-{}-{}-{}.log".format(path_to_log, st_time, optimiser_d["name"], n_comms, max_comm_size, n_iterations)
    print(LOG_PATH)
    logging.basicConfig(filename=LOG_PATH, filemode='w', level="INFO", format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger()
    return logger


logger = logger_setup("openalex-snapshot-040923/extra/works/citations/igraph/logs/")
logger.info("Execution Begins\n\n")

logger.info("Max Comm size: {}".format(max_comm_size))
logger.info("Number of Comms: {}".format(n_comms))
logger.info("Number of iter: {}".format(n_iterations))

logger.info("Edges File Loc: {}".format(edges_file))
logger.info("Number of Vertices: {}".format(vertices_count))


g = Graph()

g.add_vertices(100847204)
logger.info("Added Nodes to Graph")

with open(edges_file, "rb") as ef:
    edges_arr = np.load(ef)

g.add_edges(edges_arr)
logger.info("Added Edges to Graph")

logger.info("Finding Communities")

#-------------------------------------------------------------------------------------------------------#

ini_mem = np.random.choice(n_comms, int(vertices_count))

partition = optimiser_d["obj"](g, initial_membership=ini_mem)


logger.info("Set initial partition")


optimiser = leidenalg.Optimiser()


logger.info("Set Max Size")

optimiser.max_comm_size = max_comm_size

logger.info("Set random Seed: 70")
optimiser.set_rng_seed(70)

optimiser.consider_empty_community = False


logger.info("Optimising")
optimiser.optimise_partition(partition, n_iterations)

logger.info("Complete")

#-------------------------------------------------------------------------------------------------------#

partitions = list(partition)
logger.info("Completed Finding Communities")

result_folder = "openalex-snapshot-040923/extra/works/citations/igraph/res/"
pickle_path = "{}Leiden-{}-{}-{}-{}.pickle".format(result_folder, optimiser_d["name"], n_comms, max_comm_size, n_iterations)

logger.info("Serialising partitions")
with open(pickle_path, 'wb') as pf:
    pickle.dump(partitions, pf)

logger.info("Serialising Complete")
logger.info("Output Pickle File Path: {}".format(pickle_path))
