from . import cache, CustomLogger

from scipy.cluster.hierarchy import linkage
from scipy.cluster import hierarchy

logger = CustomLogger("HierarchyUtil")


def hierarchical_prediction(model, t):
    logger.debug("Evaluating model {}".format(t))
    link_matrix = get_link_matrix(model)
    clusters = hierarchy.fcluster(link_matrix, t, criterion="maxclust")
    return zip(range(len(clusters)), [int(el) for el in clusters])


@cache
def get_link_matrix(model):
    centroids = model.clusterCenters()
    return linkage(centroids, method="complete")
