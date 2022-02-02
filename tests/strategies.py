from hypothesis import strategies
from yarl import URL

from consensual.core.raft.node import node_url_to_id
from .utils import MAX_RUNNING_NODES_COUNT

heartbeats = strategies.floats(2, 4)
delays = strategies.floats(0, 2)
hosts = strategies.just('localhost')
ports = strategies.integers(4000, 5000)
cluster_urls = strategies.builds(URL.build,
                                 scheme=strategies.just('http'),
                                 host=hosts,
                                 port=ports)
cluster_urls_lists = strategies.lists(cluster_urls,
                                      min_size=1,
                                      max_size=MAX_RUNNING_NODES_COUNT,
                                      unique_by=node_url_to_id)
