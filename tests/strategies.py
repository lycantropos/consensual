import multiprocessing

from hypothesis import strategies
from yarl import URL

from consensual.core.raft.node import node_url_to_id

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
                                      max_size=(multiprocessing.cpu_count()
                                                // 2) - 1,
                                      unique_by=node_url_to_id)
