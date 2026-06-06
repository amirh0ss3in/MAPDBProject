from dask.distributed import Client, SSHCluster

print("Starting cluster... this takes a few seconds...")
cluster = SSHCluster(
    ["master", "worker1", "worker2"],
    connect_options={"known_hosts": None},
    scheduler_options={"port": 8786, "dashboard_address": ":8797"}
)

client = Client(cluster)
print("CLUSTER IS ALIVE!")
print(client)
