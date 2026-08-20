from uxsim import *

# world
W = World(
    name="test_world",
    deltan=5,
    tmax=100
)

# nodes
W.addNode("A", 0, 0)
W.addNode("B", 1, 0)

# road/link
W.addLink(
    "A-B",
    "A",
    "B",
    length=1000,
    free_flow_speed=20,
    jam_density=0.2
)

# traffic demand
W.adddemand(
    "A",
    "B",
    0,
    100,
    0.5
)

W.exec_simulation()

# results to pandas
df = W.analyzer.vehicles_to_pandas()

print(df.head())
print(df.columns)
