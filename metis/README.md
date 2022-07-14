# Metis: Better Atlas Vantage Point Selection for Everyone

Metis is a system that uses RIPE Atlas‘ built-in [traceroute topology
measurements](https://atlas.ripe.net/docs/built-in-measurements/#traceroute-5-000-6-999)
(measurement ids 5051, 5151 for IPv4 and 6052, 6152 for IPv6) to spatially
arrange autonomous systems (ASes) that contain Atlas probes (_probe ASes_).

Metis extracts features like AS-path length, RTT, and number of IP hops from the
traceroutes (`../traceroute_features.py`) and produces weekly distance matrices
containing all ASes that are visible in the traceroutes
(`../aggregate_traceroute_features.py`). Based on these weekly aggregates, Metis
computes two different outputs:

1. A ranking of probe ASes, where the rank indicates how “far away” an AS is
   from all other ASes.
2. A suggestion of ASes which are likely to increase Atlas‘ diversity if new
   probes are located there.

The weekly results are published at https://ihr.iijlab.net/ihr/en-us/metis.

## Probe Selection

The `compute_as_rank.py` script can take a varying number of aggregates (Metis
uses four weeks) to calculate a rank for each probe AS. The aggregates are
combined into a single matrix, which is further processed using the `Selector`
class of `../optimum_selector/selector.py`. The selector removes ASes stepwise,
starting with the AS that is “closest” to all other probe ASes according to some
distance metric. Metis currently produces results for three distance metrics:

1. AS-path length
2. Round-trip time (RTT)
3. Number of IP hops

## Probe Deployment

The `compute_candidates.py` script follows a similar process, only that the
distance matrix only includes results _coming_ from probe ASes and _going_ to
non-probe ASes. The matrix is processed with the
`../optimum_selector/candidate_selector.py` script, which again performs a
ranking and also excludes non-probe ASes with too few results. For this process
Metis uses 24 weeks of data and only includes non-probe ASes that were reached
from at least half of the probe ASes.

The remaining ASes are ranked by how “far away” they are from existing probe
ASes, indicating that placing a new probe in these ASes would increase Atlas‘
diversity.

> `extract_candidates.py` is a legacy script that is not actively used anymore.

## Publication

The process is further evaluated in our paper [“Metis: Better Atlas Vantage
Point Selection for
Everyone”](https://tma.ifip.org/2022/wp-content/uploads/sites/11/2022/06/tma2022-paper18.pdf),
which was presented at TMA 2022. If you use this work for your own measurements,
it would be cool if you cite the paper.

> Malte Appel, Emile Aben, and Romain Fontugne, “Metis: Better Atlas Vantage
Point Selection for Everyone”, Proceedings of the 6th Network Traffic
Measurement and Analysis Conference (TMA'22), June 2022.