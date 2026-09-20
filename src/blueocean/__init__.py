"""Blue Ocean ATS overnight market-data pipeline.

Submits Databento batch jobs for the Blue Ocean (OCEA.MEMOIR) overnight session,
downloads the resulting DBN shards, converts them to Parquet, and lands every
row in a Hive-partitioned local data lake, sorted for downstream reads.

Nothing is filtered on the way in. Restricting the lake to symbols of interest
is a one-semi-join change documented on :func:`blueocean.updater.merge_and_sort`.
"""

__version__ = "2.0.0"
__all__ = ["config", "bizcal", "updater"]
