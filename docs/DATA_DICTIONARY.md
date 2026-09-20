# Data Dictionary — Blue Ocean Parquet Lake

Two files per session, under one partition directory.

```
<BLUEOCEAN_ROOT>/
└── date=2026-09-18/            ← Hive partition key: the morning the session leads into
    ├── ocea_l1.parquet         ← level 1: executed trades (the tape)
    └── ocea_mbo.parquet        ← level 2: market-by-order events (the book)
```

**Partition semantics.** `date=D` holds the overnight session that ran from ~20:00 ET on the
previous trading day to 04:00 ET on `D`. In UTC that is `D 00:00Z` → `D 08:00Z`, which is why the
partition key is the *morning* date and not the evening one.

**Sort order.** Both files are written sorted by `symbol, ts_recv, sequence`. Consumers can rely on
it and skip their own sort.

**Time zone.** All timestamps are UTC instants stored as `TIMESTAMP WITH TIME ZONE`. A query engine
configured with a local session time zone will render them shifted; set the session to UTC
(`SET TimeZone='UTC'` in DuckDB) before comparing a timestamp to a partition date.

**Coverage: the whole venue.** These files hold **every row the vendor returned** for the session.
The request is `symbols="ALL_SYMBOLS"` and nothing is filtered on the way in, so a symbol absent from
a partition simply did not print that night — there is no screen to second-guess, and no need to
check a universe definition to interpret a gap.

**Scale, for one representative night (`date=2026-09-18`).** 766,142 trade rows and 34,497,498 book
rows. (For comparison, restricting to a ~2,550-symbol screen would have kept 181,955 trades and
2,348,301 book rows — 24% and 6.8%. See the README for how to apply such a filter if you want one.)

---

## `ocea_l1.parquet` — level 1, executed trades

One row per print on the venue.

| Column | Type | Description |
|---|---|---|
| `ts_recv` | `TIMESTAMPTZ` | Capture timestamp: when the vendor's gateway received the message. The canonical sort key — monotonic per symbol and immune to publisher clock skew. |
| `ts_event` | `TIMESTAMPTZ` | Venue timestamp: when the matching engine generated the event. Use for latency analysis against `ts_recv`; prefer `ts_recv` for ordering. |
| `rtype` | `UTINYINT` | Record type discriminator from the DBN schema. `0` for trade records. |
| `publisher_id` | `USMALLINT` | Vendor publisher identifier — the (dataset, venue) pair the record came from. Constant (`107`) in this single-venue dataset; kept so the files stay union-compatible with multi-venue tapes. |
| `instrument_id` | `UINTEGER` | Vendor's numeric instrument key. Stable within a session; resolve through `symbol` across sessions, as it can be reassigned. |
| `action` | `VARCHAR` | Event action. Always `T` (trade) in this file. |
| `side` | `VARCHAR` | Aggressor side: `B` buy, `A` sell (ask), `N` none/unknown when the venue does not disclose it. The basis for any buy/sell imbalance measure. |
| `depth` | `UTINYINT` | Book level the event touched. `0` for trades. |
| `price` | `DOUBLE` | Execution price, already scaled out of the vendor's fixed-point representation. |
| `size` | `UINTEGER` | Executed quantity, in shares. |
| `flags` | `UTINYINT` | DBN bitfield. Bit `0x80` (`128`) marks the last message in an event packet; other bits carry book-state hints. |
| `ts_in_delta` | `INTEGER` | Nanoseconds the vendor's gateway spent between reading and publishing the message. |
| `sequence` | `UINTEGER` | Venue sequence number. Tie-breaks rows sharing a `ts_recv`, and gaps expose dropped messages. |
| `symbol` | `VARCHAR` | Ticker as printed by the venue. The join key to every other dataset. |
| `date` | `DATE` | Session partition date, also materialised in the file (redundant with the Hive key). |

## `ocea_mbo.parquet` — level 2, market by order

One row per **order-level** book event. Market by order, not market by price: every individual
order's lifecycle is visible, so the book can be reconstructed exactly and individual participants
can be tracked across it. This is what makes queue position, order-placement rhythm and
cancel-to-fill behaviour measurable — none of which survives aggregation to price levels.

Shares every column above except `depth`, and adds:

| Column | Type | Description |
|---|---|---|
| `action` | `VARCHAR` | Order lifecycle event. Observed mix on a representative night: `C` cancel (1,057,095), `A` add (956,400), `T` trade (181,955), `F` fill (150,292), `R` book reset/clear (2,559). The schema also defines `M` (modify), which did not appear that night. |
| `side` | `VARCHAR` | Resting side of the order: `B` bid, `A` ask, `N` none (used by `R` resets). |
| `price` | `DOUBLE` | Order price. `NULL` on `R` resets, which carry no price. |
| `size` | `UINTEGER` | Order quantity, in shares. |
| `order_id` | `UBIGINT` | Venue order identifier. The thread that links an add to its later modifies, fills and cancel — the reason this file is worth ~13× the row count of the tape. |
| `channel_id` | `UTINYINT` | Vendor multiplex channel the message arrived on. |
| `rtype` | `UTINYINT` | `160` for MBO records. |

### Reading it

```sql
-- DuckDB: one night, one symbol, in order
SET TimeZone='UTC';
SELECT ts_recv, price, size, side
FROM read_parquet('C:/data/blueocean/date=2026-09-18/ocea_l1.parquet')
WHERE symbol = 'AADX'
ORDER BY ts_recv, sequence;

-- The whole lake as one table, partition pruned by the Hive key
SELECT date, symbol, sum(size * price) AS notional
FROM read_parquet('C:/data/blueocean/date=*/ocea_l1.parquet', hive_partitioning=true)
WHERE date BETWEEN DATE '2026-09-01' AND DATE '2026-09-18'
GROUP BY 1, 2;

-- Cancel-to-add ratio per symbol: a book-pressure measure only level 2 supports
SELECT symbol,
       count(*) FILTER (WHERE action = 'C')::DOUBLE
       / nullif(count(*) FILTER (WHERE action = 'A'), 0) AS cancel_to_add
FROM read_parquet('C:/data/blueocean/date=2026-09-18/ocea_mbo.parquet')
GROUP BY 1
ORDER BY 2 DESC;
```

---

## Reference inputs (PostgreSQL)

One table, read-only, and it never influences the contents of a partition.

| Table | Columns used | Purpose |
|---|---|---|
| `bizcal` | `bizdate`, `todaysdate` | Trading-day calendar. Decides **which sessions to request** and whether today is one. Nothing else in the pipeline consults the database. |

There is no universe table, security master or screen: everything the vendor returns for a requested
session is stored. Narrowing the lake to symbols of interest is documented as a read-time query in
the examples above, or — if it must happen at write time — as a semi-join in `merge_and_sort()`,
described in the README and in the function's own docstring.

This is what makes "why did this symbol stop appearing?" a question about the market rather than
about the pipeline's configuration on the night in question.
