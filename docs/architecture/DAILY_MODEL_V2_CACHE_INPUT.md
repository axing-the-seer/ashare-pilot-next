# Daily model v2 cache input

`ashare_research_app.daily_model_v2_cache` is the cache-building command used
by the existing personal daily cycle. This slice brings that data entry and its
source-bound reader into the clean research tree. It does not migrate the
personal cycle, train a model, publish a recommendation, or activate a champion.

The producer retains `daily-model-v2-cache/v1`: source manifest identifiers and
hashes, risk history binding, array file hashes, dtypes, shapes, price metadata,
and the executing cache code hash are retained. The executing code hash changes
when the producer source changes, so a newly built cache can have a different
cache ID even when its array files and source bindings are byte-identical.

To read and validate an already built cache through the same data command, pass
`--repo`, `--panel-manifest-id`, `--overlay-manifest-id`, `--risk-id`, `--as-of`
and `--verify-cache-id`. Verification is read-only. It checks the source IDs,
hashes and as-of bindings, the individual array hashes, shape and dtype,
feature order and value domains, alignment, and price dates. It validates each
day with a short-lived mapping, closes that mapping, and does not read model
weights. It does not prove first-delivery time, completeness of the upstream
market data, statistical quality or production eligibility.

The cache producer and reader require nonempty, strictly increasing ISO dates,
unique symbols, and feature and label axes aligned to those dates and symbols.
The labels must contain all three declared horizons, though individual missing
labels remain `NaN`. The reader also checks the price array axes. Before reusing
an existing cache directory, the producer verifies its expected contents; a
conflict is rejected without replacing the existing directory.

These checks bind the cache to its declared source IDs and check its own file
hashes; they do not independently derive the cached values from the source
panel. In particular, a self-consistent cache with its label columns swapped
can pass shape and hash checks. `validated` alone therefore does not prove that
the label values correspond to the declared 5/10/20-day order or that the
cache was produced by the claimed source execution. A training qualification
must establish provenance and label semantics separately.

The checked-in semantic document is the exact ordered `production50` document
from the previously reviewed registry, with SHA-256
`07b586cfb06aa1012f935e883946f7623f3c2bac4556225c1a8a8359562ffcb4`.
Its first 36 columns are the panel and the remaining 14 are the financial
overlay. It is a frozen compatibility bridge for this input format. When the
full feature registry is migrated, its semantic document must match this hash
and the reader should use that registry rather than maintaining two versions.

The normal personal refit still requires the personal training entry, model
source and provenance binding, coverage qualification, and other research
dependencies. Until those are migrated and tested, this cache reader alone
does not repair the installed app's automatic refit.
