# tdb-succinct - succinct data structures used by terminusdb-store
This repository contains all data structures from terminusdb-store, as
well as the logic for loading and storing them.

Note that these types are pretty unoptimized for standalone use. In
particular, this crate assumes tokio will be used for loading and
storing data. The intent is to eventually move such dependencies
behind feature flags and provide synchronous io strategies as well.
