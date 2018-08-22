# Optimus

Optimus API is a key-value store which allows Data Scientists to save and
retrieve reference data and model coefficients which are calculated
across millions of users or items. The API resembles a common
key/value store, however the main difference with other REST based
key/value stores is that Optimus provides transactional semantics, which
means that while an update of a new set of keys is in progress no user
will be able to observe the new values until all keys have been
successfully stored and committed.

For more information:
  * Read the [introduction](/docs/optimus/intro.md).
  * Check the [detailed API Interaction](/docs/optimus/api-interaction.md).

## Project structure:

  - **service**: REST API for model store
  - **loader**: Spark job to load the key/value pairs in parallel

## License

Copyright © 2018 Trainline.com - All rights reserved.
