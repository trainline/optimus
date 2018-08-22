# Optimus Service

REST API for Optimus.

## Usage:

This project uses leiningen for build automation.

To build the project, run:
```
lein do clean, uberjar
```

To run the project:
```
lein run
```

Fire your browser and navigate to `http://localhost:8888` to access the swagger API. For more information on how to use the API, check out the [getting started](../docs/optimus/api-interaction.md) page.

## Running Tests

To run unit tests:
```
lein do clean, midje
```

To run integration tests:
```
lein midje :filter +integration
```

To skip slow tests during development:
```
lein midje :filter -slow :autotest
```

## License

Copyright © 2018 Trainline.com - All rights reserved.
