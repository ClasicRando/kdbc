## Overview
Most tests within the project depend on containers that host database instances to confirm the
client can successfully query the database and parse results. This can technically be done with
test infrastructure itself, but I personally found them hard to work with and not as helpful as a
container that is started manually. Below you can find the template containers that have been used
by developers to run tests for each client.

```
./podmand-test-setup
```
