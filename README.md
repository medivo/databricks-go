# Databricks

This library is a Go client for the Databricks [REST API
V2](https://docs.databricks.com/api/latest/index.html#rest-api-2-0).
It's designed to be a minimal client with few dependencies.

# Authentication

This library makes little attempts to handle authentication for you. It
probably needs some more thought on how it should work. For now you can inject
your own `http.Client` to handle authentication for you. You can find more
information of how Databricks handles
[authentication](https://docs.databricks.com/api/latest/authentication.html#authentication)
for details on how to generate tokens.

# Hacking

There is some work to still be done in handling date times. Currently the
client does the lazy method of just using `int64`s in most cases (as the
Databricks API generally uses epoch nanos).

# Features

The client library supports injecting your own `http.Client` using the
`ClientHTTPClient` function.
