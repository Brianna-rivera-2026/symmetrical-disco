* Add TLS to redis and postgres
* Add internal network for between process communication that isn't exposed
* Add a gateway before the API with https certificate, only this exposed from the network
* Add openshift helm chart
* Add health and readiness probes
* Plug openshift otel collection
* Implement a "Max Memory Usage" threshold that allows the worker to stop accepting new jobs, finish its current TaskGroup, and exit gracefully.
* Add pgbouncer before the postgresql Calculate the absolute maximum number of connections PgBouncer can safely accept from this specific service. Set your HPA maxReplicas so that maxReplicas times x pool_size never exceeds that threshold.
