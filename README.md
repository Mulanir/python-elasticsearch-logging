# elasticsearch-logging-handler

This is a minimalistic Elasticsearch logging handler for python. This handler uses only url as an authentication method. For more robust authentication consider [CMRESHandler](https://github.com/cmanaha/python-elasticsearch-logger).

Parameters for **ElasticHandler**:
| Name | Type | Required | Default | Description | Example | 
| --- | --- | --- | --- | --- | --- |
| host | str | True | - | Url for Elasticsearch cluster. Currently, this handler support only basic authentication through providing user and password in the url. | https://user:password@my-cluster.es:9200, https://my-cluster.es:9200 |
| index | str | True | - | Name of the Index to write in. | preprocessing-logs |
| level | int | False | 0 (NOTSET) | Minimal logging level for the handler. Handler will only process messages with level larger than this parameter. | 20, logging.WARNING |
| batch_size | int | False | 1000 | Size of the buffer that stores logs before sending to Elasticsearch. If buffer is full, send batch immediately, without waiting for the **flush_period** | 2000 - send batches of maximum size 2000 |
| flush_period | float | False | 1.0 | Period during which handler will accumulate logs into batch before sending it ot the cluster. | 10.0 - wait 10 seconds before sending |
| timezone | str | False | None | Timezone for which to transform @timestamp for the record. | 'Europe/Amsterdam', 'Australia/Sydney' |

**ElasticHandler** is nonblocking, meaning any logging call e.g. **logging.exception** will not block calling thread. which is useful in the case of high logging load.