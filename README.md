## JSON Data and Query Generator

The growing popularity of JSON as exchange and storage format in business and analytical applications led to its rapid dissemination, thus making a timely storage and processing of JSON documents crucial for organizations. Consequently, specialized JSON document stores are ubiquitously used for diverse domain-specific workloads, while a JSON-specific benchmark is missing.

In this repository, we provide an example implementation of **DeepBench**, an extensible, scalable benchmark that addresses nested JSON data, as well as queries over JSON documents. DeepBench features configurable domain-independent (e. g., varying document sizes, concurrent users) and JSON-specific scale levels (e. g., object, array nesting).

The package `json_data_and_query_generator` contains tools to generate random `json` data and corresponding `SQL` queries.
Each of these tools needs as an input a configuration in form of a `json` document describing the fixed structure of the data and the characteristic of the generated queries.

## Setup

Install prerequisites:

```
  pip install .
```

To execute data and query generation based on the example scenario in `examples` (default):

```
  python -m json_data_and_query_generator --num-proc 5
```

with five processes.

If other scenarios should be run, then specify paths to `schema.txt`, `data.txt`, and `config.json` as described in `pipeline.py --help`.

## Support, Feedback, Contributing

This project is open to feature requests/suggestions, bug reports etc. via [GitHub issues](https://github.com/SAP/json-data-and-query-generator/issues). Contribution and feedback are encouraged and always welcome. For more information about how to contribute, the project structure, as well as additional contribution information, see our [Contribution Guidelines](CONTRIBUTING.md).

## Code of Conduct

We as members, contributors, and leaders pledge to make participation in our community a harassment-free experience for everyone. By participating in this project, you agree to abide by its [Code of Conduct](https://github.com/SAP/.github/blob/main/CODE_OF_CONDUCT.md) at all times.

## Licensing

Copyright (20xx-)20xx SAP SE or an SAP affiliate company and <your-project> contributors. Please see our [LICENSE](LICENSE) for copyright and license information. Detailed information including third-party components and their licensing/copyright information is available [via the REUSE tool](https://api.reuse.software/info/github.com/SAP/json-data-and-query-generator).

## Citation

For more documentation read the following documents. If you find this work useful for your research, please cite:

```
@inproceedings{DBLP:conf/dbtest-ws/Belloni0SR22,
  author       = {Stefano Belloni and
                  Daniel Ritter and
                  Marco Schr{\"{o}}der and
                  Nils R{\"{o}}rup},
  editor       = {Manuel Rigger and
                  Pinar T{\"{o}}z{\"{u}}n},
  title        = {DeepBench: Benchmarking {JSON} Document Stores},
  booktitle    = {DBTest@SIGMOD '22: Proceedings of the 9th International Workshop of
                  Testing Database Systems, Philadelphia, PA, USA, 17 June 2022},
  pages        = {1--9},
  publisher    = {{ACM}},
  year         = {2022},
  url          = {https://doi.org/10.1145/3531348.3532176},
  doi          = {10.1145/3531348.3532176},
  timestamp    = {Sun, 02 Oct 2022 15:58:56 +0200},
  biburl       = {https://dblp.org/rec/conf/dbtest-ws/Belloni0SR22.bib},
  bibsource    = {dblp computer science bibliography, https://dblp.org}
}
```
and / or the usage in systems:

```
@article{DBLP:journals/dbsk/BelloniR22,
  author       = {Stefano Belloni and
                  Daniel Ritter},
  title        = {Benchmarking {JSON} Document Stores in Practice},
  journal      = {Datenbank-Spektrum},
  volume       = {22},
  number       = {3},
  pages        = {217--226},
  year         = {2022},
  url          = {https://doi.org/10.1007/s13222-022-00425-y},
  doi          = {10.1007/s13222-022-00425-y},
  timestamp    = {Sat, 25 Feb 2023 21:35:08 +0100},
  biburl       = {https://dblp.org/rec/journals/dbsk/BelloniR22.bib},
  bibsource    = {dblp computer science bibliography, https://dblp.org}
}
```
