# materializer

## Compute abox inferences implied by an ontology for multiple datasets

`materializer` loads an OWL ontology and inputs any number of Abox datasets, computing inferred class assertions and object property assertions for each. 
Outputs for all input files are combined into a single N-Quads output file.

```
Usage: materializer [options]
  --usage  <bool>
        Print usage and exit
  --help | -h  <bool>
        Print help message and exit
  --ontology-file  <string>
  --input  <string>
  --output  <string>
  --suffix-output  <boolean value>
  --output-graph-name  <string?>
  --suffix-graph  <boolean value>
  --reasoner  <reasoner>
  --mark-direct-types  <boolean value>
  --output-indirect-types  <boolean value>
  --output-inconsistent  <boolean value>
  --parallelism  <int>
  --filter-graph-query  <string?>
  ```
