# DecisionMapper

Spark pipeline for csv data transformations

## Requirements
- Sbt 1.3.13
- Spark 3.0.0

## Usage

```bash
sbt assembly
spark-submit --master "local[*]" target/scala-2.12/decisionmapper-assembly.jar examples/sample.csv examples/transformations.json
```
