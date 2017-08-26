# QueryAnalysis
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Getting Started
### Prerequisites
You need to have `Maven`, `OpenJDK 8` and `Python 2` installed.

### Running the main Java log analyser
```shell
# Processes the example SPARQL log files into exampleMonthsFolder/exampleMonth/processedLogData
$ mvn -T 1.5C exec:java -Dexec.mainClass=general.Main -Dexec.args="-w exampleMonthsFolder/exampleMonth -logging"

# There are more (useful) CLI parameter available, you can list them with:
$ mvn -T 1.5C exec:java -Dexec.mainClass=general.Main -Dexec.args="--help"

```

## License
The code in this repository is released under the [Apache 2.0](LICENSE) license. External libraries used may have their own licensing terms.

