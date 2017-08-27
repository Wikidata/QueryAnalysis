# QueryAnalysis
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Getting Started
### Prerequisites
You need to have `Maven`, `OpenJDK 8` and `Python 2` installed.

### Installing
```shell
$ mvn -T 1.5C install

```

### Running the main Java log analyser
```shell
# Processes the example SPARQL log files into exampleMonthsFolder/exampleMonth/processedLogData
$ mvn -T 1.5C compile exec:java -Dexec.mainClass=general.Main -Dexec.args="-w exampleMonthsFolder/exampleMonth -logging"

# There are more (useful) CLI parameter available, you can list them with:
$ mvn -T 1.5C compile exec:java -Dexec.mainClass=general.Main -Dexec.args="--help"

```

**Important:** In order to not flush the command line with error messages all uncatched Runtime Exceptions are being written to the log files, residing in the logs/ folder, so please take a look into there regularily.

## License
The code in this repository is released under the [Apache 2.0](LICENSE.txt) license. External libraries used may have their own licensing terms.

