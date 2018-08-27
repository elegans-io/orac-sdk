# orac-sdk

SDK for ORAC API: a collection of ETL and algorithms for ORAC

The main library is written in Scala though submodules are written also in python.

The submodules can be downloaded with the following command:
```bash
git submodule update --init --recursive
```

To update the submodules:
```bash
git submodule update --recursive --remote
```

To create a self contained jar with the spark code type:
```bash
sbt assembly
```

# commands

DISCLAIMER: the readme is a work in progress and is not complete.

## SupermarketETL

### description

ETL program which convert a specific XML format into a format suitable for the item to item encoder NN
It calculate items similarity using LSH algorithm.
The program has a command line help.

### command line 

Sample submission:

```bash
ASSEMBLY_JAR=orac-sdk-assembly-dad9212b5f3ca9daefb9d7bc3c2384c6d963a304.jar
spark-submit --driver-memory 8g --class io.elegans.oracsdk.commands.SupermarketETL ${ASSEMBLY_JAR} --input data.xml --shuffle --output ETL_FOLDER --simThreshold 0.5 --sliding 2 --genMahoutActions --basketToBasket --rankIdToPopularItems 
```

## submodules

```bash
git submodule update --init --recursive
```
