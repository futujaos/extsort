# extsort
External sort algorithm.

Then array of data which you want to sort doesn't fit in memory, you can use external sort.

1. Algorithm takes some file with unsorted data as input, sequentially reads file data, sorting
parts of data (chunks) in memory and persisting sorted chunks to temporary files.
2. Algorithm opens all files with sorted chunks and sequentially reads data from them, 
putting data to priority queue, and using this queue as source for result output file with sorted data.

## Build

```
./gradlew build
```

## Usage

```
java Extsort.class <source_file> <target_file>
```
