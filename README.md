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

### As a library

```java
import com.futujaos.extsort.Extsort;
import com.futujaos.extsort.Generator;

import java.io.File;
import java.io.IOException;

public class Example {
    public static void main(String[] args) {
        final File sourceFile = new File("src.txt");
        final File targetFile = new File("out.txt");

        try {
            new Generator(sourceFile).generate();
        } catch (IOException e) {
            System.err.println("Failed to generate source file");
            e.printStackTrace();
        }

        final Extsort extsort = new Extsort(sourceFile, targetFile, true);
        try {
            extsort.sort();
        } catch (IOException e) {
            System.err.println("Failed to sort source file");
            e.printStackTrace();
        }
    }
}
```

### As a runnable

```
java -cp ... com.futujaos.extsort.Extsort <source_file> <target_file>
```

