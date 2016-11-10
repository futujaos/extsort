package com.futujaos.extsort.split;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Splitter {
    private final File sourceFile;
    private final int maxNumbersInChunk;

    public Splitter(File sourceFile, int maxNumbersInChunk) {
        this.sourceFile = sourceFile;
        this.maxNumbersInChunk = maxNumbersInChunk;
    }

    public List<File> split() throws IOException {
        System.out.println("Splitting source file to chunks...");

        final String chunkPrefix = "chunk_" + System.currentTimeMillis() + '_';
        final List<File> chunks = new ArrayList<>();

        try (final BufferedReader sourceReader = new BufferedReader(new FileReader(sourceFile))) {
            chunks.add(new File(chunkPrefix + "0"));
            int[] array = new int[maxNumbersInChunk];

            int i = 0;
            String line = sourceReader.readLine();
            while (line != null) {
                if (i == maxNumbersInChunk) {
                    System.out.println("Writing " + array.length + " sorted numbers to chunk #" + chunks.size());
                    Arrays.sort(array);
                    final File currentChunk = chunks.get(chunks.size() - 1);
                    writeToChunk(currentChunk, array);
                    chunks.add(new File(chunkPrefix + chunks.size()));
                    i = 0;
                }

                array[i] = Integer.valueOf(line);

                i++;
                line = sourceReader.readLine();
            }

            if (array.length != 0) {
                System.out.println("Writing " + array.length + " sorted numbers to chunk #" + chunks.size());
                Arrays.sort(array);
                final File currentChunk = chunks.get(chunks.size() - 1);
                writeToChunk(currentChunk, array);
            }
        }

        return chunks;
    }

    private void writeToChunk(File chunk, int[] array) throws IOException {
        try (final BufferedWriter chunkWriter = new BufferedWriter(new FileWriter(chunk))) {
            for (int n : array) {
                chunkWriter.write(String.valueOf(n));
                chunkWriter.write("\n");
            }
        }
    }
}
