package com.futujaos.extsort;

import com.futujaos.extsort.merge.Merger;
import com.futujaos.extsort.split.Splitter;

import java.io.*;
import java.util.*;

public class Extsort {
    private static final int DEFAULT_MAX_NUMBERS_IN_CHUNK = 1 << 24; // 16M
    private final File sourceFile;
    private final File targetFile;

    public Extsort(File sourceFile, File targetFile) {
        this.sourceFile = sourceFile;
        this.targetFile = targetFile;
    }

    public void sort() throws IOException {
        final Splitter splitter = new Splitter(sourceFile, DEFAULT_MAX_NUMBERS_IN_CHUNK);
        final List<File> chunks = splitter.split();

        final Merger merger = new Merger(chunks, targetFile, DEFAULT_MAX_NUMBERS_IN_CHUNK);
        merger.merge();

        System.out.println("Done!");
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Usage: java Extsort.class <source_file> <target_file>");
            return;
        }

        if (args.length != 2) {
            System.err.println("Invalid number of arguments: " + args.length + ", expected: 2");
            System.out.println("Usage: java Extsort.class <source_file> <target_file>");
            return;
        }

        final String sourcePath = args[0];
        final String targetPath = args[1];

        final File sourceFile = new File(sourcePath);
        if (!sourceFile.exists()) {
            System.err.println("No such file: " + sourcePath);
            return;
        }

        final File targetFile = new File(targetPath);

        new Extsort(sourceFile, targetFile).sort();
    }
}