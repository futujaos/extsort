package com.futujaos.extsort;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public final class Example {
    public static void main(String[] args) {
        final File sourceFile = new File("src.txt");
        final File targetFile = new File("out.txt");

        generateSourceFile(sourceFile);

        final Extsort extsort = new Extsort(sourceFile, targetFile, true);
        try {
            extsort.sort();
        } catch (IOException e) {
            System.err.println("Failed to sort source file");
            e.printStackTrace();
        }
    }

    private static void generateSourceFile(File sourceFile) {
        final long n = (1L << 26); // 64M
        final Random random = new Random();

        try (final BufferedWriter writer = new BufferedWriter(new FileWriter(sourceFile))) {
            for (long i = 0; i < n; i++) {
                final int number = random.nextInt();
                writer.write(String.valueOf(number));
                writer.write("\n");
            }
        } catch (IOException e) {
            System.err.println("Failed to generate source file");
            e.printStackTrace();
            System.exit(1);
        }
    }
}