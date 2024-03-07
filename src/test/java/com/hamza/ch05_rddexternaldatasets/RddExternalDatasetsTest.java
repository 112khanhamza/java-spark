package com.hamza.ch05_rddexternaldatasets;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.AccessDeniedException;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class RddExternalDatasetsTest {

    private final SparkConf sparkConf = new SparkConf()
            .setAppName("RddExternalDatasetsTest")
            .setMaster("local[*]");


    @ParameterizedTest
    @ValueSource(strings = {
            "src\\test\\resources\\1000words.txt",
            "src\\test\\resources\\wordslist.txt.gz"
    })
    @DisplayName("Test loading local text files into spark rdd")
    void test1(final String localFilePath) {
        try (final JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            final JavaRDD<String> myRdd = sc.textFile(localFilePath);
            System.out.printf("Total lines in file: %d%n", myRdd.count());
            System.out.println("First 10 words: ");
            myRdd.take(10).forEach(System.out::println);
            System.out.println("------------------------\n");
        }
    }

    @ParameterizedTest
    @MethodSource("getFilePaths")
    @DisplayName("Test loading local text files into spark rdd using method source")
    void test2(final String localFilePath) {
        try (final JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            final JavaRDD<String> myRdd = sc.textFile(localFilePath);
            System.out.printf("Total lines in file: %d%n", myRdd.count());
            System.out.println("First 10 words: ");
            myRdd.take(10).forEach(System.out::println);
            System.out.println("------------------------\n");
        }
    }

    private static Stream<Arguments> getFilePaths() {
        return Stream.of(
                Arguments.of(Path.of("src", "test", "resources", "1000words.txt").toString()),
                Arguments.of(Path.of("src", "test", "resources", "wordslist.txt.gz").toString())
        );
    }

    @Test
    @DisplayName("Test loading whole directory into spark rdd")
    void test3() {
        try (final JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            final String testDirectoryPath = Path.of("src", "test", "resources").toString();
            final JavaPairRDD<String, String> myRdd = sc.wholeTextFiles(testDirectoryPath);

            System.out.printf("Total number of files in directory [%s] = %d%n", testDirectoryPath, myRdd.count());

            myRdd.collect().forEach(tuple -> {
                System.out.printf("File Name: %s%n", tuple._1);
                System.out.println("------------------------");
                if (tuple._1.endsWith(".properties")) {
                    System.out.printf("Contents of [%s]: %n", tuple._1);
                    System.out.println(tuple._2);
                }
            });
        }
    }

    @Test
    @DisplayName("Test loading csv file into Spark Rdd")
    void test4() {
        try (final JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            final String filePath = Path.of("src", "test", "resources", "dma.csv").toString();
            final JavaRDD<String> data = sc.textFile(filePath);
            System.out.printf("Total lines in csv file [%s]: %d%n", filePath, data.count());
            System.out.printf("CSV Headers -> %s%n", data.first());

            System.out.println("Printing first 10 lines of csv");
            data.take(10).forEach(System.out::println);

            final JavaRDD<String[]> lines = data.map(line -> line.split(","));
            lines.take(5).forEach(line -> System.out.println(String.join("|", line)));
        }
    }

    @Test
    @DisplayName("Test loading Amazon S3 file into Spark RDD")
    void test5() {
        try (final JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            // Can find below keys in AWS IAM
            sc.hadoopConfiguration().set("fs.s3a.access.key", "AWS access-key value");
            sc.hadoopConfiguration().set("fs.s3a.secret.key", "AWS secret-key value");
            sc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazon.com");

            // Read a single text file
            final JavaRDD<String> data = sc.textFile("s3a://112khanhamza/spark/1000words.txt");

            assertThrows(AccessDeniedException.class, data::count);
        }
    }
}
