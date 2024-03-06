package com.hamza.ch03_sparkfirstprogram;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkFirstProgram {

    public static void main(String[] args) {
        try (final SparkSession spark = SparkSession.builder()
                .appName("SparkFirstProgram")
                .master("local[*]")
                .getOrCreate();
             final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())
        ) {

            final List<Integer> data = Stream.iterate(1, n -> n + 1)
                    .limit(5)
                    .collect(Collectors.toList());
//            data.forEach(System.out::println);
            final JavaRDD<Integer> myRdd = sc.parallelize(data);
            System.out.printf("Total elements in RDD: %d%n", myRdd.count());
            System.out.printf("Total number of default partitions in RDD: %d%n", myRdd.getNumPartitions());
        }

    }
}
