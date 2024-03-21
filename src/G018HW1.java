import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class G018HW1{
    static void ExactOutliers(List<Tuple2<Float, Float>> listOfPoints, float D, int M, int K) {
        // Implement ExactOutliers logic here
        System.out.println("Executing ExactOutliers with parameters: D=" + D + ", M=" + M + ", K=" + K);
        // Add your code here
    }
    static void MRApproxOutliers(JavaRDD<Tuple2<Float, Float>> inputPoints, float D, int M, int K) {
        // Implement MRApproxOutliers logic here
        System.out.println("Executing MRApproxOutliers with parameters: D=" + D + ", M=" + M + ", K=" + K);
        // Add your code here
    }

    public static void main(String[] args) {
        if (args.length < 5) {
            System.err.println("Usage: PointProcessing <inputFilePath> <D> <M> <K> <L>");
            System.exit(1);
        }

        // Retrieve command-line arguments
        String inputFilePath = args[0];
        float D = Float.parseFloat(args[1]);
        int M = Integer.parseInt(args[2]);
        int K = Integer.parseInt(args[3]);
        int L = Integer.parseInt(args[4]);

        // Create a Spark context
        SparkConf conf = new SparkConf(true).setAppName("G018HW1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // Print command-line arguments
        System.out.println("Input File Path: " + inputFilePath);
        System.out.println("D: " + D);
        System.out.println("M: " + M);
        System.out.println("K: " + K);
        System.out.println("L: " + L);

        // Read input points into an RDD of strings
        JavaRDD<String> rawData = sc.textFile(inputFilePath);

        // Transform into RDD of points (pairs of integers)
        JavaRDD<Tuple2<Float, Float>> inputPoints = rawData.map(line -> {
            String[] parts = line.split(",");
            float x = Float.parseFloat(parts[0]);
            float y = Float.parseFloat(parts[1]);
            return new Tuple2<>(x, y);
        });

        // Repartition RDD into L partitions
        inputPoints = inputPoints.repartition(L);

        // Print total number of points
        long totalPoints = inputPoints.count();
        System.out.println("Total number of points: " + totalPoints);

        if (totalPoints <= 200000) {
            // Collect points into a list
            List<Tuple2<Float, Float>> listOfPoints = inputPoints.collect();

            // Execute ExactOutliers
            long startTimeExact = System.currentTimeMillis();
            ExactOutliers(listOfPoints, D, M, K);
            long endTimeExact = System.currentTimeMillis();
            System.out.println("ExactOutliers running time: " + (endTimeExact - startTimeExact) + " milliseconds");
        }

        // Execute MRApproxOutliers
        long startTimeMRApprox = System.currentTimeMillis();
        MRApproxOutliers(inputPoints, D, M, K);
        long endTimeMRApprox = System.currentTimeMillis();
        System.out.println("MRApproxOutliers running time: " + (endTimeMRApprox - startTimeMRApprox) + " milliseconds");


        // Close Spark context
        sc.close();
    }
}
