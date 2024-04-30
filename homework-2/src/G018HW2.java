import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import java.util.*;
import java.util.List;
import java.util.ArrayList;

/**
 * Class G018HW2 implements algorithms for outlier detection using Apache Spark.
 * Group 18
 * Authors: Lorenzo Cazzador, Giovanni Cinel, Gianluca Nordio
 */
public class G018HW2{

    static JavaSparkContext sc;
    /**
     * Computes outliers using MapReduce approach based on specified parameters.
     * @param pointsRDD RDD of points.
     * @param D Distance defining when to count a point as close.
     * @param M Number of points close in order to not be an outlier.
     */
    public static void MRApproxOutliers(JavaRDD<Tuple2<Float,Float>> pointsRDD, Float D, int M) {

        // Input RDD: points
        // Output RDD: cell with coordinates (i,j) as key and number of points in that cell as value
        JavaPairRDD<Tuple2<Long, Long>, Long> cellRDD = pointsRDD.mapToPair(point -> {
            double cellSide = D / (2 * Math.sqrt(2));
            long i = (long) Math.floor(point._1() / cellSide);
            long j = (long) Math.floor(point._2() / cellSide);
            return new Tuple2<>(new Tuple2<>(i, j), 1L);
        }).reduceByKey(Long::sum).cache();


        List<Tuple2<Tuple2<Long, Long>, Long>> cellList = cellRDD.collect();
        Map<Tuple2<Long,Long>, Long> cellMap = new HashMap<>();
        for (Tuple2<Tuple2<Long, Long>, Long> cell : cellList) {
            cellMap.put(cell._1(), cell._2());
        }

        // Input RDD: cell coordinates as a key with number of points inside as value
        // Output RDD: cell coordinates as a key with pair (number of points in the cell, (N3, N7)) as value
        JavaPairRDD<Tuple2<Long, Long>, Tuple2<Long, Tuple2<Long, Long>>> cellInfoRDD = cellRDD.mapToPair(cell -> {
            long N3 = calculateN3(cell._1(), cellMap);
            long N7 = calculateN7(cell._1(), cellMap);
            return new Tuple2<>(cell._1(), new Tuple2<>(cell._2(), new Tuple2<>(N3, N7)));
        });

       // Compute the number of sure outliers
        long sureOutliers = cellInfoRDD.filter(cell -> {
            long N7 = cell._2()._2()._2();
            return N7 <= M;
        }).map(cell -> cell._2()._1()).reduce(Long::sum);


        // Compute the number of uncertain outliers
        long uncertainPoints = cellInfoRDD.filter(cell -> {
            long N3 = cell._2()._2()._1();
            long N7 = cell._2()._2()._2();
            return (N3 <= M && N7 > M);
        }).map(cell -> cell._2()._1()).reduce(Long::sum);

        // Print results
        System.out.println("Number of sure outliers = " + sureOutliers);
        System.out.println("Number of uncertain points = " + uncertainPoints);
    }


    /**
     * Computes the number of elements in the area of size 3x3 around a cell.
     * @param cell The cell coordinates.
     * @param cellRDD The map containing cell coordinates as key and number of points inside as value.
     * @return The number of points in the 3x3 area around the cell.
     */
    public static long calculateN3(Tuple2<Long, Long> cell, Map<Tuple2<Long,Long>, Long> cellRDD) {
        long i = cell._1();
        long j = cell._2();
        long count = 0;
        for (int x = -1; x <= 1; x++) {
            for (int y = -1; y <= 1; y++) {
                Tuple2<Long, Long> neighbour = new Tuple2<>(i + x, j + y);
                count += cellRDD.getOrDefault(neighbour, 0L);
            }
        }
        return count;
    }

    /**
     * Computes the number of elements in the area of size 7x7 around a cell.
     * @param cell The cell coordinates.
     * @param cellRDD The map containing cell coordinates as key and number of points inside as value.
     * @return The number of points in the 7x7 area around the cell.
     */
    public static long calculateN7(Tuple2<Long, Long> cell, Map<Tuple2<Long,Long>, Long> cellRDD) {
        long i = cell._1();
        long j = cell._2();
        long count = 0;
        for (int x = -3; x <= 3; x++) {
            for (int y = -3; y <= 3; y++) {
                Tuple2<Long, Long> neighbour = new Tuple2<>(i + x, j + y);
                count += cellRDD.getOrDefault(neighbour, 0L);
            }
        }
        return count;
    }

    // Define a Point class to represent points in 2D space
    public static class Point {
        protected double x;
        protected double y;

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }
    }

    // Calculate Euclidean distance between two points
    private static double euclideanDistance(Point p1, Point p2) {
        double dx = p1.x - p2.x;
        double dy = p1.y - p2.y;
        return Math.sqrt(dx * dx + dy * dy);
    }

    public static List<Point> SequentialFFT(List<Point> points, int K) {
        List<Point> centers = new ArrayList<>();

        // Choose the first point arbitrarily
        Point firstCenter = points.get(0);
        centers.add(firstCenter);
        points.remove(firstCenter);

        double[] minDistanceFromCenter = new double[points.size()];
        for (int i = 0; i < points.size(); i++) {
            minDistanceFromCenter[i] = euclideanDistance(firstCenter, points.get(i));
        }

        // Repeat until we have K centers
        while (centers.size() < K) {
            // Find the point farthest from the current set of centers
            Point farthestPoint = null;
            double maxDistance = Double.NEGATIVE_INFINITY;

            // Find the farthest point from the centers
            for (int i = 0; i < points.size(); i++) {
                if (minDistanceFromCenter[i] > maxDistance) {
                    maxDistance = minDistanceFromCenter[i];
                    farthestPoint = points.get(i);
                }
            }

            // Add the farthest point to the centers
            centers.add(farthestPoint);
            points.remove(farthestPoint);

            // For each point update the distance from the nearest center
            for (int i = 0; i < points.size(); i++) {
                double distFromLastCenter = euclideanDistance(farthestPoint, points.get(i));
                minDistanceFromCenter[i] = Math.min(minDistanceFromCenter[i], distFromLastCenter);
            }
        }

        return centers;
    }

    public static float MRFFT(JavaSparkContext sc, JavaRDD<Tuple2<Float, Float>> inputPoints, int k) {
        return 1;
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: java Main <inputFilePath> <M> <K> <L>");
            System.exit(1);
        }

        String inputFilePath = args[0];
        int M = Integer.parseInt(args[1]);
        int K = Integer.parseInt(args[2]);
        int L = Integer.parseInt(args[3]);

        // Create a Spark context
        SparkConf conf = new SparkConf(true).setAppName("G018HW2");
        sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // Print command-line arguments
        System.out.println("Input File Path: " + inputFilePath);
        System.out.println("M: " + M);
        System.out.println("K: " + K);
        System.out.println("L: " + L);

        // Read input points into an RDD of strings
        JavaRDD<String> rawData = sc.textFile(inputFilePath);

        // Transform raw data into an RDD of points (pairs of floats)
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

        // Execute MRFFT to get radius D
        float D = MRFFT(sc, inputPoints, K);

        long startTimeMRApprox = System.currentTimeMillis();
        MRApproxOutliers(inputPoints, D, M);
        long endTimeMRApprox = System.currentTimeMillis();
        System.out.println("Running time of MRApproxOutliers = " + (endTimeMRApprox - startTimeMRApprox) + " ms");

        // Test SequentialFFT method
        List<Point> points = new ArrayList<>();
        for (Tuple2<Float, Float> t : inputPoints.collect()) {
            points.add(new Point(t._1(), t._2()));
        }
        int k_fft = 5;
        List<Point> centers = SequentialFFT(points, k_fft);
        System.out.println("Centers returned by SequentialFFT");
        for (Point p : centers) {
            System.out.println("Punto (" + p.x + "," + p.y + ")");
        }

        sc.close();
    }
}
