import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import java.util.*;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Class G018HW2
 * Group 18
 * Authors: Lorenzo Cazzador, Giovanni Cinel, Gianluca Nordio
 */
public class G018HW2{

    // Shared Spark context for the application
    static JavaSparkContext sc;

    /**
     * Uses a MapReduce approach to detect outliers in a dataset of 2D points.
     * Outliers are identified based on the distance between points and the number of neighboring points within a specified distance.
     *
     * @param pointsRDD RDD of 2D points as tuples (x, y)
     * @param D The distance threshold to consider a point close to another
     * @param M The minimum number of neighboring points required for a point to not be considered an outlier
     */
    public static void MRApproxOutliers(JavaRDD<Tuple2<Float,Float>> pointsRDD, Float D, int M) {

        // Input RDD: points
        // Output RDD: cell with coordinates (i,j) as key and number of points in that cell as value
        JavaPairRDD<Tuple2<Long, Long>, Long> cellRDD = pointsRDD.mapToPair(point -> {
            double cellSide = D / (2 * Math.sqrt(2));
            long i = (long) Math.floor(point._1() / cellSide);
            long j = (long) Math.floor(point._2() / cellSide);
            return new Tuple2<>(new Tuple2<>(i, j), 1L);
        }).reduceByKey(Long::sum).persist(StorageLevel.MEMORY_AND_DISK());

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
         JavaRDD<Long> sureOutliersRDD = cellInfoRDD.filter(cell -> {
            long N7 = cell._2()._2()._2();
            return N7 <= M;
        }).map(cell -> cell._2()._1());

        long sureOutliers = 0;
        if (!sureOutliersRDD.isEmpty()) {
            sureOutliers = sureOutliersRDD.reduce(Long::sum);
        }

        // Compute the number of uncertain outliers
        JavaRDD<Long> uncertainPointsRDD = cellInfoRDD.filter(cell -> {
            long N3 = cell._2()._2()._1();
            long N7 = cell._2()._2()._2();
            return (N3 <= M && N7 > M);
        }).map(cell -> cell._2()._1());

        long uncertainPoints = 0;
        if (!uncertainPointsRDD.isEmpty()) {
            uncertainPoints = uncertainPointsRDD.reduce(Long::sum);
        }

        // Print results
        System.out.println("Number of sure outliers = " + sureOutliers);
        System.out.println("Number of uncertain points = " + uncertainPoints);
    }


    /**
     * Computes the number of elements in the area of size 3x3 around a cell.
     *
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
     *
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

    /**
     * Calculates the Euclidean distance between two 2D points.
     *
     * @param p1 The first point as a tuple (x, y)
     * @param p2 The second point as a tuple (x, y)
     * @return The Euclidean distance between the two points
     */
    private static float euclideanDistance(Tuple2<Float, Float> p1, Tuple2<Float, Float> p2) {
        float dx = p1._1() - p2._1();
        float dy = p1._2() - p2._2();
        return (float) Math.sqrt(dx * dx + dy * dy);
    }

    /**
     * Implements the Sequential FFT algorithm to select a specified number of centers from a dataset of points.
     * The algorithm iteratively selects the point that is the furthest from any existing center.
     *
     * @param points A list of 2D points as tuples (x, y)
     * @param K The number of centers to select
     * @return A list of selected centers
     */
    public static List<Tuple2<Float, Float>> SequentialFFT(List<Tuple2<Float, Float>> points, int K) {

        List<Tuple2<Float, Float>> centers = new ArrayList<>();

        // Choose the first point arbitrarily
        int randomIndex = ThreadLocalRandom.current().nextInt(points.size());
        Tuple2<Float, Float> firstCenter = points.get(randomIndex);
        centers.add(firstCenter);

        // Initialize minimum distance from any center
        float[] minDistanceFromCenter = new float[points.size()];
        for (int i = 0; i < points.size(); i++) {
            minDistanceFromCenter[i] = euclideanDistance(firstCenter, points.get(i));
        }

        // Repeat until we have K centers
        while (centers.size() < Math.min(K, points.size())) {
            // Find the point farthest from the current set of centers
            Tuple2<Float, Float> farthestPoint = points.get(0);
            float maxDistance = Float.NEGATIVE_INFINITY;

            // Find the farthest point from the centers
            for (int i = 0; i < points.size(); i++) {
                if (minDistanceFromCenter[i] > maxDistance) {
                    maxDistance = minDistanceFromCenter[i];
                    farthestPoint = points.get(i);
                }
            }

            // Add the farthest point to the centers
            centers.add(farthestPoint);

            // For each point update the distance from the nearest center
            for (int i = 0; i < points.size(); i++) {
                float distFromLastCenter = euclideanDistance(farthestPoint, points.get(i));
                minDistanceFromCenter[i] = Math.min(minDistanceFromCenter[i], distFromLastCenter);
            }
        }

        return centers;
    }
    /**
     * Uses a MapReduce-based FFT approach to find a suitable radius for clustering.
     * This process involves multiple rounds of computation to refine the center selection and radius.
     *
     * @param P An RDD of 2D points as tuples (x, y)
     * @param K The number of centers to select
     * @return The calculated maximum radius
     */
    public static float MRFFT(JavaRDD<Tuple2<Float, Float>> P, int K) {
        // ROUND 1
        long startTimeMRFFTRound1 = System.currentTimeMillis();

        JavaRDD<Tuple2<Float, Float>> coresets = P.mapPartitions(points -> {
            List<Tuple2<Float, Float>> pointsList = new ArrayList<>();
            points.forEachRemaining(pointsList::add);
            List<Tuple2<Float, Float>> coresets1 = SequentialFFT(pointsList, K);
            return coresets1.iterator();
        }).persist(StorageLevel.MEMORY_AND_DISK());
        coresets.count();

        long endTimeMRFFTRound1 = System.currentTimeMillis();
        System.out.println("Running time of MRFFT Round 1 = " + (endTimeMRFFTRound1 - startTimeMRFFTRound1) + " ms");

        // ROUND 2
        long startTimeMRFFTRound2 = System.currentTimeMillis();

        List<Tuple2<Float,Float>> cor = coresets.collect();
        List<Tuple2<Float, Float>> centers = SequentialFFT(cor, K);
        Broadcast<List<Tuple2<Float, Float>>> sharedVar = sc.broadcast(centers);

        long endTimeMRFFTRound2 = System.currentTimeMillis();
        System.out.println("Running time of MRFFT Round 2 = " + (endTimeMRFFTRound2 - startTimeMRFFTRound2) + " ms");

        // ROUND 3
        long startTimeMRFFTRound3 = System.currentTimeMillis();

        Float R = P.map(point -> {
            List<Tuple2<Float, Float>> inputCenters = sharedVar.value();
            float minDistance = Float.POSITIVE_INFINITY;

            for (Tuple2<Float, Float> center : inputCenters) {
                float dist = euclideanDistance(point, center);
                if (dist < minDistance) {
                    minDistance = dist;
                }
            }
            return minDistance;
        }).reduce(Float::max);

        long endTimeMRFFTRound3 = System.currentTimeMillis();
        System.out.println("Running time of MRFFT Round 3 = " + (endTimeMRFFTRound3 - startTimeMRFFTRound3) + " ms");

        return R;
    }
    /**
     * The main function initializes the Spark environment and executes outlier detection algorithms.
     * The results of the outlier detection are printed to the console.
     *
     * @param args Command-line arguments: <inputFilePath> <M> <K> <L>
     */
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
        conf.set("spark.locality.wait", "0s");
        sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // Print command-line arguments
        System.out.println(inputFilePath + " M=" + M + " K=" + K + " L=" + L);

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
        inputPoints = inputPoints.repartition(L).cache();

        // Print total number of points
        long totalPoints = inputPoints.count();
        System.out.println("Number of points = " + totalPoints);

        // Execute MRFFT to get radius D
        float D = MRFFT(inputPoints, K);
        System.out.println("Radius = " + D);

        // Execute MRApproxOutliers with radius D returned by MRFFT
        long startTimeMRApprox = System.currentTimeMillis();
        MRApproxOutliers(inputPoints, D, M);
        long endTimeMRApprox = System.currentTimeMillis();
        System.out.println("Running time of MRApproxOutliers = " + (endTimeMRApprox - startTimeMRApprox) + " ms");

        // Close Spark context
        sc.close();
    }
}
