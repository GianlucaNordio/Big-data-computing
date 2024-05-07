import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
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

    // Calculate Euclidean distance between two points
    private static float euclideanDistance(Tuple2<Float, Float> p1, Tuple2<Float, Float> p2) {
        float dx = p1._1() - p2._1();
        float dy = p1._2() - p2._2();
        return (float) Math.sqrt(dx * dx + dy * dy);
    }

    public static List<Tuple2<Float, Float>> SequentialFFT(List<Tuple2<Float, Float>> points, int K) {
        System.out.println(points.size());

        List<Tuple2<Float, Float>> centers = new ArrayList<>();

        // Choose the first point arbitrarily
        Tuple2<Float, Float> firstCenter = points.get(0);
        centers.add(firstCenter);

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

    public static float MRFFT(JavaSparkContext sc, JavaRDD<Tuple2<Float, Float>> P, int K) {
        // ROUND 1
        long startTimeMRFFTRound1 = System.currentTimeMillis();

        JavaRDD<Tuple2<Float, Float>> coresets = P.mapPartitions((FlatMapFunction<Iterator<Tuple2<Float, Float>>, Tuple2<Float, Float>>) points -> {
            List<Tuple2<Float, Float>> pointsList = new ArrayList<>();
            points.forEachRemaining(pointsList::add);
            List<Tuple2<Float, Float>> coresets1 = SequentialFFT(pointsList, K);
            return coresets1.iterator();
        }).cache();
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
            List<Tuple2<Float, Float>> inputCenters = sharedVar.value(); // TODO confirm this
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
        inputPoints = inputPoints.repartition(L);

        // Print total number of points
        long totalPoints = inputPoints.count();
        System.out.println("Number of points = " + totalPoints);

        // Execute MRFFT to get radius D
        //float D = 1;
        float D = MRFFT(sc, inputPoints, K);

        long startTimeMRApprox = System.currentTimeMillis();
        MRApproxOutliers(inputPoints, D, M);
        long endTimeMRApprox = System.currentTimeMillis();
        System.out.println("Running time of MRApproxOutliers    = " + (endTimeMRApprox - startTimeMRApprox) + " ms");

        /*
        // Test SequentialFFT method
        List<Tuple2<Float, Float>> points = inputPoints.collect();
        int k_fft = 5;
        List<Tuple2<Float, Float>> centers = SequentialFFT(points, k_fft);
        System.out.println("Centers returned by SequentialFFT");
        for (Tuple2<Float, Float> p : centers) {
            System.out.println("Punto (" + p._1() + "," + p._2() + ")");
        }
        */
        sc.close();
    }
}
