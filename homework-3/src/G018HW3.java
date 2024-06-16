
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.*;

import java.util.concurrent.Semaphore;

/**
 * Processes streaming data and implements exact 
 * and approximate frequent item algorithms.
 * Group 18
 * Authors: Lorenzo Cazzador, Giovanni Cinel, Gianluca Nordio
 */
public class G018HW3 {

    /**
     * Checks if a given value is not in the range (0, 1).
     *
     * @param value the value to check
     * @return true if the value is <= 0 or >= 1, false otherwise
     */
    public static boolean isNotInRange(double value) {
        return value <= 0 || value >= 1;
    }

    /**
     * The main method that initializes Spark Streaming, processes the stream, and performs 
     * exact and approximate frequent item calculations.
     *
     * @param args command line arguments: n, phi, epsilon, delta, portExp
     * @throws Exception if an error occurs during processing
     */
    public static void main(String[] args) throws Exception {
        // Validate command line arguments
        if (args.length != 5) {
            throw new IllegalArgumentException("USAGE: n, phi, epsilon, delta, portExp");
        }

        // Parse command line arguments
        int n = Integer.parseInt(args[0]);
        float phi = Float.parseFloat(args[1]);
        float epsilon = Float.parseFloat(args[2]);
        float delta = Float.parseFloat(args[3]);
        int portExp = Integer.parseInt(args[4]);

        // Validate that phi, epsilon, and delta are within the range (0, 1)
        if( isNotInRange(phi)  || isNotInRange(epsilon) || isNotInRange(delta) ){
            throw new IllegalArgumentException("phi, epsilon and delta must be between 0 and 1");
        }

        // Print input parameters
        System.out.println("INPUT PROPERTIES");
        System.out.print("n = " + n);
        System.out.print(" phi = " + phi);
        System.out.print(" epsilon = " + epsilon);
        System.out.print(" delta = " + delta);
        System.out.println(" port = " + portExp);

        // Set up Spark configuration and streaming context
        SparkConf conf = new SparkConf(true).setMaster("local[*]").setAppName("G018HW3");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(10));
        sc.sparkContext().setLogLevel("ERROR");

        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        // Initialize data structures for stream processing
        long[] streamLength = new long[1];
        streamLength[0] = 0L;
        HashMap<Long, Long> histogram = new HashMap<>();
        HashMap<Long, Long> stickySampling = new HashMap<>();
        ArrayList<Long> reservoir = new ArrayList<>();

        // Calculate the sample size for reservoir sampling
        int m = (int) Math.ceil(1 / phi);
        Random random = new Random();

        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                .foreachRDD((batch, time) -> {
                    if (streamLength[0] < n) {
                        long batchSize = batch.count();

                        // Adjust batch size if it exceeds the remaining required item
                        if(batchSize + streamLength[0] > n)
                            batchSize = n - streamLength[0];

                        List<Long> elements = batch.map(Long::parseLong).collect();
                        int counter = 0;
                        for (Long item : elements){
                            counter++;

                            // Update the exact histogram
                            histogram.put(item, histogram.getOrDefault(item, 0L) + 1);

                            // Update sticky sampling
                            if (stickySampling.containsKey(item)) {
                                stickySampling.put(item, stickySampling.get(item) + 1);
                            } else {
                                double x = random.nextDouble();
                                double r = Math.log(1.0 / (phi * delta)) / epsilon;
                                double p = r / n;
                                if (x <= p) {
                                    stickySampling.put(item, 1L);
                                }
                            }

                            // Update reservoir sampling
                            int size = reservoir.size();
                            if (size < m) {
                                reservoir.add(item);
                            } else {
                                double x = random.nextDouble();
                                double p = (double) m / (streamLength[0] + counter);
                                if (x <= p) {
                                    reservoir.remove(random.nextInt(size));
                                    reservoir.add(item);
                                }
                            }

                            // Stop processing if the batch size limit is reached
                            if(batchSize == counter)
                                break;
                        }

                        streamLength[0] += batchSize;

                        // Release the semaphore if the stream length reaches n
                        if (streamLength[0] >= n) {
                            stoppingSemaphore.release();
                        }
                    }
                });

        sc.start();
        stoppingSemaphore.acquire();
        sc.stop(false, false);

        // Print results of the exact algorithm
        System.out.println("EXACT ALGORITHM");
        System.out.println("Number of items in the data structure = " + histogram.size());

        double freqThreshold = phi * streamLength[0];
        ArrayList<Long> trueFrequentItems = new ArrayList<>();
        
        for (Map.Entry<Long, Long> entry : histogram.entrySet()) {
            if (entry.getValue() >= freqThreshold) {
                trueFrequentItems.add(entry.getKey());
            }
        }
        trueFrequentItems.sort(Long::compareTo);

        System.out.println("Number of true frequent items = " + trueFrequentItems.size());
        System.out.println("True frequent items:");
        for (Long item : trueFrequentItems) {
            System.out.println(item);
        }

        char positive_sign = '+';
        char negative_sign = '-';

        // Print results of reservoir sampling
        System.out.println("RESERVOIR SAMPLING");
        System.out.println("Size m of the sample = " + m);
        Set<Long> distinctReservoir = new TreeSet<>(reservoir);
        System.out.println("Number of estimated frequent items = " + distinctReservoir.size());
        System.out.println("Estimated frequent items:");

        for (Long item : distinctReservoir) {
            Long value = histogram.getOrDefault(item, 0L);
            if(value >= freqThreshold)
                System.out.println(item + " " + positive_sign);
            else
                System.out.println(item + " " + negative_sign);
        }

        // Print results of sticky sampling
        System.out.println("STICKY SAMPLING");
        System.out.println("Number of items in the Hash Table = " + stickySampling.size());
        ArrayList<Long> stickyFrequentItems = new ArrayList<>();
        double stickyThreshold = (phi - epsilon) * n;
        for (Map.Entry<Long, Long> entry : stickySampling.entrySet()) {
            if (entry.getValue() >= stickyThreshold)
                stickyFrequentItems.add(entry.getKey());
        }
        stickyFrequentItems.sort(Long::compareTo);
        System.out.println("Number of estimated frequent items = " + stickyFrequentItems.size());
        System.out.println("Estimated frequent items:");
        for (Long item : stickyFrequentItems) {
            Long value = histogram.getOrDefault(item, 0L);
            if(value >= freqThreshold)
                System.out.println(item + " " + positive_sign);
            else
                System.out.println(item + " " + negative_sign);
        }
    }
}
