TODO LIST:
- change the JavaRDD<Tuple> with a JavaPairRDD
- ask professor if it's okay to use a non-efficient method to compute the exact number of non-outliers
- running time:
    ExactOutliers running time: 9 milliseconds
    MRApproxOutliers running time: 731 milliseconds
    This doesn't make sense because by deploying parallelism MRApproxOutlier should take less time (probably due to 
    the System.out.println())
- ask professor if the time to sort the elements should be counted when calculating the computation time
- understand if we need to cache cellRDD
- check the distance in the method calculateN7