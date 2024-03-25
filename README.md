TODO LIST:
- change the JavaRDD<Tuple> with a JavaPairRDD
- running time:
    ExactOutliers running time: 9 milliseconds
    MRApproxOutliers running time: 731 milliseconds
    This doesn't make sense because by deploying parallelism MRApproxOutlier should take less time (probably due to 
    the System.out.println())
- check the distance in the method calculateN7
- ask if it's okay to use double instead of float