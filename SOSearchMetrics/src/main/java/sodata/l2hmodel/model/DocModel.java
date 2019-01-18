package sodata.l2hmodel.model;

import java.util.stream.IntStream;

import sodata.l2hmodel.entropy.Math;

public class DocModel {
    private int id;
    private double[] proportions;
    private double diversity;
    
    private long docInfoId;
    private int[] docInfoTagIds;
    private int numWords;
    
    // we can't compute this before knowing the collection the document it is in
    // and it takes different value in different document collection. it is better
    // in the document collection
    // private double divergence;

    public DocModel(int id, double[] proportions) {
        this.id = id;
        this.proportions = proportions;
        this.diversity = computeDiversity(proportions);
    }
    
    public double getDiversity() {
        return diversity;
    }
    
    public int getId() {
        return id;
    }
    
    public long getDocInfoId() {
        return docInfoId;
    }

    public void setDocInfoId(long docInfoId) {
        this.docInfoId = docInfoId;
    }

    public int[] getDocInfoTagIds() {
        return docInfoTagIds;
    }

    public void setDocInfoTagIds(int[] docInfoTagIds) {
        this.docInfoTagIds = docInfoTagIds;
    }

    public int getNumWords() {
        return numWords;
    }

    public void setNumWords(int numWords) {
        this.numWords = numWords;
    }

    public double proportionOf(int topicId) {
        return proportions[topicId];
    }
    
    public double computeDivergence(int numTopics, double[] meanProportions) {
        if (numTopics > proportions.length || 
                numTopics > meanProportions.length || proportions.length != meanProportions.length) {
            throw new IllegalArgumentException("Inconsistent arguemnts");
        }
        return computeDivergence(numTopics, proportions, meanProportions);
    }

    private double computeDiversity(double[] proportions) {
//        double entropy = 0.;
//        for (double p : proportions) {
//            if (p > 0. && !Double.isInfinite(p)) {
//                entropy -= p * Math.log2(p);
//            }
//        }
//        return entropy;
        
        // Note that \lim_x->0 x log(x) = 0.  When x = 0, we can't and won't need to do log_2.
        // It seems it is easier to express the computeMeanDiversity using streams, just to 
        // have uniformity, rewrite the above in streams as follows,
        double entropy = IntStream.range(0, proportions.length)
                .filter(topicId -> proportions[topicId] > 0 && Double.isFinite(proportions[topicId]))
                .mapToDouble(topicId -> - proportions[topicId] * Math.log2(proportions[topicId]))
                .sum();
        return entropy;
    }
    
    private double computeDiversity(int numTopics, double[] proportions) {
      // Note that \lim_x->0 x log(x) = 0.  When x = 0, we can't and won't need to do log_2.
      // It seems it is easier to express the computeMeanDiversity using streams, just to 
      // have uniformity, rewrite the above in streams as follows,
      double entropy = IntStream.range(0, numTopics)
              .filter(topicId -> proportions[topicId] > 0 && Double.isFinite(proportions[topicId]))
              .mapToDouble(topicId -> - proportions[topicId] * Math.log2(proportions[topicId]))
              .sum();
      return entropy;
  }

    private double computeMeanDiversity(int numTopics, double[] proportions, double[] meanProportions) {
        // Note that \lim_x->0 x log(x) = 0.  When x = 0, we can't and won't need to do log_2.
        double entropy = IntStream.range(0, numTopics)
                .filter(topicId -> meanProportions[topicId] > 0. && 
                        Double.isFinite(proportions[topicId]) && Double.isFinite(meanProportions[topicId]))
                .mapToDouble(topicId -> - proportions[topicId] * Math.log2(meanProportions[topicId]))
                .sum();
        return entropy;
    }
    
    private double computeDivergence(int numTopics, double[] proportions, double[] meanProportions) {
        double meanDiversity = computeMeanDiversity(numTopics, proportions, meanProportions);
        double diversity = computeDiversity(numTopics, proportions);
        return meanDiversity - diversity;
    }
}
