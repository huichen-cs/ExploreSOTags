package com.base;

import cc.mallet.util.Maths;

public class TagSynonyms {
    public static boolean[][] detectSynonyms(double[][] tagDistributions, double maxThreshold) {
        return detectSynonymsFromJSDistance(jensonShannonDistances(tagDistributions), maxThreshold);
    }

    public static boolean[][] detectSynonymsFromJSDistance(double[][] jsDistances, double maxThreshold) {
        boolean[][] result = new boolean[jsDistances.length][jsDistances[0].length];
        for(int i = 0; i < jsDistances.length; i++) {
            for(int j = 0; j < jsDistances[0].length; j++) {
                result[i][j] = jsDistances[i][j] < maxThreshold;
            }
        }
        return result;
    }

    public static double[][] jensonShannonDistances(double[][] tagDistributions) {
        /*
         *  NOTE: The time complexity of this algorithm is of O(L^2 * N) where L is the number of tags and
         *  N is the number of words in the vocabulary. 
         */
        double[][] result = new double[tagDistributions.length][tagDistributions.length];

        for(int i = 0; i < tagDistributions.length; i++) {
            for(int j = i; j < tagDistributions.length; j++) {
                result[i][j] = Math.sqrt(Maths.jensenShannonDivergence(tagDistributions[i], tagDistributions[j]));
                result[j][i] = result[i][j];
            }
        }

        return result;
    }
}
