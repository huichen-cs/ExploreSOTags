package com.base;

import org.junit.Assert;
import org.junit.Test;

public class TagSynonymsTest {
    @Test
    public void detectSynonyms() throws Exception {
        double[][] testCase = new double[][] {
                {0.3,0.4,0.2,0.1},
                {0.31,0.42,0.18,0.09},
                {0.1,0.1,0.7,0.1},
                {0.09,0.08,0.72,0.11},
                {0.5,0.2,0.1,0.2}
        };

        boolean[][] testCaseExpected = new boolean[][] {
                {true, true, false, false, false},
                {true, true, false, false, false},
                {false, false, true, true, false},
                {false, false, true, true, false},
                {false, false, false, false, false},
        };

        boolean[][] result = TagSynonyms.detectSynonyms(testCase, 0.1);
        for(int i = 0; i < result.length; i++) {
            for(int j = 0;  j < result.length; j++) {
                // Assert.assertEquals(testCaseExpected[i][j], result[i][j]);
            }
        }
    }

}
