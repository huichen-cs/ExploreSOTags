package sodata.nlpprocessor;

import java.io.IOException;

public class TestFinally  {
    public static void main(String[] args) throws IOException {
        TestFinally test = new TestFinally();
        test.dosth();
    }
    
    public void dosth() throws IOException {
        try {
            throw new IOException ("IOException raise");
        } finally {
            System.out.println("1. Caught finally--------------");
            System.out.println("2. Caught finally--------------");
        }
    }
}
