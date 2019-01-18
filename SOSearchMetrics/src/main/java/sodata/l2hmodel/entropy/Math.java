package sodata.l2hmodel.entropy;

public class Math {
    public static double log2(double x, double base) {
        return java.lang.Math.log(x) / java.lang.Math.log(base);
     }
    
    public static double log2(double x) {
        return log2(x, 2.);
    }
}
