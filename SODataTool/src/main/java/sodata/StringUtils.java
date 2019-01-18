package sodata;

import java.nio.charset.Charset;

public class StringUtils {
    public static String head(String value, int numBytes, Charset charset) {
        return headNiceButSlow(value, numBytes, charset);
    }
    
    public static String headFastButImperfect(String value, int numBytes, Charset charset) {
        /* this may be fast, last byte or character can be wrong */
        byte[] valueBytes = value.getBytes(charset);
        byte[] headBytes = new byte[numBytes>valueBytes.length?valueBytes.length:numBytes];
        System.arraycopy(valueBytes, 0, headBytes, 0, numBytes>valueBytes.length?valueBytes.length:numBytes);
        String head = new String(headBytes, charset);
        return head;
    }
    
    public static String headNiceButSlow(String value, int numBytes, Charset charset) {
        if (null != value) {
            while (value.getBytes(charset).length > numBytes) {
                value = value.substring(0, value.length() - 1);
            }
        }
        return value;
    }

}
