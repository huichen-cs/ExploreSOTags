package sodata;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;

import org.junit.Test;



public class StringUtilsTest  {
    @Test
    public void testHead() {
        String s = "Rückruf ins Ausland";
        String h = StringUtils.head(s, 4, StandardCharsets.UTF_8);
        System.out.println(h);
        System.out.println(h.getBytes().length);
        assertEquals(h.getBytes().length, 4);
        assertTrue(s.contains(h));
        assertEquals(s.indexOf(h), 0);
        
        h = StringUtils.head(s, 2, StandardCharsets.UTF_8);
        System.out.println(h);
        System.out.println(h.getBytes().length);
        assertEquals(h.getBytes().length, 1);
        assertTrue(s.contains(h));
        assertEquals(s.indexOf(h), 0);
        
        h = StringUtils.head(s, 128, StandardCharsets.UTF_8);
        System.out.println(h);
        System.out.println(h.getBytes().length);
        assertEquals(h.getBytes().length, 20);
        assertTrue(s.contains(h));
        assertEquals(s.indexOf(h), 0);

    }
    
    @Test
    public void testHeadFastButImperfect() {
        String s = "Rückruf ins Ausland";
        String h = StringUtils.headFastButImperfect(s, 4, StandardCharsets.UTF_8);
        System.out.println(h);
        System.out.println(h.getBytes().length);
        assertEquals(h.getBytes().length, 4);
        assertTrue(s.contains(h));
        assertEquals(s.indexOf(h), 0);
        
        h = StringUtils.headFastButImperfect(s, 2, StandardCharsets.UTF_8);
        System.out.println(h);
        System.out.println(h.getBytes().length);
        assertEquals(h.getBytes().length, 4);
        assertFalse(s.contains(h));
        
        h = StringUtils.headFastButImperfect(s, 128, StandardCharsets.UTF_8);
        System.out.println(h);
        System.out.println(h.getBytes().length);
        assertEquals(h.getBytes().length, 20);
        assertTrue(s.contains(h));
        assertEquals(s.indexOf(h), 0);
    }
    
    @Test
    public void testHeadNiceButSlow() {
        String s = "Rückruf ins Ausland";
        String h = StringUtils.headNiceButSlow(s, 4, StandardCharsets.UTF_8);
        System.out.println(h);
        System.out.println(h.getBytes().length);
        assertEquals(h.getBytes().length, 4);
        assertTrue(s.contains(h));
        assertEquals(s.indexOf(h), 0);
        
        h = StringUtils.headNiceButSlow(s, 2, StandardCharsets.UTF_8);
        System.out.println(h);
        System.out.println(h.getBytes().length);
        assertEquals(h.getBytes().length, 1);
        assertTrue(s.contains(h));
        assertEquals(s.indexOf(h), 0);
        
        h = StringUtils.headNiceButSlow(s, 128, StandardCharsets.UTF_8);
        System.out.println(h);
        System.out.println(h.getBytes().length);
        assertEquals(h.getBytes().length, 20);
        assertTrue(s.contains(h));
        assertEquals(s.indexOf(h), 0);         
    }
    
    /*
    @Test
    public void testHeadFast() {
        String s = new String("Rückruf ins Ausland".getBytes(), StandardCharsets.UTF_8);
        String h = StringUtils.headFast(s, 4, StandardCharsets.UTF_8);
        System.out.println(h);
        System.out.println(h.getBytes().length);
        assertEquals(h.getBytes().length, 4);
        assertTrue(s.contains(h));
        assertEquals(s.indexOf(h), 0);
        
        h = StringUtils.headFast(s, 2, StandardCharsets.UTF_8);
        assertEquals(h.getBytes().length, 1);
        assertTrue(s.contains(h));
        assertEquals(s.indexOf(h), 0);
    }
    */
}