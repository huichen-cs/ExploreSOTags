package sotags.dataconverter;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class HtmEnTagRecBridgeTest {

	/**
	 * Rigourous Test :-)
	 */
	@Test
	public void testDoc() {
	    final String docText = 
	            "3633,flex actionscript-3 javascript flash,swf load text into s rite resiz base " + 
	            "content ut into like on than age browser us nativ scroll bar rather than handl " + 
	            "actionscri t veri much like href htt www nike com nikeskateboard rel nofollow " + 
	            "htt www nike com nikeskateboard look stuff nike just wasn abl ull off ani idea"; 
	    Doc d = Doc.fromString(docText, true);

	    assertEquals(d.getId(), "3633");
	    assertEquals(d.getVocSize(), 42);
	    assertEquals(d.getWordFreq("like"), 2);
	    assertEquals(d.getWordFreq("nike"), 3);
	}
}
