package sodata.processor.dataselector;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TagSynonyms {
    private static Logger logger = LoggerFactory.getLogger(TagSynonyms.class);
    
    private HashMap<String, Integer> tags;
    
    public TagSynonyms(String[] tags) {
        this.tags = new HashMap<String, Integer>();
        for(String aTag: tags) {
            this.tags.put(aTag,  0);
        }
    }
    
    public boolean setValue(String tagName, int count) {
        if (this.tags.get(tagName) != null) {
            this.tags.put(tagName,  count);
            return true;
        } else {
            logger.warn("Tag " + tagName + " is not found in the TagSynonyms object.");
            return false;
        }
    }

    public int getValue(String tagName) {
        return this.tags.get(tagName);
    }
    
    public HashMap<String, Integer> getTagPostCountMap() {
        return tags;
    }
}


