package sotags.dataconverter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class Doc {
    private String id;
    private List<String> tagList;
    private Map<String, Integer> wordMap;

    public Doc(String id, String[] tags, String[] words) {
        this.id = id;

        checkingOnTags(tags);
        this.tagList = Arrays.asList(tags);

        wordMap = buildWordMap(words);
    }
    
    public Doc(String id, String[] words) {
        this.id = id;

        tagList = null;

        wordMap = buildWordMap(words);
    }
    

    public String getId() {
        return id;
    }
    
    public void forEachWord(BiConsumer<String, Integer> action) {
        wordMap.forEach(action);
    }
    
    public void forEachTag(Consumer<String> action) {
        tagList.forEach(action);
    }

    public static Doc fromString(String line, boolean hasTag) {
        String[] parts = line.split(",");
        if (hasTag) {
            if (parts.length != 3) {
                throw new IllegalArgumentException("Format <docid,[tag1 tag2 ...], [word1 word2 ...] expected");
            }
            return new Doc(parts[0], parts[1].split(" "), parts[2].split(" "));
        } else {
            if (parts.length != 2) {
                throw new IllegalArgumentException("Format <docid, [word1 word2 ...] expected");
            }
            return new Doc(parts[0], parts[1].split(" "));
        }
    }

    private void checkingOnTags(String[] tags) {
        HashMap<String, Integer> tagMap = new HashMap<>();
        
        for (String tag: tags) {
            if (tagMap.containsKey(tag)) {
                tagMap.put(tag, tagMap.get(tag) + 1);
            } else {
                tagMap.put(tag, 1);
            }
        }
        
        for (String tag: tagMap.keySet()) {
            if (tagMap.get(tag) > 1) {
                throw new IllegalStateException("Tag " + tag + " isn't unique in doc " + id);
            }
        }
    }
    
    private Map<String, Integer> buildWordMap(String[] words) {
        Map<String, Integer> wordMap = new HashMap<>();
        
        for (String word: words) {
            if (wordMap.containsKey(word)) {
                wordMap.put(word, wordMap.get(word) + 1);
            } else {
                wordMap.put(word, 1);
            }
        }

        return wordMap;
    }


    public Iterator<String> getTagIterator() {
        if (null == tagList) return null;
        else return tagList.iterator();
    }

    public Iterator<String> getWordIterator() {
        if (null == wordMap || 0 == wordMap.size()) { 
            throw new IllegalStateException("Document must have content");
        }
        return wordMap.keySet().iterator();
    }

    public int getVocSize() {
        return wordMap.size();
    }
    
    public int getWordFreq(String word) {
        if (!wordMap.containsKey(word)) {
            throw new IllegalStateException("Expect word " + word + " to exist in the word map");
        }
        return wordMap.get(word);
    }
}
