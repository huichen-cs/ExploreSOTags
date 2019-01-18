package sodata;

public class WordFrequency {
    public long wordId;
    public long frequency;
    
    public WordFrequency(long wordId, long frequency) {
        this.wordId = wordId;
        this.frequency = frequency;
    }
    
    public WordFrequency(WordFrequency wf) {
        this.wordId = wf.wordId;
        this.frequency = wf.frequency;
    }

    public long getWordId() {
        return wordId;
    }

    public long getFrequency() {
        return frequency;
    }

    public void setFrequency(long frequency) {
        this.frequency = frequency;
    }
}