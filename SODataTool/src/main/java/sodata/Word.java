package sodata;

public class Word {
    public String word;
    public long id;
    public long nDocuments;
    public long nOccurrences;

    public Word(String word, long nDocuments, long nOccurrences) {
        this.word = word;
        this.nDocuments = nDocuments;
        this.nOccurrences = nOccurrences;
        this.id = -1L;
    }

    public Word(long id, String word, long nDocuments, long nOccurrences) {
        this.id = id;
        this.word = word;
        this.nDocuments = nDocuments;
        this.nOccurrences = nOccurrences;
    }
}
