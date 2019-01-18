package sodata;

public class QuestionWord {
    public long questionId;
    public long wordId;
    public String word;
    public long nOccurrences;

    public QuestionWord(long questionId, String word, long nOccurrences) {
        this.questionId = questionId;
        this.word = word;
        this.nOccurrences = nOccurrences;
        this.wordId = -1;
    }

    public QuestionWord(long questionId, long wordId, long nOccurrences) {
        this.questionId = questionId;
        this.word = null;
        this.nOccurrences = nOccurrences;
        this.wordId = wordId;
    }

}
