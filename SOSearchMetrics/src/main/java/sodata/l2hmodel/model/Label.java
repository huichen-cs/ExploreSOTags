package sodata.l2hmodel.model;

public class Label {
    private int id;
    private String text;
    private String mstPath;
    
    public Label(int id, String text, String mstPath) {
        this.id = id;
        this.text = text;
        this.mstPath = mstPath;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getMstPath() {
        return mstPath;
    }

    public void setMstPath(String mstPath) {
        this.mstPath = mstPath;
    }
}
