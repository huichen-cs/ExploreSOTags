package sodata.l2hmodel.model;

import sodata.l2hmodel.tree.L2hTree;

public class DocModelTree {
    private L2hTree tree;
    private DocModel docModel;
    
    public DocModelTree(DocModel docModel, L2hTree tree) {
        this.docModel = docModel;
        this.tree = tree;
    }

    public L2hTree getTree() {
        return tree;
    }

    public DocModel getDocModel() {
        return docModel;
    }
}
