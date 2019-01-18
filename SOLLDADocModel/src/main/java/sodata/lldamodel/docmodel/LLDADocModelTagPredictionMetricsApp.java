package sodata.lldamodel.docmodel;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LLDADocModelTagPredictionMetricsApp {
    private final static Logger LOGGER = LoggerFactory.getLogger(LLDADocModelTagPredictionMetricsApp.class);

    public static void main(String[] args) throws IOException {
        if (args.length < 5) {
            System.out.println("Usage: LLDADocModelTagPredictionMetricsApp "
                    + "<target_label_vocabulary_file> <target_doc_label_file> <filtered_label_vocabulary_file>"
                    + "<doc_label_weight_list_file> <top_n_starting_from_0> [<target_doc_data_file> <max_doc_len_in_words>]");
            return;
        }

        LabelCollection targetLabelCollection = LabelCollection.fromLabelVocFile(args[0]);
        DocLabel[] targetDocLabels = DocLabel.fromDocInfoFile(args[1]);
        LabelCollection filteredLabelCollection = LabelCollection.fromLabelVocFile(args[2]);
        DocLabel[] filteredDocLabels = DocLabel.updateDocLabel(targetDocLabels, targetLabelCollection,
                filteredLabelCollection);
        DocTopic[] docTopics = DocTopic.fromIterReport(args[3]);
        int topN = Integer.parseInt(args[4]);
        LOGGER.info("Read all data.");

        int docLengthUpThreshold = Integer.MAX_VALUE;
        int docLengthLoThreshold = Integer.MIN_VALUE;
        if (args.length >= 8) {
            DocCollection docCollection = DocCollection.fromDocDataFile(args[5]);
            if (docCollection.numOfDocs() != docTopics.length) {
                throw new IllegalStateException("Inconsistent data files.");
            }
            DocCollection.updateDocTopics(docTopics, docCollection);
            docLengthLoThreshold = Integer.parseInt(args[6]);
            docLengthUpThreshold = Integer.parseInt(args[7]);
        }

        if (filteredDocLabels.length != docTopics.length) {
            throw new IllegalArgumentException(
                    "The doc label file and the doc topic file do not belong to the same set.");
        }

        int nDocs = 0, nDocsMatch = 0;
        int nTagsTotal = 0, nTagsMatch = 0;

        for (int i = 0; i < docTopics.length; i++) {
            if (docTopics[i].getDocLengthInWords() > docLengthLoThreshold
                    && docTopics[i].getDocLengthInWords() <= docLengthUpThreshold) {
                nDocs++;
                boolean found = false;
                int topNPerDoc = Math.max(topN, filteredDocLabels[i].getNumberOfTags());
                System.out.println("topNPerDoc = " + topNPerDoc);
                for (int j = 0; j < filteredDocLabels[i].getNumberOfTags(); j++) {
                    System.out.println("docTopics[" + i + "][" + j + "] = "
                            + docTopics[i].getRank(filteredDocLabels[i].tagIdAt(j)));
                    // if (docTopics[i].getRank(filteredDocLabels[i].tagIdAt(j)) < topN) {
                    // nTagsMatch ++;
                    // found = true;
                    // }
                    if (docTopics[i].getRank(filteredDocLabels[i].tagIdAt(j)) < topNPerDoc) {
                        nTagsMatch++;
                        found = true;
                    }
                }
                nTagsTotal += filteredDocLabels[i].getNumberOfTags();
                if (found)
                    nDocsMatch++;
            }
        }
        System.out.println(topN + "," + nDocs + "," + nDocsMatch + "," + (double) nDocsMatch / (double) nDocs + ","
                + nTagsTotal + "," + nTagsMatch + "," + (double) nTagsMatch / (double) nTagsTotal + ","
                + docLengthLoThreshold + "," + docLengthUpThreshold);
        // System.out.println(docTopics[0].toSortedString());
    }

}
