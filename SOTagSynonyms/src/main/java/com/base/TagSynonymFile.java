package com.base;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by batman on 8/10/17.
 */
public class TagSynonymFile {
    /*
    Tag synonym:
    - Can have a potentially infinite number of other synonyms
    - Commutative: If A is a synonym of B, then B is a synonym of A
    - Need quick/easy way to look up if some string X has any synonyms
     */

    private final Map<String, List<String>> synonyms;

    public TagSynonymFile(File file) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
        try {
            // Intentionally skip first line; it's just header information.
            String line = br.readLine();
            synonyms = new HashMap<>();

            while((line = br.readLine()) != null) {
                String[] values = line.split(",");
                for (int i = 0; i < values.length; i++) {
                    values[i] = values[i].trim();
                }

                addSynonym(
                        values[1].substring(1,values[1].length()-1),
                        values[2].substring(1,values[2].length()-1));
            }

        } finally {
            br.close();
        }
    }

    private void addSynonym(String word, String synonym) {
        addSynonymToASingleSynonymList(word, synonym);
        addSynonymToASingleSynonymList(synonym, word);
    }

    private void addSynonymToASingleSynonymList(String word, String synonym) {
        List<String> synonymList = synonyms.get(word);
        if(synonymList == null) {
            synonymList = new ArrayList<String>();
            synonyms.put(word, synonymList);
        }

        synonymList.add(synonym);
    }

    public boolean isSynonym(String word, String potentialSynonym) {
        if(word.equals(potentialSynonym)) {
            return true;
        }

        List<String> synonymList = synonyms.get(word);
        if(synonymList == null) {
            return false;
        }

        for(String synonym : synonymList) {
            if(synonym.equals(potentialSynonym)) {
                return true;
            }
        }

        return false;
    }

    public String findSynonym(String word, Map<String, Object> allowedSynonyms) {
        List<String> synonymList = synonyms.get(word);
        if(synonymList == null) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        for(String synonym : synonymList) {
			if(allowedSynonyms == null || allowedSynonyms.containsKey(synonym)) {
				result.append(":");
				result.append(synonym);
			}
        }
        return result.toString();
    }

    public static void findKnownSynonyms(File inputFile, File outputFile, TagSynonymFile tagSynonyms, Map<String, Object> allowedSynonyms) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), StandardCharsets.UTF_8));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8));
        try {
            String line;
            while((line = br.readLine()) != null) {
                String[] values = line.split("\\s+");
                bw.write(line + tagSynonyms.findSynonym(values[2], allowedSynonyms));
                bw.newLine();
            }
        } finally {
            br.close();
            bw.close();
        }
    }

    private static void annotateTagDistance(File inputFile, File outputFile, TagDistributions distributions) throws IOException {
        TagTree tagTree = new TagTree(inputFile);
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), StandardCharsets.UTF_8));
        BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
        try {
            String line;
            while((line = br.readLine()) != null) {
                String[] values = line.split("\\s+");
                String name = values[2];
                String parentName = tagTree.getParentName(name);

                if(parentName != null && !parentName.equals("root")) {
                    double distance = distributions.getJSDistance(name, parentName);
                    line = line + "=" + distance;
                }

                bw.write(line);
                bw.newLine();
            }
        } finally {
            br.close();
            bw.close();
        }
    }

	public static Map<String, Object> buildAllowedSynonyms(File inputFile) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), StandardCharsets.UTF_8));
        Map<String, Object> result = new HashMap<String, Object>();
        try {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split("\\s+");
                result.put(values[2], null);
            }
        } finally {
            br.close();
        }
        return result;
    }

    // TODO: Move this into a unit test!
    public static void main(String[] args) throws IOException {
        //TagSynonymFile tagSynonyms = new TagSynonymFile(new File("TagSynonyms_20170613.csv"));
        //System.out.println(tagSynonyms.isSynonym("windows-forms", "winforms"));
        //System.out.println(tagSynonyms.isSynonym("windows-forms", "c#"));
        TagDistributions tagDistributions = new TagDistributions(new File("so2016p5000w300_iter500_tag_topic_distr.txt"));
        annotateTagDistance(new File("so2016p5000w300_iter500_tree.txt"), new File("tree3_with_distance.txt"), tagDistributions);
        //Map<String, Object> allowedSynonyms = buildAllowedSynonyms(new File("tree.txt"));
        //findKnownSynonyms(new File("tree.txt"), new File("tree_synonyms2.txt"), tagSynonyms, allowedSynonyms);
//        Iterator<String> it = tagSynonyms.synonyms.keySet().iterator();
//        while(it.hasNext()) {
//            System.out.println(it.next());
//        }
    }
}
