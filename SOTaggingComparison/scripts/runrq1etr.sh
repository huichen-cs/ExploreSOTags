#!/bin/bash

. ./env.sh

rootdir=./stackoverflow.U2_U2

if [ ! -f ${ENTAGREC_JAR} ]; then 
  echo "Cannot locate ${ENTAGREC_JAR}, exit ..." 
  exit 1
fi

if [ ! -d "${rootdir}/rawdata" ]; then 
  echo "Cannot locate EnTagRec data at ${rootdir}/rawdata." 
  echo "Download from EnTagRec and copy it to ${rootdir}/rawdata"
  echo "Exit ..."
  exit 1 
fi

if [ ! -f "${rootdir}/tag_doc_50.txt" ]; then
  echo "Missing tag_doc_50.txt in ${rootdir}"
  echo "Download it from EnTagRec and copy it to ${rootdir}/tag_doc_50.txt"
  exit 1
fi

if [ ! -f "${rootdir}/meta.txt" ]; then
  echo "Missing meta.txt in ${rootdir}"
  echo "Download it from EnTagRec and copy it to ${rootdir}/meta.txt"
  exit 1
fi

if [ ! -f "${rootdir}/posts.xml" ]; then
  echo "Missing posts.xml in ${rootdir}"
  echo "Download Posts.xml from the Stack Exchange data dump and copy it to ${rootdir}/posts.xml"
  exit 1
fi

step="cross_validation"

if [ $# -ge 1 ]; then
  step=$1
fi
echo "Doing step ${step}" 


case ${step} in
  process_raw_text)
    # Step 1. remove blank spaces, some punctuation, filter out stop words
    #         stackoverflow/rawdata -> stackoverflow/descriptionCleaned
    java -cp ${ENTAGREC_JAR} \
      org.preprocess.rawTextDataPreprocessor ${rootdir}/
    ;;
  filter_html_tags)
    # Step 2. remove HTML tags and symbols
    #         stackoverflow/descriptionCleaned -> stackoverflow/htmlFilterred
    java -cp ${ENTAGREC_JAR} org.preprocess.HtmlFilter ${rootdir}/
    ;;
  gen_dataset_for_llda)
    # Step 3. convert the data set in the LabeledLDA format
    #         stackoverflow/descriptionCleaned -> stackoverflow/dataset.csv
    java -cp ${ENTAGREC_JAR} LabeledLDA.generateDataset ${rootdir}/
    ;;
  post_tagger_via_maxenttagger)
    # Step 4. tag posts using maxenttagger so that each word is tagged, such 
    #         as "considering" becomes "considering_VBG" (considering is a
    #         verb gerund?) 
    #         stackoverflow/htmlFiltered -> stackoverflow/posdata
    java -cp ${ENTAGREC_JAR} TermTagIndex.POS ${rootdir}/
    ;;
  build_term_tag_index)
    # Step 5. build term tag index from tagged posts, i.e., 
    #         stackoverflow/posdata -> stackoverflow/posdata_preprocessed
    #                                  stackoverflow/term_tag_index.txt
    #         where in each post, only NN, NNP, NNS, NNPS terms are included
    #         (see https://cs.nyu.edu/grishman/jet/guide/PennPOS.html)
    #         NN: noun, single or plural
    #         NNP: proper noun, singular
    #         NNS: noun, plural 
    #         NNPS: proper noun, plural
    #         In stacioverflow/term_tag_index.txt
    #           term: tag1@count,tag2@count ...
    java -cp ${ENTAGREC_JAR} TermTagIndex.TermTagIndexBuilder ${rootdir}/
    ;;
  cross_validation)
    # Step 6. doing cross validation
    #   a. generate cross validation data
    #      stackoverflow/dataset.csv -> 
    #        stackoverflow/testcase_repeat#1/#2/train
    #        stackoverflow/testcase_repeat#1/#2/test
    #        stackoverflow/testcase_repeat#1/#2/golden
    #      where #1 is the repetition number, and #2 is the cross-validation
    #      number
    #   b. process meta.txt and posts.xml to get user tagging records
    #        stackoverflow/posts.xml -> stackoverflow/QuestionInformation.cvs
    #                                -> stackoverflow/userTagInformation.txt
    #   c. 
    java -Xmx6g -cp ${ENTAGREC_JAR}  \
      evaluate_EnTagRec_U2.RunKtimesForEffectSizeTest_U2 \
        ${rootdir}/ entagrec.properties
     ;;
esac






