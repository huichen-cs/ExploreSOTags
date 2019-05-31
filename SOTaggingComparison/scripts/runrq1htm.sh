#!/bin/bash

. ./env.sh
. ./functions.sh

rootdir=./stackoverflow.U2_UH
numrepetitions=1
numfolds=10

step="infer_htm_model"
if [ $# -ge 1 ]; then
	step=$1
fi
echo "Doing ${step} ..."

case ${step} in
  make_htm_data)
    has_entagrec_xv_data ${rootdir} ${numrepetitions} ${numfolds}
    [ $? -ne 0 ] && exit 1
    java -cp ${DATACONVERTER_JAR} sotags.dataconverter.HtmEnTagRecBridge \
      ${rootdir} ${numfolds} ${numrepetitions} 
    ;;
  clean_htm_model)
    clean_htm_l2h_output ${rootdir} ${numrepetitions} ${numfolds}
    ;;
  infer_htm_model)
    has_htm_l2h_train_data ${rootdir} ${numrepetitions} ${numfolds}
    [ $? -ne 0 ] && exit 1
    infer_htm_model ${rootdir} ${numrepetitions} ${numfolds} ${L2H_JAR}
    ;;
  infer_doc_model_for_train_data)
    has_htm_l2h_output ${rootdir} ${numrepetitions} ${numfolds}
    [ $? -ne 0 ] && exit 1
    infer_htm_train_doc_model \
	    ${rootdir} ${numrepetitions} ${numfolds} ${L2H_JAR}
    [ $? -eq 0 ] && echo "Completed"
    ;;
  mv_doc_model_for_train_data)
    has_htm_l2h_output ${rootdir} ${numrepetitions} ${numfolds}
    [ $? -ne 0 ] && exit 1
    mv_htm_l2h_doc_model_output \
	    ${rootdir} ${numrepetitions} ${numfolds} train
    [ $? -eq 0 ] && echo "Completed"
    ;;
  infer_doc_model_for_test_data)
    has_htm_l2h_output ${rootdir} ${numrepetitions} ${numfolds}
    [ $? -ne 0 ] && exit 1
    infer_htm_test_doc_model \
	    ${rootdir} ${numrepetitions} ${numfolds} ${L2H_JAR}
    [ $? -eq 0 ] && echo "Completed"
    ;;
  mv_doc_model_for_test_data)
    has_htm_l2h_output ${rootdir} ${numrepetitions} ${numfolds}
    [ $? -ne 0 ] && exit 1
    mv_htm_l2h_doc_model_output \
	    ${rootdir} ${numrepetitions} ${numfolds} test
    [ $? -eq 0 ] && echo "Completed"
  ;;
  convert_l2h_to_entagrec)
    java -cp ${MODELCONVERTER_JAR} \
      sotags.modelconverter.L2hToEntagrecModelConverter ./stackoverflow.U2_UH \
        ./entagrec.properties ./l2hparas.sh
    [ $? -eq 0 ] && echo "Completed"
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
    java -Xmx9g -cp ${ENTAGREC_JAR}  \
      evaluate_EnTagRec_U2.RunKtimesForEffectSizeTest_UH \
        ${rootdir}/ entagrec.properties
     ;;

esac

