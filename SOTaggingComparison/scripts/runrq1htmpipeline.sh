#!/bin/bash

steps=(make_htm_data clean_htm_model infer_htm_model infer_doc_model_for_train_data mv_doc_model_for_train_data infer_doc_model_for_test_data mv_doc_model_for_test_data convert_l2h_to_entagrec cross_validation)
for step in ${steps[*]}; do
  ./runrq1htm.sh ${step}
done

