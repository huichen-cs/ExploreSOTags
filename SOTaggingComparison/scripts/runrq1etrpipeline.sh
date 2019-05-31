#!/bin/bash

steps=(process_raw_text filter_html_tags gen_dataset_for_llda post_tagger_via_maxenttagger build_term_tag_index cross_validation)
for step in ${steps[*]}; do
  ./runrq1etr.sh ${step}
done


