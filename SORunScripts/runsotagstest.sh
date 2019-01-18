#!/bin/bash

#
# sodumpdb=> SELECT MIN(creationdate),MAX(creationdate) FROM questions;
#           min           |          max
# -------------------------+------------------------
# 2008-07-31 21:42:52.667 | 2016-03-13 21:56:12.66
# (1 row)
#

verbose=1

jarlibpath=../../jar

datafolder=so2016p500android500x1k5
dataset=so2016p500android500x1k5

dbprefix=wk16_ic_
startdatetime="2016-01-01 00:00:00"
enddatetime="2018-01-01 00:00:00"
dbmiddle=v500a500x1k5_
selectedtagsfile=./post_tags_withandroid.csv
dbpropertiesfile=sodumpdb.properties

function dataset_ready() {
  datafolder=$1
  dataset=$2
  if [ ! -f ${datafolder}/${dataset}.dat ]; then
    return 1
  fi
  [ ${verbose} == 1 ] && echo "Found ${datafolder}/${dataset}.dat"

  if [ ! -f ${datafolder}/${dataset}.docinfo ]; then
    return 1
  fi
  [ ${verbose} == 1 ] && echo "Found ${datafolder}/${dataset}.docinfo"

  if [ ! -f ${datafolder}/${dataset}.lvoc ]; then
    return 1
  fi
  [ ${verbose} == 1 ] && echo "Found ${datafolder}/${dataset}.lvoc"

  if [ ! -f ${datafolder}/${dataset}.wvoc ]; then
    return 1
  fi
  [ ${verbose} == 1 ] && echo "Found ${datafolder}/${dataset}.wvoc"

  return 0;
}

function model_ready() {
  datafolder=$1

  if [ -d ${datafolder}_out ]; then
    return 0
  else
    return 1
  fi
}

function testdata_ready() {
  datafolder=$1
  dataset=$2

  testdatafn=${datafolder}/${dataset}_test.data
  if [ ! -f ${testdatafn} ]; then
    return 1
  else
    return 0
  fi
}

function docmodel_ready() {
  datafolder=$1
  dataset=$2

  mst_tree=${data_folder}_out/mst/tree.txt
  if [ ! -f ${mst_tree} ]; then
    return 1;
  fi

  doc_model=`ls ${data_folder}_out/preset*/iter-predictions/iter-500.txt`
  if [ $? -ne 0 ]; then
    return 1;
  fi

  global_tree=`ls ${data_folder}_out/preset*/report/topwords-500.txt`
  if [ $? -ne 0 ]; then
    return 1;
  fi
  
  return 0;
}

function specificity_ready() {
  datafolder=$1
  output_csv_fn=${data_folder}_out/global_specificity.csv

  if [ ! -f ${output_csv_fn} ]; then
    return 0
  else 
    return 1
  fi
}

function xdivergence_ready() {
  datafolder=$1
  output_csv_fn=${data_folder}_out/global_xdivergence.csv

  if [ -f ${output_csv_fn} ]; then
    return 0
  else 
    return 1
  fi
}


function make_dataset_from_db_from_scratch() {
  dbprefix=$1
  startdatetime=$2
  enddatetime=$3
  dbmiddle=$4
  selectedtagsfile=$5
  dbpropertiesfile=$6
  datafolder=$7
  dataset=$8

  java -Xmx6g -cp "${jarlibpath}/SODataTool.jar" \
          sodata.processor.QuestionTextProcessor \
          ${dbprefix} \
          "${startdatetime}" "${enddatetime}" \
          --usecopy --ignorecase
          

  java -Xmx6g -cp "${jarlibpath}/SODataTool.jar" \
          sodata.processor.QuestionTagProcessor \
          ${dbprefix}

  java -cp "${jarlibpath}/SODataTool.jar" \
          sodata.processor.dataselector.SelectorOnTagList \
          ${dbprefix} \
          ${dbmiddle} \
          ${selectedtagsfile} \
          ${dbpropertiesfile} \
          250 \
          android \
          250 \
          1500

  java -Xmx4g -cp "${jarlibpath}/SODataTool.jar" \
          sodata.processor.segan.L2hDataMakerTagListDataset \
          ${datafolder} ${dataset} \
          ${dbprefix}${dbmiddle} \
          ${dbpropertiesfile} \
          _f 200 0.5 0 1 --relative
}

function make_model() {
  datafolder=$1
  dataset=$2

  [ -d ${datafolder}_out ] && rm -rf ${datafolder}_out

  java -Xmx6g \
	-Xms6g \
	-cp "${jarlibpath}/segan.jar" \
	sampler.labeled.hierarchy.L2H \
	-v \
	--dataset ${dataset} \
	--output ${datafolder}_out \
	--format-file ${dataset} \
	--numTopwords 20 \
	--min-label-freq 100 \
	--burnIn 250 \
	--maxIter 500 \
	--sampleLag 25 \
	--report 1 \
	--alpha 10 \
	--beta 1000 \
	--a0 90 \
	--b0 10 \
	--format-folder ${datafolder} \
	--path max 

	#--tree\
	#--exact\
	#-d
	#-XX:-UseGCOverheadLimit 

}

function make_doc_model() {
  datafolder=$1
  dataset=$2

  java -cp ${jarlibpath}/SOL2hDocModel.jar \
          sodata.l2hmodel.docmodel.L2hDocModelApp \
          -v \
          --dataset ${dataset}\
          --output ${datafolder}_out \
          --format-file ${dataset} \
          --format-folder ${datafolder} \
          --data-folder ${datafolder} \
          --numTopwords 20 --min-label-freq 100 \
          --burnIn 250  --maxIter 500 --sampleLag 25 \
          --report 1 --alpha 10 --beta 1000 --a0 90 --b0 10 \
          -d
}


function make_doc_tree() {
  datafolder=$1
  dataset=$2
  num_random_docs=$3

  mst_tree=${data_folder}_out/mst/tree.txt
  doc_model=`ls ${data_folder}_out/preset*/iter-predictions/iter-500.txt`
  global_tree=`ls ${data_folder}_out/preset*/report/topwords-500.txt`

  doc_tree_folder=${data_folder}_out/doctree

  if [ ! -d ${doc_tree_folder} ]; then
    mkdir ${doc_tree_folder}
    if [ $? -ne 0 ]; then
      echo "Cannot create folder to store the document trees"
      return 1
    fi
  fi

  java -cp ${jarlibpath}/SOSearchMetrics.jar \
        sodata.l2hmodel.doctree.DocTreeApp \
        ${mst_tree} \
        ${doc_mode} \
        ${global_tree} \
        ${num_random_docs} \
        ${doc_tree_folder}
}

function compute_specificity() {
  datafolder=$1
  dataset=$2
  output_csv_fn=${data_folder}_out/global_specificity.csv

  mst_tree=${data_folder}_out/mst/tree.txt
  doc_model=`ls ${data_folder}_out/preset*/iter-predictions/iter-500.txt`
  global_tree=`ls ${data_folder}_out/preset*/report/topwords-500.txt`

  java -cp ${jarlibpath}/SOSearchMetrics.jar \
        sodata.l2hmodel.entropy.GlobalTreeSpecificityApp  \
        ${mst_tree} \
        ${doc_mode} \
        ${global_tree} \
        ${output_csv_fn}
}

function compute_xdivergence() {
  datafolder=$1
  dataset=$2
  output_csv_fn=${data_folder}_out/global_xdivergence.csv

  mst_tree=${data_folder}_out/mst/tree.txt
  doc_model=`ls ${data_folder}_out/preset*/iter-predictions/iter-500.txt`
  global_tree=`ls ${data_folder}_out/preset*/report/topwords-500.txt`

  java -cp ${jarlibpath}/SOSearchMetrics.jar \
        sodata.l2hmodel.entropy.GlobalTreeCrossDivergenceApp \
        ${mst_tree} \
        ${doc_mode} \
        ${global_tree} \
        ${output_csv_fn}
}
        

dataset_ready ${datafolder} ${dataset}

if [[ $? != 0  ||  -f force_remake_dataset ]]; then 
  make_dataset_from_db_from_scratch \
    ${dbprefix} \
    ${startdatetime} \
    ${enddatetime} \
    ${dbmiddle} \
    ${selectedtagsfile} \
    ${dbpropertiesfile} \
    ${datafolder} \
    ${dataset} 
else
  echo "Dataset ${dataset} has already been genereated at ${datafolder}"
fi

[ -f force_remake_dataset ] && rm -f force_remake_dataset


model_ready ${datafolder}

if [[ $? != 0  ||  -f force_remake_model ]]; then 
  make_model ${datafolder} ${dataset}
else
  echo "Model for dataset ${dataset} has already been genereated at " \
    "${datafolder}_out"
fi

[ -f force_remake_model ] && rm -f force_remake_model

testdata_ready ${datafolder} ${dataset}

if [ $? != 0 ]; then
  echo "Test data isn't ready. Exit."
  return 1
else
  echo "Found test data"
fi

make_doc_model ${datafolder} ${dataset}

doc_mode_ready ${datafolder} ${dataset}

if [ $? != 0 ]; then
  echo "Doc model isn't ready. Exit."
  return 1
else
  echo "Found doc model"
fi

# randomly select and generate document trees
make_doc_tree ${datafolder} ${dataset} 10

specificity_ready ${datafolder}

if [ $? -eq 0 ]; then
  echo "Found specificity. To recompute, remove the file and rerun the job."
  return 1
fi

compute_specificity ${datafolder} ${dataset}

xdivergence_ready ${datafolder}

if [ $? -eq 0 ]; then
  echo "Found xdivergence. To recompute, remove the file and rerun the job."
  return 1
fi

compute_xdivergence ${datafolder} ${dataset}

exit $?



