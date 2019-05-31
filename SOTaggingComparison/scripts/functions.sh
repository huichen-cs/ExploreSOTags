#!/bin/bash 

function has_entagrec_lib() {
  if [ ! -f ${ENTAGREC_JAR} ]; then 
    echo "Cannot locate ${ENTAGREC_JAR}, exit ..." 
    return 1
  fi
  return 0
}

function has_entagrec_data() {

  if [ ! -d "stackoverflow/rawdata" ]; then 
    echo "Cannot locate EnTagRec data at stackoverflow/rawdata." 
    echo "Download from EnTagRec and copy it to stackoverflow/rawdata"
    return 1
  fi

  if [ ! -f "stackoverflow/tag_doc_50.txt" ]; then
    echo "Missing tag_doc_50.txt in stackoverflow"
    echo "Download it from EnTagRec and copy it to stackoverflow/tag_doc_50.txt"
    return 1
  fi

  if [ ! -f "stackoverflow/meta.txt" ]; then
    echo "Missing meta.txt in stackoverflow"
    echo "Download it from EnTagRec and copy it to stackoverflow/meta.txt"
    return 1
  fi

  if [ ! -f "stackoverflow/posts.xml" ]; then
    echo "Missing posts.xml in stackoverflow"
    echo "Download Posts.xml from the Stac kExchange data dump and copy it to stackoverflow/posts.xml"
    return 1
  fi
  return 0
}

function has_entagrec_xv_data() {
  if [ $# -lt 3 ]; then
    echo "function has_entagrec_xv_data requires 3 parameters"
    return 1
  fi
  _rootdir=$1
  _numrepetition=$2
  _numfolds=$3
  _r=0
  while [ ${_r} -lt ${_numrepetition} ]; do
    _f=0
    while [ ${_f} -lt ${_numfolds} ]; do 
      _folddir=${_rootdir}/testcase_repeat${_r}/${_f}
      _foldtrain=${_folddir}/trainDataset_distr.csv
      if [ ! -f ${_foldtrain} ]; then
        echo "No data file ${_foldtrain} for fold ${_f} of repetition ${_r}"
        echo "Run entagrec first to generate the data"
        return 1
      fi
      let _f=${_f}+1
    done
    let _r=${_r}+1
  done
  return 0
}

function has_htm_l2h_train_data() {
  if [ $# -lt 3 ]; then
    echo "function has_htm_l2h_train_data requires 3 parameters"
    return 1
  fi
  _rootdir=$1
  _numrepetition=$2
  _numfolds=$3
  _r=0
  while [ ${_r} -lt ${_numrepetition} ]; do
    _f=0
    while [ ${_f} -lt ${_numfolds} ]; do 
      _folddir=${_rootdir}/testcase_repeat${_r}/${_f}/l2h
      _datafiles=(l2h.dat  l2h.docinfo  l2h.lvoc  l2h.wvoc)
      for _df in ${_datafiles[*]}; do
        _file=${_folddir}/${_df}
        if [ ! -f ${_file} ]; then
          echo "No L2H data file ${_file} for fold ${_f} of repetition ${_r}"
          echo "Run HtmEnTagRecBridge first to generate the data"
          return 1
        fi
      done
      let _f=${_f}+1
    done
    let _r=${_r}+1
  done
  return 0
}

function has_l2h_para_set() {
  if [ x"${l2h_alpha}" == x"" ]; then
    echo "L2h model parameters not set"
    return 1
  fi
  return 0
}

function run_l2h_model() {
  if [ $# -lt 4 ]; then
    echo "function run_l2h_model requires 4 parameters"
    return 1
  fi
  _l2hlibpath=$1
  _l2hdataset=$2
  _l2hdatafolder=$3
  _parafp=$4
  . ${_parafp}/l2hparas.sh
  has_l2h_para_set
  [ $? -ne 0 ] && return 1
  echo "${_l2hlibpath}/segan.jar"
  echo \
  "java \
  -Xmx9g \
  -cp ${_l2hlibpath}/segan.jar \
  sampler.labeled.hierarchy.L2H \
  -v \
  --dataset ${_l2hdataset} \
  --output ${_l2hdatafolder}_out \
  --format-file ${_l2hdataset} \
  --numTopwords ${l2h_ntopwords} \
  --min-label-freq ${l2h_minlabelfreq} \
  --burnIn ${l2h_burnin} \
  --maxIter ${l2h_maxiter} \
  --sampleLag ${l2h_samplelag} \
  --report ${l2h_report} \
  --alpha ${l2h_alpha} \
  --beta ${l2h_beta} \
  --a0 ${l2h_a0} \
  --b0 ${l2h_b0} \
  --format-folder ${_l2hdatafolder} \
  --path max 
  "
  # -Xmx7680m \
  java \
  -Xmx9g \
  -cp ${_l2hlibpath}/segan.jar \
  sampler.labeled.hierarchy.L2H \
  -v \
  --dataset ${_l2hdataset} \
  --output ${_l2hdatafolder}_out \
  --format-file ${_l2hdataset} \
  --numTopwords ${l2h_ntopwords} \
  --min-label-freq ${l2h_minlabelfreq} \
  --burnIn ${l2h_burnin} \
  --maxIter ${l2h_maxiter} \
  --sampleLag ${l2h_samplelag} \
  --report ${l2h_report} \
  --alpha ${l2h_alpha} \
  --beta ${l2h_beta} \
  --a0 ${l2h_a0} \
  --b0 ${l2h_b0} \
  --format-folder ${_l2hdatafolder} \
  --path max 
  [ $? -ne 0 ] && exit 1
}

function clean_htm_l2h_output() {
  if [ $# -lt 3 ]; then
    echo "function clean_htm_l2h_output requires 3 parameters"
    return 1
  fi
  _rootdir=$1
  _numrepetition=$2
  _numfolds=$3
  _r=0
  while [ ${_r} -lt ${_numrepetition} ]; do
    _f=0
    while [ ${_f} -lt ${_numfolds} ]; do 
      _folddir=${_rootdir}/testcase_repeat${_r}/${_f}/l2h_out
      rm -rf ${_folddir}
      let _f=${_f}+1
    done
    let _r=${_r}+1
  done
  return 0
}


function infer_htm_model() {
  if [ $# -lt 4 ]; then
    echo "function infer_htm_model requires 4 parameters"
    return 1
  fi
  _rootdir=$1
  _numrepetition=$2
  _numfolds=$3
  _l2hlibpath=$4
  _r=0
  while [ ${_r} -lt ${_numrepetition} ]; do
    _f=0
    while [ ${_f} -lt ${_numfolds} ]; do 
      _folddir=${_rootdir}/testcase_repeat${_r}/${_f}/
      _cwd=`pwd`
      _l2hparafilepath=${_cwd}
      cd ${_folddir}
      run_l2h_model ${_l2hlibpath} l2h l2h ${_l2hparafilepath}
      cd ${_cwd}
      let _f=${_f}+1
    done
    let _r=${_r}+1
  done
  return 0
}

function has_htm_l2h_output() {
  if [ $# -lt 3 ]; then
    echo "function has_htm_l2h_output requires 3 parameters"
    return 1
  fi
  _rootdir=$1
  _numrepetition=$2
  _numfolds=$3
  _r=0
  while [ ${_r} -lt ${_numrepetition} ]; do
    _f=0
    while [ ${_f} -lt ${_numfolds} ]; do 
      _folddir=${_rootdir}/testcase_repeat${_r}/${_f}/l2h_out
      _outdirs=(mst PRESET_L2H* RANDOM_*)
      for _od in ${_outdirs[*]}; do
        _dir=${_folddir}/${_od}
        if [ ! -d ${_dir} ]; then
          echo "not found ${_dir}"
          return 1
        fi
      done
      let _f=${_f}+1
    done
    let _r=${_r}+1
  done
  return 0
}

function run_l2h_doc_model() {
  if [ $# -lt 4 ]; then
    echo "function run_l2h_doc_model requires 4 parameters"
    return 1
  fi
  _l2hlibpath=$1
  _l2hdataset=$2
  _l2hdatafolder=$3
  _parafp=$4
  echo "_parafp = ${_parafp}"
  echo "----${_parafp}/l2hparas.sh"

  . ${_parafp}/l2hparas.sh
  has_l2h_para_set
  [ $? -ne 0 ] && return 1

  if [ ! -f ${_l2hlibpath}/segan.jar ]; then
    echo "Cannot locate ${_l2hlibpath}/SOL2hDocModel.jar."
    return 1
  fi

  #  -Xmx7680m \

  java \
  -Xmx9g \
  -cp ${_l2hlibpath}/SOL2hDocModel.jar \
        sodata.l2hmodel.docmodel.L2hDocModelApp \
        -v \
  --dataset ${_l2hdataset} \
  --output ${_l2hdatafolder}_out \
  --format-file ${_l2hdataset} \
  --format-folder ${_l2hdatafolder} \
        --data-folder ${_l2hdatafolder} \
  --numTopwords ${l2h_ntopwords} \
  --min-label-freq ${l2h_minlabelfreq} \
  --burnIn ${l2h_burnin} \
  --maxIter ${l2h_maxiter} \
  --sampleLag ${l2h_samplelag} \
  --report ${l2h_report} \
  --alpha ${l2h_alpha} \
  --beta ${l2h_beta} \
  --a0 ${l2h_a0} \
  --b0 ${l2h_b0} \
        -d

  [ $? -ne 0 ] && exit 1
}


function infer_htm_train_doc_model() {
  if [ $# -lt 4 ]; then
    echo "function infer_htm_train_doc_model requires 4 parameters"
    return 1
  fi
  _rootdir=$1
  _numrepetition=$2
  _numfolds=$3
  _l2hlibpath=$4
  _r=0
  while [ ${_r} -lt ${_numrepetition} ]; do
    _f=0
    while [ ${_f} -lt ${_numfolds} ]; do 
      _folddir=${_rootdir}/testcase_repeat${_r}/${_f}/
      _cwd=`pwd`
      _l2hparafilepath=${_cwd}
      cd ${_folddir}
      cp l2h/l2h.dat l2h/l2h_test.data
      if [ $? -ne 0 ]; then
        echo "failed to copy l2h/l2h.dat to l2h/l2h_test.data in ${_folddir}"
        return 1
      fi
      echo "run_l2h_doc_model ${_l2hlibpath} l2h l2h ${_l2hparafilepath}"
      run_l2h_doc_model ${_l2hlibpath} l2h l2h ${_l2hparafilepath}
      cd ${_cwd}
      let _f=${_f}+1
    done
    let _r=${_r}+1
  done
  return 0
}

function mv_htm_l2h_doc_model_output() {
  if [ $# -lt 4 ]; then
    echo "function mv_htm_l2h_doc_model_output requires 4 parameters"
    return 1
  fi
  _rootdir=$1
  _numrepetition=$2
  _numfolds=$3
  _doctype=$4
  _l2hparafilepath=$(pwd)

  . ${_l2hparafilepath}/l2hparas.sh
  has_l2h_para_set

  _r=0
  while [ ${_r} -lt ${_numrepetition} ]; do
    _f=0
    while [ ${_f} -lt ${_numfolds} ]; do 
      _folddir=${_rootdir}/testcase_repeat${_r}/${_f}/l2h_out
      _docmodelfile=${_folddir}/PRESET*/iter-predictions/iter-${l2h_maxiter}.txt
      for _df in ${_docmodelfile[*]}; do
        if [ ! -f ${_df} ]; then
          echo "Missing doc model file ${_df}"
          return 1
        else
          echo "Found doc model file ${_df}"
          _filename="${_df%.*}"
          _extension="${_df##*.}"
          _newfilename="${_filename}_${_doctype}.${_extension}"
          echo "rename ${_df} to ${_newfilename}"
          mv ${_df} ${_newfilename}
          if [ $? -ne 0 ]; then
            echo "Failed to rename ${_df} to ${_newfilename}"
            return 1
          fi
        fi
      done
      let _f=${_f}+1
    done
    let _r=${_r}+1
  done
  return 0
}

function infer_htm_test_doc_model() {
  if [ $# -lt 4 ]; then
    echo "function infer_htm_test_doc_model requires 4 parameters"
    return 1
  fi
  _rootdir=$1
  _numrepetition=$2
  _numfolds=$3
  _l2hlibpath=$4
  _r=0
  while [ ${_r} -lt ${_numrepetition} ]; do
    _f=0
    while [ ${_f} -lt ${_numfolds} ]; do 
      _folddir=${_rootdir}/testcase_repeat${_r}/${_f}/
      _cwd=`pwd`
      _l2hparafilepath=${_cwd}
      cd ${_folddir}
      cp l2h/l2h_testetr.dat l2h/l2h_test.data
      if [ $? -ne 0 ]; then
        echo "failed to copy l2h/l2h_testetr.dat to l2h/l2h_test.data in ${_folddir}"
        return 1
      fi
      echo "run_l2h_doc_model ${_l2hlibpath} l2h l2h ${_l2hparafilepath}"
      run_l2h_doc_model ${_l2hlibpath} l2h l2h ${_l2hparafilepath}
      cd ${_cwd}
      let _f=${_f}+1
    done
    let _r=${_r}+1
  done
  return 0
}

