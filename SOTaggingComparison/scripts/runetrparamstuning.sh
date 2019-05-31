#!/bin/bash

ROOTDIR=./stackoverflow.U2_UH
STEP=0.2
W1=${STEP}
while (( $(echo "${W1} <= 1.0" | bc -l) )); do
  W2=${STEP}
  while (( $(echo "${W2} <= 1.0" | bc -l) )); do
    W3=${STEP}
    while (( $(echo "${W3} <= 1.0" | bc -l) )); do
      W4=${STEP}
      while (( $(echo "${W4} <= 1.0" | bc -l) )); do
        printf "(w1,w2,w3,w4)=(%4.2f,%4.2f,%4.2f,%4.2f)\n" \
		${W1} ${W2} ${W3} ${W4} 
        printf "%4.2f\n%4.2f\n%4.2f\n%4.2f\n" \
		${W1} ${W2} ${W3} ${W4} > linearCombineParas.txt
	sync
	sync
	sync
	./runrq1htm.sh cross_validation
	sync
	sync
	sync
	mkdir -p ${ROOTDIR}/testcase_repeat0/${W1}${W2}${W3}${W4}
	mv ${ROOTDIR}/testcase_repeat0/ubf*.csv ${ROOTDIR}/testcase_repeat0/${W1}${W2}${W3}${W4}
        W4=`echo ${W4} + ${STEP} | bc -l`
      done
      W3=`echo ${W3} + ${STEP} | bc -l`
    done
    W2=`echo ${W2} + ${STEP} | bc -l`
  done
  W1=`echo ${W1} + ${STEP} | bc -l`
done
