#!/bin/bash 

wget -c http://opennlp.sourceforge.net/models-1.5/en-sent.bin
[ $? -eq 0 ] || exit 1
wget -c http://opennlp.sourceforge.net/models-1.5/en-token.bin
[ $? -eq 0 ] || exit 1
wget -c http://apache.cs.utah.edu/opennlp/models/langdetect/1.8.3/langdetect-183.bin
[ $? -eq 0 ] || exit 1
wget -c http://opennlp.sourceforge.net/models-1.5/en-pos-maxent.bin
[ $? -eq 0 ] || exit 1
wget -c https://raw.githubusercontent.com/richardwilly98/elasticsearch-opennlp-auto-tagging/master/src/main/resources/models/en-lemmatizer.dict
[ $? -eq 0 ] || exit 1
wget -c https://gist.githubusercontent.com/carloschavez9/63414d83f68b09b4ef2926cc20ad641c/raw/7557ef316b35207b4524068e32e4705e2c517c2c/nlp_en_stop_words.txt
[ $? -eq 0 ] || exit 1
mv nlp_en_stop_words.txt en_stop_words.txt
