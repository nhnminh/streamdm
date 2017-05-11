
#Run program 
echo "Submitting"
# ./spark_cluster.sh "EvaluatePrequential -l (SGDLearner -l 0.01 -o LogisticLoss -r ZeroRegularizer) -e (BasicClassificationEvaluator)  " 1>sth.res 2>sth.log
./spark_cluster.sh "EvaluatePrequential -l trees.HoeffdingTree –s (FileReader –k 100 –d 60 –f ../data/covtypeNorm.arff)"  2>stage2.log 1>split2.log
# ./spark_cluster.sh 1>hdt.res 2>hdt.log
echo "Finish."
#Copy log to local.
# scp nhnguyen@lame11.enst.fr:/cal/homes/nhnguyen/streamdm/hdt.log new.log
# scp nhnguyen@lame11.enst.fr:/cal/homes/nhnguyen/streamdm/hdt.res new.res
