
cd $STREAM_DM
sbt package 
echo "Finish compiling ...."
cd scripts/
echo "Receiving data...."
# ./spark.sh "EvaluatePrequential -l (SGDLearner -l 0.01 -o LogisticLoss -r ZeroRegularizer) -e (BasicClassificationEvaluator)  " 1>sth.res 2>sth.log


start=`date +%s`
# ./spark.sh "EvaluatePrequential -l trees.HoeffdingTree -s (FileReader -f ../data/covtypeNorm.arff)" 1>hdt2.res 2>hdt2.log 
./spark.sh "EvaluatePrequential -l (trees.HoeffdingTree -l 2 -t 0.05 ) -s (FileReader -f ../data/randomtreedata.arff -k 1000)" 1> result/majcl5_t_1000_NBA.res 2> result/majcl4_t.log
end=`date +%s`

echo runtime = $((end-start)) "seconds"
