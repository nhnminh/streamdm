../data/randomtreedata.arff
Learning Node Type: 2
File reading gets started.
FileReader RDD count:10000
==============================
Counter: 10000
Chunk: 10000
This root: Leaf weight = {0.0,0.0}{0.0,0.0}

That root: Leaf weight = {0.0,0.0}{2089.0,2911.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
This root: Leaf weight = {0.0,0.0}{2089.0,2911.0}

That root: Leaf weight = {0.0,0.0}{2007.0,2993.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
tmodel: Hoeffding Tree Model description:
Leaf weight = {0.0,0.0}{4096.0,5904.0}

Before merge: Hoeffding Tree Model description:
Leaf weight = {0.0,0.0}{0.0,0.0}

This root: Leaf weight = {0.0,0.0}{0.0,0.0}

That root: Leaf weight = {0.0,0.0}{4096.0,5904.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
HoeffdingTreeModel: addOnWeight = 10000.0
Best suggestion: FeatureSplit, merit=0.4785365748236884, NumericBinaryTest(false) feature[5] = 0.9999057216873717
Distribution From Split: class [0]= {1328.9310471486347,5904.0}
Distribution From Split: class [1]= {2767.0689528513653,0.0}
HoeffdingTreeModel: {3,2,2,2}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  Leaf weight = {1328.9310471486347,5904.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2767.0689528513653,0.0}{0.0,0.0}

Total instances: 10000.0
Accuracy 0.410
FileReader RDD count:10000
==============================
Counter: 20000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  Leaf weight = {1328.9310471486347,5904.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2767.0689528513653,0.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  Leaf weight = {1328.9310471486347,5904.0}{2109.0,2890.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2767.0689528513653,0.0}{0.0,1.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  Leaf weight = {1328.9310471486347,5904.0}{2109.0,2890.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2767.0689528513653,0.0}{0.0,1.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  Leaf weight = {1328.9310471486347,5904.0}{2137.0,2862.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2767.0689528513653,0.0}{1.0,0.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  Leaf weight = {1328.9310471486347,5904.0}{4246.0,5752.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2767.0689528513653,0.0}{1.0,1.0}

Before merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  Leaf weight = {1328.9310471486347,5904.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2767.0689528513653,0.0}{0.0,0.0}

This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  Leaf weight = {1328.9310471486347,5904.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2767.0689528513653,0.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  Leaf weight = {1328.9310471486347,5904.0}{4246.0,5752.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2767.0689528513653,0.0}{1.0,1.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
HoeffdingTreeModel: addOnWeight = 9998.0
Best suggestion: FeatureSplit, merit=0.07306136451445189, NominalMultiwayTestfeature[4] 5
Distribution From Split: class [0]= {258.0,1740.0}
Distribution From Split: class [1]= {587.0,1378.0}
Distribution From Split: class [2]= {1581.0,438.0}
Distribution From Split: class [3]= {891.0,1133.0}
Distribution From Split: class [4]= {929.0,1063.0}
HoeffdingTreeModel: {8,6,6,7}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {258.0,1740.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {587.0,1378.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {1581.0,438.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {891.0,1133.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {929.0,1063.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,1.0}{0.0,0.0}

Total instances: 12767.068952851365
Accuracy 0.425
FileReader RDD count:10000
==============================
Counter: 30000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {258.0,1740.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {587.0,1378.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {1581.0,438.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {891.0,1133.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {929.0,1063.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,1.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {258.0,1740.0}{113.0,939.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {587.0,1378.0}{301.0,683.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {1581.0,438.0}{730.0,227.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {891.0,1133.0}{430.0,568.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {929.0,1063.0}{465.0,543.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,1.0}{0.0,1.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {258.0,1740.0}{113.0,939.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {587.0,1378.0}{301.0,683.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {1581.0,438.0}{730.0,227.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {891.0,1133.0}{430.0,568.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {929.0,1063.0}{465.0,543.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,1.0}{0.0,1.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {258.0,1740.0}{114.0,877.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {587.0,1378.0}{298.0,707.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {1581.0,438.0}{755.0,223.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {891.0,1133.0}{482.0,566.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {929.0,1063.0}{458.0,519.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,1.0}{0.0,1.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {258.0,1740.0}{227.0,1816.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {587.0,1378.0}{599.0,1390.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {1581.0,438.0}{1485.0,450.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {891.0,1133.0}{912.0,1134.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {929.0,1063.0}{923.0,1062.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,1.0}{0.0,2.0}

Before merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {258.0,1740.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {587.0,1378.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {1581.0,438.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {891.0,1133.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {929.0,1063.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,1.0}{0.0,0.0}

This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {258.0,1740.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {587.0,1378.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {1581.0,438.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {891.0,1133.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {929.0,1063.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,1.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {258.0,1740.0}{227.0,1816.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {587.0,1378.0}{599.0,1390.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {1581.0,438.0}{1485.0,450.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {891.0,1133.0}{912.0,1134.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {929.0,1063.0}{923.0,1062.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,1.0}{0.0,2.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
HoeffdingTreeModel: addOnWeight = 1985.0
Best suggestion: FeatureSplit, merit=0.3575108868957887, NominalMultiwayTestfeature[1] 5
Distribution From Split: class [0]= {200.0,199.0}
Distribution From Split: class [1]= {361.0,10.0}
Distribution From Split: class [2]= {74.0,326.0}
Distribution From Split: class [3]= {265.0,156.0}
Distribution From Split: class [4]= {23.0,371.0}
HoeffdingTreeModel: {13,10,10,8}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {485.0,3556.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1186.0,2768.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {3066.0,888.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {1803.0,2267.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {200.0,199.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {361.0,10.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {74.0,326.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {265.0,156.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {23.0,371.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,3.0}{0.0,0.0}

Total instances: 20775.068952851365
Accuracy 0.691
FileReader RDD count:10000
==============================
Counter: 40000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {485.0,3556.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1186.0,2768.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {3066.0,888.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {1803.0,2267.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {200.0,199.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {361.0,10.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {74.0,326.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {265.0,156.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {23.0,371.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,3.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {485.0,3556.0}{120.0,897.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1186.0,2768.0}{291.0,693.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {3066.0,888.0}{788.0,224.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {1803.0,2267.0}{455.0,554.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {200.0,199.0}{107.0,110.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {361.0,10.0}{198.0,5.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {74.0,326.0}{42.0,154.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {265.0,156.0}{107.0,69.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {23.0,371.0}{6.0,180.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,3.0}{0.0,0.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {485.0,3556.0}{120.0,897.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1186.0,2768.0}{291.0,693.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {3066.0,888.0}{788.0,224.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {1803.0,2267.0}{455.0,554.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {200.0,199.0}{107.0,110.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {361.0,10.0}{198.0,5.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {74.0,326.0}{42.0,154.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {265.0,156.0}{107.0,69.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {23.0,371.0}{6.0,180.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,3.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {485.0,3556.0}{126.0,871.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1186.0,2768.0}{308.0,689.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {3066.0,888.0}{790.0,230.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {1803.0,2267.0}{449.0,519.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {200.0,199.0}{94.0,97.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {361.0,10.0}{187.0,6.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {74.0,326.0}{28.0,177.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {265.0,156.0}{124.0,85.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {23.0,371.0}{11.0,208.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,3.0}{1.0,0.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {485.0,3556.0}{246.0,1768.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1186.0,2768.0}{599.0,1382.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {3066.0,888.0}{1578.0,454.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {1803.0,2267.0}{904.0,1073.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {200.0,199.0}{201.0,207.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {361.0,10.0}{385.0,11.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {74.0,326.0}{70.0,331.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {265.0,156.0}{231.0,154.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {23.0,371.0}{17.0,388.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,3.0}{1.0,0.0}

Before merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {485.0,3556.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1186.0,2768.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {3066.0,888.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {1803.0,2267.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {200.0,199.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {361.0,10.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {74.0,326.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {265.0,156.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {23.0,371.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,3.0}{0.0,0.0}

This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {485.0,3556.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1186.0,2768.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {3066.0,888.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {1803.0,2267.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {200.0,199.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {361.0,10.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {74.0,326.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {265.0,156.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {23.0,371.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,3.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {485.0,3556.0}{246.0,1768.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1186.0,2768.0}{599.0,1382.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {3066.0,888.0}{1578.0,454.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {1803.0,2267.0}{904.0,1073.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {200.0,199.0}{201.0,207.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {361.0,10.0}{385.0,11.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {74.0,326.0}{70.0,331.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {265.0,156.0}{231.0,154.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {23.0,371.0}{17.0,388.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,3.0}{1.0,0.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
HoeffdingTreeModel: addOnWeight = 4057.0
Best suggestion: FeatureSplit, merit=0.24529377464970437, NumericBinaryTest(false) feature[8] = 0.699756786990496
Distribution From Split: class [0]= {241.54933644797038,4279.093201643156}
Distribution From Split: class [1]= {231.45066355202962,0.0}
HoeffdingTreeModel: {15,11,11,9}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          Leaf weight = {241.54933644797038,4279.093201643156}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {231.45066355202962,0.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1785.0,4150.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {4644.0,1342.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {2707.0,3340.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {401.0,406.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {746.0,21.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {144.0,657.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {496.0,310.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {40.0,759.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2769.0689528513653,3.0}{0.0,0.0}

Total instances: 29472.16215449452
Accuracy 0.652
FileReader RDD count:10000
==============================
Counter: 50000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          Leaf weight = {241.54933644797038,4279.093201643156}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {231.45066355202962,0.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1785.0,4150.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {4644.0,1342.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {2707.0,3340.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {401.0,406.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {746.0,21.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {144.0,657.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {496.0,310.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {40.0,759.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2769.0689528513653,3.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          Leaf weight = {241.54933644797038,4279.093201643156}{108.0,561.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {231.45066355202962,0.0}{27.0,275.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1785.0,4150.0}{299.0,706.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {4644.0,1342.0}{788.0,221.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {2707.0,3340.0}{442.0,563.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {401.0,406.0}{101.0,99.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {746.0,21.0}{190.0,5.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {144.0,657.0}{34.0,193.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {496.0,310.0}{110.0,76.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {40.0,759.0}{12.0,188.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2769.0689528513653,3.0}{1.0,1.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          Leaf weight = {241.54933644797038,4279.093201643156}{108.0,561.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {231.45066355202962,0.0}{27.0,275.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1785.0,4150.0}{299.0,706.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {4644.0,1342.0}{788.0,221.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {2707.0,3340.0}{442.0,563.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {401.0,406.0}{101.0,99.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {746.0,21.0}{190.0,5.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {144.0,657.0}{34.0,193.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {496.0,310.0}{110.0,76.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {40.0,759.0}{12.0,188.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2769.0689528513653,3.0}{1.0,1.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          Leaf weight = {241.54933644797038,4279.093201643156}{115.0,611.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {231.45066355202962,0.0}{20.0,292.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1785.0,4150.0}{301.0,665.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {4644.0,1342.0}{732.0,218.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {2707.0,3340.0}{425.0,576.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {401.0,406.0}{89.0,98.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {746.0,21.0}{198.0,7.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {144.0,657.0}{38.0,186.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {496.0,310.0}{136.0,83.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {40.0,759.0}{10.0,199.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2769.0689528513653,3.0}{0.0,1.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          Leaf weight = {241.54933644797038,4279.093201643156}{223.0,1172.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {231.45066355202962,0.0}{47.0,567.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1785.0,4150.0}{600.0,1371.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {4644.0,1342.0}{1520.0,439.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {2707.0,3340.0}{867.0,1139.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {401.0,406.0}{190.0,197.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {746.0,21.0}{388.0,12.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {144.0,657.0}{72.0,379.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {496.0,310.0}{246.0,159.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {40.0,759.0}{22.0,387.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2769.0689528513653,3.0}{1.0,2.0}

Before merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          Leaf weight = {241.54933644797038,4279.093201643156}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {231.45066355202962,0.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1785.0,4150.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {4644.0,1342.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {2707.0,3340.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {401.0,406.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {746.0,21.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {144.0,657.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {496.0,310.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {40.0,759.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2769.0689528513653,3.0}{0.0,0.0}

This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          Leaf weight = {241.54933644797038,4279.093201643156}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {231.45066355202962,0.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1785.0,4150.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {4644.0,1342.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {2707.0,3340.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {401.0,406.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {746.0,21.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {144.0,657.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {496.0,310.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {40.0,759.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2769.0689528513653,3.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          Leaf weight = {241.54933644797038,4279.093201643156}{223.0,1172.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {231.45066355202962,0.0}{47.0,567.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1785.0,4150.0}{600.0,1371.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {4644.0,1342.0}{1520.0,439.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {2707.0,3340.0}{867.0,1139.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {401.0,406.0}{190.0,197.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {746.0,21.0}{388.0,12.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {144.0,657.0}{72.0,379.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {496.0,310.0}{246.0,159.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {40.0,759.0}{22.0,387.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2769.0689528513653,3.0}{1.0,2.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
HoeffdingTreeModel: addOnWeight = 1395.0
Best suggestion: FeatureSplit, merit=0.13811461572468892, NumericBinaryTest(false) feature[8] = 0.5597368462536156
Distribution From Split: class [0]= {80.07465056682828,1559.6555004735615}
Distribution From Split: class [1]= {142.9253494331717,0.0}
HoeffdingTreeModel: {17,12,12,11}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {80.07465056682828,1559.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {142.9253494331717,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {278.4506635520296,567.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {2385.0,5521.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {6164.0,1781.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {3574.0,4479.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {591.0,603.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1134.0,33.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {216.0,1036.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {742.0,469.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {62.0,1146.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,5.0}{0.0,0.0}

Total instances: 35339.175116876955
Accuracy 0.704
FileReader RDD count:10000
==============================
Counter: 60000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {80.07465056682828,1559.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {142.9253494331717,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {278.4506635520296,567.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {2385.0,5521.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {6164.0,1781.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {3574.0,4479.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {591.0,603.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1134.0,33.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {216.0,1036.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {742.0,469.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {62.0,1146.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,5.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {80.07465056682828,1559.6555004735615}{92.0,434.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {142.9253494331717,0.0}{12.0,123.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {278.4506635520296,567.0}{15.0,281.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {2385.0,5521.0}{301.0,643.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {6164.0,1781.0}{875.0,204.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {3574.0,4479.0}{440.0,600.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {591.0,603.0}{96.0,92.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1134.0,33.0}{197.0,7.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {216.0,1036.0}{37.0,178.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {742.0,469.0}{111.0,78.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {62.0,1146.0}{10.0,173.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,5.0}{0.0,1.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {80.07465056682828,1559.6555004735615}{92.0,434.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {142.9253494331717,0.0}{12.0,123.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {278.4506635520296,567.0}{15.0,281.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {2385.0,5521.0}{301.0,643.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {6164.0,1781.0}{875.0,204.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {3574.0,4479.0}{440.0,600.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {591.0,603.0}{96.0,92.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1134.0,33.0}{197.0,7.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {216.0,1036.0}{37.0,178.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {742.0,469.0}{111.0,78.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {62.0,1146.0}{10.0,173.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,5.0}{0.0,1.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {80.07465056682828,1559.6555004735615}{96.0,475.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {142.9253494331717,0.0}{10.0,139.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {278.4506635520296,567.0}{19.0,271.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {2385.0,5521.0}{318.0,695.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {6164.0,1781.0}{787.0,197.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {3574.0,4479.0}{456.0,574.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {591.0,603.0}{86.0,98.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1134.0,33.0}{196.0,5.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {216.0,1036.0}{30.0,186.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {742.0,469.0}{113.0,60.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {62.0,1146.0}{9.0,180.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,5.0}{0.0,0.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {80.07465056682828,1559.6555004735615}{188.0,909.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {142.9253494331717,0.0}{22.0,262.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {278.4506635520296,567.0}{34.0,552.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {2385.0,5521.0}{619.0,1338.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {6164.0,1781.0}{1662.0,401.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {3574.0,4479.0}{896.0,1174.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {591.0,603.0}{182.0,190.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1134.0,33.0}{393.0,12.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {216.0,1036.0}{67.0,364.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {742.0,469.0}{224.0,138.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {62.0,1146.0}{19.0,353.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,5.0}{0.0,1.0}

Before merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {80.07465056682828,1559.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {142.9253494331717,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {278.4506635520296,567.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {2385.0,5521.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {6164.0,1781.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {3574.0,4479.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {591.0,603.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1134.0,33.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {216.0,1036.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {742.0,469.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {62.0,1146.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,5.0}{0.0,0.0}

This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {80.07465056682828,1559.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {142.9253494331717,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {278.4506635520296,567.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {2385.0,5521.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {6164.0,1781.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {3574.0,4479.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {591.0,603.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1134.0,33.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {216.0,1036.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {742.0,469.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {62.0,1146.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,5.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {80.07465056682828,1559.6555004735615}{188.0,909.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {142.9253494331717,0.0}{22.0,262.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {278.4506635520296,567.0}{34.0,552.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {2385.0,5521.0}{619.0,1338.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {6164.0,1781.0}{1662.0,401.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {3574.0,4479.0}{896.0,1174.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {591.0,603.0}{182.0,190.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1134.0,33.0}{393.0,12.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {216.0,1036.0}{67.0,364.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {742.0,469.0}{224.0,138.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {62.0,1146.0}{19.0,353.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,5.0}{0.0,1.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
HoeffdingTreeModel: addOnWeight = 1200.0
Best suggestion: FeatureSplit, merit=0.6453721732591294, NumericBinaryTest(false) feature[5] = 0.6996062278336554
Distribution From Split: class [0]= {21.344561552954158,1338.651122887459}
Distribution From Split: class [1]= {59.655438447045846,0.0}
HoeffdingTreeModel: {19,13,13,11}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {164.9253494331717,262.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {21.344561552954158,1338.651122887459}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {59.655438447045846,0.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {4470.0,5653.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {773.0,793.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1527.0,45.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {283.0,1400.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {966.0,607.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {81.0,1499.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

Total instances: 45327.375576212384
Accuracy 0.758
FileReader RDD count:10000
==============================
Counter: 70000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {164.9253494331717,262.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {21.344561552954158,1338.651122887459}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {59.655438447045846,0.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {4470.0,5653.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {773.0,793.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1527.0,45.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {283.0,1400.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {966.0,607.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {81.0,1499.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{95.0,448.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {164.9253494331717,262.0}{7.0,132.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {21.344561552954158,1338.651122887459}{23.0,182.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {59.655438447045846,0.0}{2.0,86.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{333.0,682.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{835.0,199.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {4470.0,5653.0}{456.0,550.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {773.0,793.0}{100.0,101.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1527.0,45.0}{195.0,1.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {283.0,1400.0}{29.0,150.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {966.0,607.0}{112.0,99.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {81.0,1499.0}{8.0,175.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{95.0,448.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {164.9253494331717,262.0}{7.0,132.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {21.344561552954158,1338.651122887459}{23.0,182.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {59.655438447045846,0.0}{2.0,86.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{333.0,682.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{835.0,199.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {4470.0,5653.0}{456.0,550.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {773.0,793.0}{100.0,101.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1527.0,45.0}{195.0,1.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {283.0,1400.0}{29.0,150.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {966.0,607.0}{112.0,99.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {81.0,1499.0}{8.0,175.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{110.0,453.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {164.9253494331717,262.0}{7.0,151.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {21.344561552954158,1338.651122887459}{15.0,197.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {59.655438447045846,0.0}{2.0,100.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{291.0,699.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{780.0,229.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {4470.0,5653.0}{434.0,568.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {773.0,793.0}{90.0,88.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1527.0,45.0}{217.0,7.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {283.0,1400.0}{33.0,152.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {966.0,607.0}{122.0,71.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {81.0,1499.0}{6.0,178.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{205.0,901.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {164.9253494331717,262.0}{14.0,283.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {21.344561552954158,1338.651122887459}{38.0,379.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {59.655438447045846,0.0}{4.0,186.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{624.0,1381.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{1615.0,428.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {4470.0,5653.0}{890.0,1118.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {773.0,793.0}{190.0,189.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1527.0,45.0}{412.0,8.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {283.0,1400.0}{62.0,302.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {966.0,607.0}{234.0,170.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {81.0,1499.0}{14.0,353.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

Before merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {164.9253494331717,262.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {21.344561552954158,1338.651122887459}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {59.655438447045846,0.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {4470.0,5653.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {773.0,793.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1527.0,45.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {283.0,1400.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {966.0,607.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {81.0,1499.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {164.9253494331717,262.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {21.344561552954158,1338.651122887459}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {59.655438447045846,0.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {4470.0,5653.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {773.0,793.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1527.0,45.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {283.0,1400.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {966.0,607.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {81.0,1499.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{205.0,901.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {164.9253494331717,262.0}{14.0,283.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {21.344561552954158,1338.651122887459}{38.0,379.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {59.655438447045846,0.0}{4.0,186.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{624.0,1381.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{1615.0,428.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {4470.0,5653.0}{890.0,1118.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {773.0,793.0}{190.0,189.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1527.0,45.0}{412.0,8.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {283.0,1400.0}{62.0,302.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {966.0,607.0}{234.0,170.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {81.0,1499.0}{14.0,353.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
HoeffdingTreeModel: addOnWeight = 10032.0
Best suggestion: FeatureSplit, merit=0.37266093749602036, NumericBinaryTest(false) feature[6] = 0.9999148746584813
Distribution From Split: class [0]= {7860.0,793.0719014793361}
Distribution From Split: class [1]= {0.0,1378.9280985206638}
HoeffdingTreeModel: {21,14,14,11}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {473.07465056682827,3369.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {178.9253494331717,545.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {59.34456155295416,1717.651122887459}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {63.655438447045846,186.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {7860.0,793.0719014793361}{0.0,0.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {5360.0,6771.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {963.0,982.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1939.0,53.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {345.0,1702.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1200.0,777.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {95.0,1852.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

Total instances: 53308.375576212384
Accuracy 0.812
FileReader RDD count:10000
==============================
Counter: 80000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {473.07465056682827,3369.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {178.9253494331717,545.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {59.34456155295416,1717.651122887459}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {63.655438447045846,186.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {7860.0,793.0719014793361}{0.0,0.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {5360.0,6771.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {963.0,982.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1939.0,53.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {345.0,1702.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1200.0,777.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {95.0,1852.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {473.07465056682827,3369.6555004735615}{101.0,460.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {178.9253494331717,545.0}{7.0,116.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {59.34456155295416,1717.651122887459}{22.0,186.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {63.655438447045846,186.0}{3.0,95.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{306.0,673.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {7860.0,793.0719014793361}{796.0,214.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {5360.0,6771.0}{431.0,578.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {963.0,982.0}{93.0,95.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1939.0,53.0}{197.0,5.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {345.0,1702.0}{29.0,170.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1200.0,777.0}{119.0,101.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {95.0,1852.0}{11.0,192.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {473.07465056682827,3369.6555004735615}{101.0,460.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {178.9253494331717,545.0}{7.0,116.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {59.34456155295416,1717.651122887459}{22.0,186.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {63.655438447045846,186.0}{3.0,95.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{306.0,673.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {7860.0,793.0719014793361}{796.0,214.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {5360.0,6771.0}{431.0,578.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {963.0,982.0}{93.0,95.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1939.0,53.0}{197.0,5.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {345.0,1702.0}{29.0,170.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1200.0,777.0}{119.0,101.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {95.0,1852.0}{11.0,192.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {473.07465056682827,3369.6555004735615}{99.0,460.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {178.9253494331717,545.0}{10.0,141.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {59.34456155295416,1717.651122887459}{17.0,209.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {63.655438447045846,186.0}{1.0,81.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{307.0,714.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {7860.0,793.0719014793361}{790.0,225.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {5360.0,6771.0}{405.0,513.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {963.0,982.0}{98.0,103.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1939.0,53.0}{200.0,3.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {345.0,1702.0}{25.0,180.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1200.0,777.0}{127.0,93.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {95.0,1852.0}{18.0,180.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,1.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {473.07465056682827,3369.6555004735615}{200.0,920.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {178.9253494331717,545.0}{17.0,257.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {59.34456155295416,1717.651122887459}{39.0,395.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {63.655438447045846,186.0}{4.0,176.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{613.0,1387.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {7860.0,793.0719014793361}{1586.0,439.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {5360.0,6771.0}{836.0,1091.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {963.0,982.0}{191.0,198.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1939.0,53.0}{397.0,8.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {345.0,1702.0}{54.0,350.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1200.0,777.0}{246.0,194.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {95.0,1852.0}{29.0,372.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,1.0}

Before merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {473.07465056682827,3369.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {178.9253494331717,545.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {59.34456155295416,1717.651122887459}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {63.655438447045846,186.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {7860.0,793.0719014793361}{0.0,0.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {5360.0,6771.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {963.0,982.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1939.0,53.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {345.0,1702.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1200.0,777.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {95.0,1852.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {473.07465056682827,3369.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {178.9253494331717,545.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {59.34456155295416,1717.651122887459}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {63.655438447045846,186.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {7860.0,793.0719014793361}{0.0,0.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {5360.0,6771.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {963.0,982.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1939.0,53.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {345.0,1702.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1200.0,777.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {95.0,1852.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {473.07465056682827,3369.6555004735615}{200.0,920.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {178.9253494331717,545.0}{17.0,257.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {59.34456155295416,1717.651122887459}{39.0,395.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {63.655438447045846,186.0}{4.0,176.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{613.0,1387.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {7860.0,793.0719014793361}{1586.0,439.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      Leaf weight = {5360.0,6771.0}{836.0,1091.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {963.0,982.0}{191.0,198.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {1939.0,53.0}{397.0,8.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {345.0,1702.0}{54.0,350.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1200.0,777.0}{246.0,194.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {95.0,1852.0}{29.0,372.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,1.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
HoeffdingTreeModel: addOnWeight = 12034.0
Best suggestion: FeatureSplit, merit=0.10406844011933714, NominalMultiwayTestfeature[2] 5
Distribution From Split: class [0]= {464.0,1965.0}
Distribution From Split: class [1]= {880.0,1503.0}
Distribution From Split: class [2]= {1278.0,1102.0}
Distribution From Split: class [3]= {1784.0,636.0}
Distribution From Split: class [4]= {899.0,1523.0}
HoeffdingTreeModel: {26,18,18,11}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {673.0746505668283,4289.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {195.9253494331717,802.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {98.34456155295416,2112.6511228874588}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {67.65543844704584,362.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {9446.0,1232.0719014793362}{0.0,0.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {464.0,1965.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {880.0,1503.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1278.0,1102.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1784.0,636.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {899.0,1523.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1154.0,1180.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2336.0,61.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {399.0,2052.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1446.0,971.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {124.0,2224.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

Total instances: 61284.37557621239
Accuracy 0.770
FileReader RDD count:10000
==============================
Counter: 90000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {673.0746505668283,4289.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {195.9253494331717,802.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {98.34456155295416,2112.6511228874588}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {67.65543844704584,362.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {9446.0,1232.0719014793362}{0.0,0.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {464.0,1965.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {880.0,1503.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1278.0,1102.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1784.0,636.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {899.0,1523.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1154.0,1180.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2336.0,61.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {399.0,2052.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1446.0,971.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {124.0,2224.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {673.0746505668283,4289.655500473562}{98.0,462.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {195.9253494331717,802.0}{9.0,135.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {98.34456155295416,2112.6511228874588}{16.0,185.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {67.65543844704584,362.0}{2.0,86.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{336.0,665.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {9446.0,1232.0719014793362}{811.0,221.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {464.0,1965.0}{28.0,158.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {880.0,1503.0}{76.0,138.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1278.0,1102.0}{107.0,79.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1784.0,636.0}{154.0,52.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {899.0,1523.0}{64.0,121.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1154.0,1180.0}{101.0,99.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2336.0,61.0}{187.0,7.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {399.0,2052.0}{26.0,177.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1446.0,971.0}{112.0,75.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {124.0,2224.0}{7.0,206.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {673.0746505668283,4289.655500473562}{98.0,462.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {195.9253494331717,802.0}{9.0,135.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {98.34456155295416,2112.6511228874588}{16.0,185.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {67.65543844704584,362.0}{2.0,86.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{336.0,665.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {9446.0,1232.0719014793362}{811.0,221.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {464.0,1965.0}{28.0,158.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {880.0,1503.0}{76.0,138.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1278.0,1102.0}{107.0,79.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1784.0,636.0}{154.0,52.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {899.0,1523.0}{64.0,121.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1154.0,1180.0}{101.0,99.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2336.0,61.0}{187.0,7.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {399.0,2052.0}{26.0,177.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1446.0,971.0}{112.0,75.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {124.0,2224.0}{7.0,206.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {673.0746505668283,4289.655500473562}{96.0,456.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {195.9253494331717,802.0}{11.0,142.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {98.34456155295416,2112.6511228874588}{16.0,178.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {67.65543844704584,362.0}{4.0,99.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{301.0,714.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {9446.0,1232.0719014793362}{768.0,239.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{1.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {464.0,1965.0}{32.0,169.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {880.0,1503.0}{82.0,130.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1278.0,1102.0}{105.0,80.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1784.0,636.0}{137.0,52.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {899.0,1523.0}{82.0,131.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1154.0,1180.0}{96.0,110.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2336.0,61.0}{181.0,8.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {399.0,2052.0}{29.0,161.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1446.0,971.0}{120.0,80.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {124.0,2224.0}{9.0,181.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {673.0746505668283,4289.655500473562}{194.0,918.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {195.9253494331717,802.0}{20.0,277.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {98.34456155295416,2112.6511228874588}{32.0,363.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {67.65543844704584,362.0}{6.0,185.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{637.0,1379.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {9446.0,1232.0719014793362}{1579.0,460.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{1.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {464.0,1965.0}{60.0,327.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {880.0,1503.0}{158.0,268.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1278.0,1102.0}{212.0,159.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1784.0,636.0}{291.0,104.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {899.0,1523.0}{146.0,252.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1154.0,1180.0}{197.0,209.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2336.0,61.0}{368.0,15.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {399.0,2052.0}{55.0,338.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1446.0,971.0}{232.0,155.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {124.0,2224.0}{16.0,387.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

Before merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {673.0746505668283,4289.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {195.9253494331717,802.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {98.34456155295416,2112.6511228874588}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {67.65543844704584,362.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {9446.0,1232.0719014793362}{0.0,0.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {464.0,1965.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {880.0,1503.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1278.0,1102.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1784.0,636.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {899.0,1523.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1154.0,1180.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2336.0,61.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {399.0,2052.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1446.0,971.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {124.0,2224.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {673.0746505668283,4289.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {195.9253494331717,802.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {98.34456155295416,2112.6511228874588}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {67.65543844704584,362.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {9446.0,1232.0719014793362}{0.0,0.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {464.0,1965.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {880.0,1503.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1278.0,1102.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1784.0,636.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {899.0,1523.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1154.0,1180.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2336.0,61.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {399.0,2052.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1446.0,971.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {124.0,2224.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {673.0746505668283,4289.655500473562}{194.0,918.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {195.9253494331717,802.0}{20.0,277.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {98.34456155295416,2112.6511228874588}{32.0,363.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {67.65543844704584,362.0}{6.0,185.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{637.0,1379.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {9446.0,1232.0719014793362}{1579.0,460.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {0.0,1378.9280985206638}{1.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {464.0,1965.0}{60.0,327.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {880.0,1503.0}{158.0,268.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1278.0,1102.0}{212.0,159.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1784.0,636.0}{291.0,104.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {899.0,1523.0}{146.0,252.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1154.0,1180.0}{197.0,209.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2336.0,61.0}{368.0,15.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {399.0,2052.0}{55.0,338.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1446.0,971.0}{232.0,155.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {124.0,2224.0}{16.0,387.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
HoeffdingTreeModel: addOnWeight = 2357.0
HoeffdingTreeModel: {26,18,18,11}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {867.0746505668283,5207.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {215.9253494331717,1079.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {130.34456155295416,2475.6511228874588}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {73.65543844704584,547.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {11025.0,1692.0719014793362}{0.0,0.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {1.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {524.0,2292.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {1038.0,1771.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1490.0,1261.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {2075.0,740.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {1045.0,1775.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1351.0,1389.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2704.0,76.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {454.0,2390.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1678.0,1126.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {140.0,2611.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

Total instances: 71284.37557621238
Accuracy 0.833
FileReader RDD count:10000
==============================
Counter: 100000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {867.0746505668283,5207.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {215.9253494331717,1079.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {130.34456155295416,2475.6511228874588}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {73.65543844704584,547.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {11025.0,1692.0719014793362}{0.0,0.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {1.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {524.0,2292.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {1038.0,1771.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1490.0,1261.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {2075.0,740.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {1045.0,1775.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1351.0,1389.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2704.0,76.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {454.0,2390.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1678.0,1126.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {140.0,2611.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {867.0746505668283,5207.655500473562}{104.0,449.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {215.9253494331717,1079.0}{9.0,128.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {130.34456155295416,2475.6511228874588}{17.0,193.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {73.65543844704584,547.0}{1.0,89.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{315.0,650.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {11025.0,1692.0719014793362}{824.0,230.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {1.0,1378.9280985206638}{1.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {524.0,2292.0}{31.0,166.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {1038.0,1771.0}{71.0,120.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1490.0,1261.0}{124.0,101.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {2075.0,740.0}{110.0,50.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {1045.0,1775.0}{70.0,131.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1351.0,1389.0}{109.0,91.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2704.0,76.0}{190.0,6.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {454.0,2390.0}{40.0,151.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1678.0,1126.0}{138.0,88.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {140.0,2611.0}{9.0,194.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {867.0746505668283,5207.655500473562}{104.0,449.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {215.9253494331717,1079.0}{9.0,128.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {130.34456155295416,2475.6511228874588}{17.0,193.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {73.65543844704584,547.0}{1.0,89.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{315.0,650.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {11025.0,1692.0719014793362}{824.0,230.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {1.0,1378.9280985206638}{1.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {524.0,2292.0}{31.0,166.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {1038.0,1771.0}{71.0,120.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1490.0,1261.0}{124.0,101.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {2075.0,740.0}{110.0,50.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {1045.0,1775.0}{70.0,131.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1351.0,1389.0}{109.0,91.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2704.0,76.0}{190.0,6.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {454.0,2390.0}{40.0,151.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1678.0,1126.0}{138.0,88.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {140.0,2611.0}{9.0,194.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {867.0746505668283,5207.655500473562}{87.0,473.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {215.9253494331717,1079.0}{14.0,145.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {130.34456155295416,2475.6511228874588}{19.0,172.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {73.65543844704584,547.0}{2.0,102.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{274.0,681.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {11025.0,1692.0719014793362}{787.0,245.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {1.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {524.0,2292.0}{52.0,152.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {1038.0,1771.0}{82.0,126.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1490.0,1261.0}{113.0,77.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {2075.0,740.0}{157.0,47.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {1045.0,1775.0}{65.0,117.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1351.0,1389.0}{93.0,117.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2704.0,76.0}{188.0,5.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {454.0,2390.0}{24.0,174.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1678.0,1126.0}{128.0,77.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {140.0,2611.0}{14.0,191.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = false
Node merge only
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {867.0746505668283,5207.655500473562}{191.0,922.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {215.9253494331717,1079.0}{23.0,273.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {130.34456155295416,2475.6511228874588}{36.0,365.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {73.65543844704584,547.0}{3.0,191.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{589.0,1331.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {11025.0,1692.0719014793362}{1611.0,475.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {1.0,1378.9280985206638}{1.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {524.0,2292.0}{83.0,318.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {1038.0,1771.0}{153.0,246.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1490.0,1261.0}{237.0,178.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {2075.0,740.0}{267.0,97.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {1045.0,1775.0}{135.0,248.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1351.0,1389.0}{202.0,208.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2704.0,76.0}{378.0,11.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {454.0,2390.0}{64.0,325.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1678.0,1126.0}{266.0,165.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {140.0,2611.0}{23.0,385.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

Before merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {867.0746505668283,5207.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {215.9253494331717,1079.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {130.34456155295416,2475.6511228874588}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {73.65543844704584,547.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {11025.0,1692.0719014793362}{0.0,0.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {1.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {524.0,2292.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {1038.0,1771.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1490.0,1261.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {2075.0,740.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {1045.0,1775.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1351.0,1389.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2704.0,76.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {454.0,2390.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1678.0,1126.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {140.0,2611.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {867.0746505668283,5207.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {215.9253494331717,1079.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {130.34456155295416,2475.6511228874588}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {73.65543844704584,547.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {11025.0,1692.0719014793362}{0.0,0.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {1.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {524.0,2292.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {1038.0,1771.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1490.0,1261.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {2075.0,740.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {1045.0,1775.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1351.0,1389.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2704.0,76.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {454.0,2390.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1678.0,1126.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {140.0,2611.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {867.0746505668283,5207.655500473562}{191.0,922.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {215.9253494331717,1079.0}{23.0,273.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {130.34456155295416,2475.6511228874588}{36.0,365.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {73.65543844704584,547.0}{3.0,191.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{589.0,1331.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          Leaf weight = {11025.0,1692.0719014793362}{1611.0,475.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {1.0,1378.9280985206638}{1.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {524.0,2292.0}{83.0,318.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {1038.0,1771.0}{153.0,246.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1490.0,1261.0}{237.0,178.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {2075.0,740.0}{267.0,97.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {1045.0,1775.0}{135.0,248.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1351.0,1389.0}{202.0,208.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {2704.0,76.0}{378.0,11.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {454.0,2390.0}{64.0,325.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1678.0,1126.0}{266.0,165.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {140.0,2611.0}{23.0,385.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
LearningNodeNBAdaptive Merge
NodeMerge: trySplit = true
Node merge with Split!
HoeffdingTreeModel: addOnWeight = 6150.0
Best suggestion: FeatureSplit, merit=0.00875964973992982, NumericBinaryTest(false) feature[9] = 0.6999897484377254
Distribution From Split: class [0]= {5768.489959646972,1050.9282431764977}
Distribution From Split: class [1]= {0.0,323.0717568235022}
HoeffdingTreeModel: {28,19,19,11}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {1058.0746505668283,6129.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              Leaf weight = {238.9253494331717,1352.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          
       if [feature 5 numeric 0] < 0.6996062278336554
              Leaf weight = {166.34456155295416,2840.6511228874588}{0.0,0.0}
       if [feature 5 numeric 1] >= 0.6996062278336554
              Leaf weight = {76.65543844704584,738.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {5467.0,12337.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 6 numeric 0] < 0.9999148746584813
          
       if [feature 9 numeric 0] < 0.6999897484377254
              Leaf weight = {5768.489959646972,1050.9282431764977}{0.0,0.0}
       if [feature 9 numeric 1] >= 0.6999897484377254
              Leaf weight = {0.0,323.0717568235022}{0.0,0.0}
     if [feature 6 numeric 1] >= 0.9999148746584813
          Leaf weight = {2.0,1378.9280985206638}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {607.0,2610.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {1191.0,2017.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1727.0,1439.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {2342.0,837.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {1180.0,2023.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          Leaf weight = {1553.0,1597.0}{0.0,0.0}
     if [feature 1 nominal 1] == 1
          Leaf weight = {3082.0,87.0}{0.0,0.0}
     if [feature 1 nominal 2] == 2
          Leaf weight = {518.0,2715.0}{0.0,0.0}
     if [feature 1 nominal 3] == 3
          Leaf weight = {1944.0,1291.0}{0.0,0.0}
     if [feature 1 nominal 4] == 4
          Leaf weight = {163.0,2996.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,7.0}{0.0,0.0}

Total instances: 73623.79363438002
Accuracy 0.821
FileReader RDD count:10000
==============================
Counter: 110000
Chunk: 10000
Over 100.000 instances
Running time = 102.218519704
Reading file stopped.
