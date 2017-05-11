../data/randomtreedata.arff
Learning Node Type: 0
File reading gets started.
FileReader RDD count:10000
==============================
Counter: 10000
Chunk: 10000
This root: Leaf weight = {0.0,0.0}{0.0,0.0}

That root: Leaf weight = {0.0,0.0}{2007.0,2993.0}

After merging 2 nodes, ClassDistr: {0.0,0.0}
This root: Leaf weight = {0.0,0.0}{2007.0,2993.0}

That root: Leaf weight = {0.0,0.0}{2089.0,2911.0}

After merging 2 nodes, ClassDistr: {0.0,0.0}
tmodel: Hoeffding Tree Model description:
Leaf weight = {0.0,0.0}{4096.0,5904.0}

Before merge: Hoeffding Tree Model description:
Leaf weight = {0.0,0.0}{0.0,0.0}

This root: Leaf weight = {0.0,0.0}{0.0,0.0}

That root: Leaf weight = {0.0,0.0}{4096.0,5904.0}

After merging 2 nodes, ClassDistr: {4096.0,5904.0}
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
Accuracy 0.591
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
  Leaf weight = {1328.9310471486347,5904.0}{2137.0,2862.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2767.0689528513653,0.0}{1.0,0.0}

After merging 2 nodes, ClassDistr: {1328.9310471486347,5904.0}
After merging 2 nodes, ClassDistr: {2767.0689528513653,0.0}
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  Leaf weight = {1328.9310471486347,5904.0}{2137.0,2862.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2767.0689528513653,0.0}{1.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  Leaf weight = {1328.9310471486347,5904.0}{2109.0,2890.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2767.0689528513653,0.0}{0.0,1.0}

After merging 2 nodes, ClassDistr: {1328.9310471486347,5904.0}
After merging 2 nodes, ClassDistr: {2767.0689528513653,0.0}
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

After merging 2 nodes, ClassDistr: {5574.931047148635,11656.0}
After merging 2 nodes, ClassDistr: {2768.0689528513653,1.0}
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
Accuracy 0.690
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

After merging 2 nodes, ClassDistr: {258.0,1740.0}
After merging 2 nodes, ClassDistr: {587.0,1378.0}
After merging 2 nodes, ClassDistr: {1581.0,438.0}
After merging 2 nodes, ClassDistr: {891.0,1133.0}
After merging 2 nodes, ClassDistr: {929.0,1063.0}
After merging 2 nodes, ClassDistr: {2768.0689528513653,1.0}
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

After merging 2 nodes, ClassDistr: {258.0,1740.0}
After merging 2 nodes, ClassDistr: {587.0,1378.0}
After merging 2 nodes, ClassDistr: {1581.0,438.0}
After merging 2 nodes, ClassDistr: {891.0,1133.0}
After merging 2 nodes, ClassDistr: {929.0,1063.0}
After merging 2 nodes, ClassDistr: {2768.0689528513653,1.0}
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

After merging 2 nodes, ClassDistr: {485.0,3556.0}
After merging 2 nodes, ClassDistr: {1186.0,2768.0}
After merging 2 nodes, ClassDistr: {3066.0,888.0}
After merging 2 nodes, ClassDistr: {1803.0,2267.0}
After merging 2 nodes, ClassDistr: {1852.0,2125.0}
After merging 2 nodes, ClassDistr: {2768.0689528513653,3.0}
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
Accuracy 0.735
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

After merging 2 nodes, ClassDistr: {485.0,3556.0}
After merging 2 nodes, ClassDistr: {1186.0,2768.0}
After merging 2 nodes, ClassDistr: {3066.0,888.0}
After merging 2 nodes, ClassDistr: {1803.0,2267.0}
After merging 2 nodes, ClassDistr: {200.0,199.0}
After merging 2 nodes, ClassDistr: {361.0,10.0}
After merging 2 nodes, ClassDistr: {74.0,326.0}
After merging 2 nodes, ClassDistr: {265.0,156.0}
After merging 2 nodes, ClassDistr: {23.0,371.0}
After merging 2 nodes, ClassDistr: {2768.0689528513653,3.0}
This root: 
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

After merging 2 nodes, ClassDistr: {485.0,3556.0}
After merging 2 nodes, ClassDistr: {1186.0,2768.0}
After merging 2 nodes, ClassDistr: {3066.0,888.0}
After merging 2 nodes, ClassDistr: {1803.0,2267.0}
After merging 2 nodes, ClassDistr: {200.0,199.0}
After merging 2 nodes, ClassDistr: {361.0,10.0}
After merging 2 nodes, ClassDistr: {74.0,326.0}
After merging 2 nodes, ClassDistr: {265.0,156.0}
After merging 2 nodes, ClassDistr: {23.0,371.0}
After merging 2 nodes, ClassDistr: {2768.0689528513653,3.0}
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

After merging 2 nodes, ClassDistr: {731.0,5324.0}
After merging 2 nodes, ClassDistr: {1785.0,4150.0}
After merging 2 nodes, ClassDistr: {4644.0,1342.0}
After merging 2 nodes, ClassDistr: {2707.0,3340.0}
After merging 2 nodes, ClassDistr: {401.0,406.0}
After merging 2 nodes, ClassDistr: {746.0,21.0}
After merging 2 nodes, ClassDistr: {144.0,657.0}
After merging 2 nodes, ClassDistr: {496.0,310.0}
After merging 2 nodes, ClassDistr: {40.0,759.0}
After merging 2 nodes, ClassDistr: {2769.0689528513653,3.0}
HoeffdingTreeModel: addOnWeight = 3970.0
Best suggestion: FeatureSplit, merit=0.11404247917520782, NominalMultiwayTestfeature[2] 5
Distribution From Split: class [0]= {317.0,480.0}
Distribution From Split: class [1]= {103.0,641.0}
Distribution From Split: class [2]= {76.0,744.0}
Distribution From Split: class [3]= {470.0,328.0}
Distribution From Split: class [4]= {232.0,579.0}
HoeffdingTreeModel: {18,14,14,11}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {731.0,5324.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {317.0,480.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {103.0,641.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {76.0,744.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {470.0,328.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {232.0,579.0}{0.0,0.0}
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

Total instances: 28810.068952851365
Accuracy 0.739
FileReader RDD count:10000
==============================
Counter: 50000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {731.0,5324.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {317.0,480.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {103.0,641.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {76.0,744.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {470.0,328.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {232.0,579.0}{0.0,0.0}
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
      Leaf weight = {731.0,5324.0}{135.0,836.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {317.0,480.0}{95.0,115.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {103.0,641.0}{28.0,172.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {76.0,744.0}{13.0,195.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {470.0,328.0}{96.0,84.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {232.0,579.0}{67.0,140.0}
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

After merging 2 nodes, ClassDistr: {731.0,5324.0}
After merging 2 nodes, ClassDistr: {317.0,480.0}
After merging 2 nodes, ClassDistr: {103.0,641.0}
After merging 2 nodes, ClassDistr: {76.0,744.0}
After merging 2 nodes, ClassDistr: {470.0,328.0}
After merging 2 nodes, ClassDistr: {232.0,579.0}
After merging 2 nodes, ClassDistr: {4644.0,1342.0}
After merging 2 nodes, ClassDistr: {2707.0,3340.0}
After merging 2 nodes, ClassDistr: {401.0,406.0}
After merging 2 nodes, ClassDistr: {746.0,21.0}
After merging 2 nodes, ClassDistr: {144.0,657.0}
After merging 2 nodes, ClassDistr: {496.0,310.0}
After merging 2 nodes, ClassDistr: {40.0,759.0}
After merging 2 nodes, ClassDistr: {2769.0689528513653,3.0}
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {731.0,5324.0}{135.0,836.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {317.0,480.0}{95.0,115.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {103.0,641.0}{28.0,172.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {76.0,744.0}{13.0,195.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {470.0,328.0}{96.0,84.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {232.0,579.0}{67.0,140.0}
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
      Leaf weight = {731.0,5324.0}{135.0,903.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {317.0,480.0}{84.0,108.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {103.0,641.0}{27.0,181.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {76.0,744.0}{8.0,172.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {470.0,328.0}{122.0,77.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {232.0,579.0}{60.0,127.0}
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

After merging 2 nodes, ClassDistr: {731.0,5324.0}
After merging 2 nodes, ClassDistr: {317.0,480.0}
After merging 2 nodes, ClassDistr: {103.0,641.0}
After merging 2 nodes, ClassDistr: {76.0,744.0}
After merging 2 nodes, ClassDistr: {470.0,328.0}
After merging 2 nodes, ClassDistr: {232.0,579.0}
After merging 2 nodes, ClassDistr: {4644.0,1342.0}
After merging 2 nodes, ClassDistr: {2707.0,3340.0}
After merging 2 nodes, ClassDistr: {401.0,406.0}
After merging 2 nodes, ClassDistr: {746.0,21.0}
After merging 2 nodes, ClassDistr: {144.0,657.0}
After merging 2 nodes, ClassDistr: {496.0,310.0}
After merging 2 nodes, ClassDistr: {40.0,759.0}
After merging 2 nodes, ClassDistr: {2769.0689528513653,3.0}
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {731.0,5324.0}{270.0,1739.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {317.0,480.0}{179.0,223.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {103.0,641.0}{55.0,353.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {76.0,744.0}{21.0,367.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {470.0,328.0}{218.0,161.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {232.0,579.0}{127.0,267.0}
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
      Leaf weight = {731.0,5324.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {317.0,480.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {103.0,641.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {76.0,744.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {470.0,328.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {232.0,579.0}{0.0,0.0}
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
      Leaf weight = {731.0,5324.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {317.0,480.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {103.0,641.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {76.0,744.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {470.0,328.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {232.0,579.0}{0.0,0.0}
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
      Leaf weight = {731.0,5324.0}{270.0,1739.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {317.0,480.0}{179.0,223.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {103.0,641.0}{55.0,353.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {76.0,744.0}{21.0,367.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {470.0,328.0}{218.0,161.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {232.0,579.0}{127.0,267.0}
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

After merging 2 nodes, ClassDistr: {1001.0,7063.0}
After merging 2 nodes, ClassDistr: {496.0,703.0}
After merging 2 nodes, ClassDistr: {158.0,994.0}
After merging 2 nodes, ClassDistr: {97.0,1111.0}
After merging 2 nodes, ClassDistr: {688.0,489.0}
After merging 2 nodes, ClassDistr: {359.0,846.0}
After merging 2 nodes, ClassDistr: {6164.0,1781.0}
After merging 2 nodes, ClassDistr: {3574.0,4479.0}
After merging 2 nodes, ClassDistr: {591.0,603.0}
After merging 2 nodes, ClassDistr: {1134.0,33.0}
After merging 2 nodes, ClassDistr: {216.0,1036.0}
After merging 2 nodes, ClassDistr: {742.0,469.0}
After merging 2 nodes, ClassDistr: {62.0,1146.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,5.0}
HoeffdingTreeModel: addOnWeight = 6066.0
Best suggestion: FeatureSplit, merit=0.2361327036609328, NumericBinaryTest(false) feature[8] = 0.6998122671377988
Distribution From Split: class [0]= {394.6529553186302,6373.913903662247}
Distribution From Split: class [1]= {348.3470446813698,0.0}
HoeffdingTreeModel: {20,15,15,11}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          Leaf weight = {394.6529553186302,6373.913903662247}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {348.3470446813698,0.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {496.0,703.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {158.0,994.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {97.0,1111.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {688.0,489.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {359.0,846.0}{0.0,0.0}
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

Total instances: 37862.98285651362
Accuracy 0.691
FileReader RDD count:10000
==============================
Counter: 60000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          Leaf weight = {394.6529553186302,6373.913903662247}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {348.3470446813698,0.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {496.0,703.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {158.0,994.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {97.0,1111.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {688.0,489.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {359.0,846.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          Leaf weight = {394.6529553186302,6373.913903662247}{106.0,614.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {348.3470446813698,0.0}{19.0,271.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {496.0,703.0}{106.0,109.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {158.0,994.0}{35.0,159.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {97.0,1111.0}{13.0,192.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {688.0,489.0}{98.0,97.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {359.0,846.0}{66.0,138.0}
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

After merging 2 nodes, ClassDistr: {394.6529553186302,6373.913903662247}
After merging 2 nodes, ClassDistr: {348.3470446813698,0.0}
After merging 2 nodes, ClassDistr: {496.0,703.0}
After merging 2 nodes, ClassDistr: {158.0,994.0}
After merging 2 nodes, ClassDistr: {97.0,1111.0}
After merging 2 nodes, ClassDistr: {688.0,489.0}
After merging 2 nodes, ClassDistr: {359.0,846.0}
After merging 2 nodes, ClassDistr: {6164.0,1781.0}
After merging 2 nodes, ClassDistr: {3574.0,4479.0}
After merging 2 nodes, ClassDistr: {591.0,603.0}
After merging 2 nodes, ClassDistr: {1134.0,33.0}
After merging 2 nodes, ClassDistr: {216.0,1036.0}
After merging 2 nodes, ClassDistr: {742.0,469.0}
After merging 2 nodes, ClassDistr: {62.0,1146.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,5.0}
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          Leaf weight = {394.6529553186302,6373.913903662247}{106.0,614.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {348.3470446813698,0.0}{19.0,271.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {496.0,703.0}{106.0,109.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {158.0,994.0}{35.0,159.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {97.0,1111.0}{13.0,192.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {688.0,489.0}{98.0,97.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {359.0,846.0}{66.0,138.0}
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

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          Leaf weight = {394.6529553186302,6373.913903662247}{104.0,557.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {348.3470446813698,0.0}{15.0,281.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {496.0,703.0}{79.0,92.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {158.0,994.0}{19.0,166.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {97.0,1111.0}{18.0,171.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {688.0,489.0}{116.0,98.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {359.0,846.0}{69.0,116.0}
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

After merging 2 nodes, ClassDistr: {394.6529553186302,6373.913903662247}
After merging 2 nodes, ClassDistr: {348.3470446813698,0.0}
After merging 2 nodes, ClassDistr: {496.0,703.0}
After merging 2 nodes, ClassDistr: {158.0,994.0}
After merging 2 nodes, ClassDistr: {97.0,1111.0}
After merging 2 nodes, ClassDistr: {688.0,489.0}
After merging 2 nodes, ClassDistr: {359.0,846.0}
After merging 2 nodes, ClassDistr: {6164.0,1781.0}
After merging 2 nodes, ClassDistr: {3574.0,4479.0}
After merging 2 nodes, ClassDistr: {591.0,603.0}
After merging 2 nodes, ClassDistr: {1134.0,33.0}
After merging 2 nodes, ClassDistr: {216.0,1036.0}
After merging 2 nodes, ClassDistr: {742.0,469.0}
After merging 2 nodes, ClassDistr: {62.0,1146.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,5.0}
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          Leaf weight = {394.6529553186302,6373.913903662247}{210.0,1171.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {348.3470446813698,0.0}{34.0,552.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {496.0,703.0}{185.0,201.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {158.0,994.0}{54.0,325.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {97.0,1111.0}{31.0,363.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {688.0,489.0}{214.0,195.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {359.0,846.0}{135.0,254.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          Leaf weight = {394.6529553186302,6373.913903662247}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {348.3470446813698,0.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {496.0,703.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {158.0,994.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {97.0,1111.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {688.0,489.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {359.0,846.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          Leaf weight = {394.6529553186302,6373.913903662247}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {348.3470446813698,0.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {496.0,703.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {158.0,994.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {97.0,1111.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {688.0,489.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {359.0,846.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          Leaf weight = {394.6529553186302,6373.913903662247}{210.0,1171.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {348.3470446813698,0.0}{34.0,552.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {496.0,703.0}{185.0,201.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {158.0,994.0}{54.0,325.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {97.0,1111.0}{31.0,363.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {688.0,489.0}{214.0,195.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {359.0,846.0}{135.0,254.0}
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

After merging 2 nodes, ClassDistr: {604.6529553186301,7544.913903662247}
After merging 2 nodes, ClassDistr: {382.3470446813698,552.0}
After merging 2 nodes, ClassDistr: {681.0,904.0}
After merging 2 nodes, ClassDistr: {212.0,1319.0}
After merging 2 nodes, ClassDistr: {128.0,1474.0}
After merging 2 nodes, ClassDistr: {902.0,684.0}
After merging 2 nodes, ClassDistr: {494.0,1100.0}
After merging 2 nodes, ClassDistr: {7826.0,2182.0}
After merging 2 nodes, ClassDistr: {4470.0,5653.0}
After merging 2 nodes, ClassDistr: {773.0,793.0}
After merging 2 nodes, ClassDistr: {1527.0,45.0}
After merging 2 nodes, ClassDistr: {283.0,1400.0}
After merging 2 nodes, ClassDistr: {966.0,607.0}
After merging 2 nodes, ClassDistr: {81.0,1499.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,6.0}
HoeffdingTreeModel: addOnWeight = 1381.0
Best suggestion: FeatureSplit, merit=0.18773496165843606, NumericBinaryTest(false) feature[6] = 0.9999712170017597
Distribution From Split: class [0]= {42.85017581452138,1171.0}
Distribution From Split: class [1]= {167.14982418547862,0.0}
HoeffdingTreeModel: {22,16,16,11}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {42.85017581452138,1171.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {382.3470446813698,552.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {681.0,904.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {212.0,1319.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {128.0,1474.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {902.0,684.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {494.0,1100.0}{0.0,0.0}
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

Total instances: 41094.41599753274
Accuracy 0.744
FileReader RDD count:10000
==============================
Counter: 70000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {42.85017581452138,1171.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {382.3470446813698,552.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {681.0,904.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {212.0,1319.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {128.0,1474.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {902.0,684.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {494.0,1100.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {42.85017581452138,1171.0}{102.0,581.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {382.3470446813698,552.0}{25.0,267.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {681.0,904.0}{93.0,119.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {212.0,1319.0}{16.0,174.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {128.0,1474.0}{20.0,186.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {902.0,684.0}{126.0,82.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {494.0,1100.0}{78.0,121.0}
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

After merging 2 nodes, ClassDistr: {42.85017581452138,1171.0}
After merging 2 nodes, ClassDistr: {167.14982418547862,0.0}
After merging 2 nodes, ClassDistr: {382.3470446813698,552.0}
After merging 2 nodes, ClassDistr: {681.0,904.0}
After merging 2 nodes, ClassDistr: {212.0,1319.0}
After merging 2 nodes, ClassDistr: {128.0,1474.0}
After merging 2 nodes, ClassDistr: {902.0,684.0}
After merging 2 nodes, ClassDistr: {494.0,1100.0}
After merging 2 nodes, ClassDistr: {7826.0,2182.0}
After merging 2 nodes, ClassDistr: {4470.0,5653.0}
After merging 2 nodes, ClassDistr: {773.0,793.0}
After merging 2 nodes, ClassDistr: {1527.0,45.0}
After merging 2 nodes, ClassDistr: {283.0,1400.0}
After merging 2 nodes, ClassDistr: {966.0,607.0}
After merging 2 nodes, ClassDistr: {81.0,1499.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,6.0}
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {42.85017581452138,1171.0}{102.0,581.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {382.3470446813698,552.0}{25.0,267.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {681.0,904.0}{93.0,119.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {212.0,1319.0}{16.0,174.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {128.0,1474.0}{20.0,186.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {902.0,684.0}{126.0,82.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {494.0,1100.0}{78.0,121.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {42.85017581452138,1171.0}{117.0,604.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {382.3470446813698,552.0}{17.0,297.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {681.0,904.0}{71.0,106.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {212.0,1319.0}{29.0,204.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {128.0,1474.0}{13.0,191.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {902.0,684.0}{128.0,80.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {494.0,1100.0}{50.0,118.0}
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

After merging 2 nodes, ClassDistr: {42.85017581452138,1171.0}
After merging 2 nodes, ClassDistr: {167.14982418547862,0.0}
After merging 2 nodes, ClassDistr: {382.3470446813698,552.0}
After merging 2 nodes, ClassDistr: {681.0,904.0}
After merging 2 nodes, ClassDistr: {212.0,1319.0}
After merging 2 nodes, ClassDistr: {128.0,1474.0}
After merging 2 nodes, ClassDistr: {902.0,684.0}
After merging 2 nodes, ClassDistr: {494.0,1100.0}
After merging 2 nodes, ClassDistr: {7826.0,2182.0}
After merging 2 nodes, ClassDistr: {4470.0,5653.0}
After merging 2 nodes, ClassDistr: {773.0,793.0}
After merging 2 nodes, ClassDistr: {1527.0,45.0}
After merging 2 nodes, ClassDistr: {283.0,1400.0}
After merging 2 nodes, ClassDistr: {966.0,607.0}
After merging 2 nodes, ClassDistr: {81.0,1499.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,6.0}
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {42.85017581452138,1171.0}{219.0,1185.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {382.3470446813698,552.0}{42.0,564.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {681.0,904.0}{164.0,225.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {212.0,1319.0}{45.0,378.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {128.0,1474.0}{33.0,377.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {902.0,684.0}{254.0,162.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {494.0,1100.0}{128.0,239.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {42.85017581452138,1171.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {382.3470446813698,552.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {681.0,904.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {212.0,1319.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {128.0,1474.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {902.0,684.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {494.0,1100.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {42.85017581452138,1171.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {382.3470446813698,552.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {681.0,904.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {212.0,1319.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {128.0,1474.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {902.0,684.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {494.0,1100.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {42.85017581452138,1171.0}{219.0,1185.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {382.3470446813698,552.0}{42.0,564.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {681.0,904.0}{164.0,225.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {212.0,1319.0}{45.0,378.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {128.0,1474.0}{33.0,377.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {902.0,684.0}{254.0,162.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {494.0,1100.0}{128.0,239.0}
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

After merging 2 nodes, ClassDistr: {261.8501758145214,2356.0}
After merging 2 nodes, ClassDistr: {167.14982418547862,0.0}
After merging 2 nodes, ClassDistr: {424.3470446813698,1116.0}
After merging 2 nodes, ClassDistr: {845.0,1129.0}
After merging 2 nodes, ClassDistr: {257.0,1697.0}
After merging 2 nodes, ClassDistr: {161.0,1851.0}
After merging 2 nodes, ClassDistr: {1156.0,846.0}
After merging 2 nodes, ClassDistr: {622.0,1339.0}
After merging 2 nodes, ClassDistr: {9441.0,2610.0}
After merging 2 nodes, ClassDistr: {5360.0,6771.0}
After merging 2 nodes, ClassDistr: {963.0,982.0}
After merging 2 nodes, ClassDistr: {1939.0,53.0}
After merging 2 nodes, ClassDistr: {345.0,1702.0}
After merging 2 nodes, ClassDistr: {1200.0,777.0}
After merging 2 nodes, ClassDistr: {95.0,1852.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,6.0}
HoeffdingTreeModel: addOnWeight = 10032.0
Best suggestion: FeatureSplit, merit=0.37266093749602036, NumericBinaryTest(false) feature[6] = 0.9999148746584813
Distribution From Split: class [0]= {7860.0,793.0719014793361}
Distribution From Split: class [1]= {0.0,1378.9280985206638}
HoeffdingTreeModel: {24,17,17,11}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {261.8501758145214,2356.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {424.3470446813698,1116.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {845.0,1129.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {257.0,1697.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {161.0,1851.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1156.0,846.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {622.0,1339.0}{0.0,0.0}
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

Total instances: 49075.41599753274
Accuracy 0.745
FileReader RDD count:10000
==============================
Counter: 80000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {261.8501758145214,2356.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {424.3470446813698,1116.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {845.0,1129.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {257.0,1697.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {161.0,1851.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1156.0,846.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {622.0,1339.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {261.8501758145214,2356.0}{108.0,576.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {424.3470446813698,1116.0}{25.0,281.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {845.0,1129.0}{90.0,87.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {257.0,1697.0}{23.0,185.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {161.0,1851.0}{14.0,189.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1156.0,846.0}{114.0,93.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {622.0,1339.0}{65.0,119.0}
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

After merging 2 nodes, ClassDistr: {261.8501758145214,2356.0}
After merging 2 nodes, ClassDistr: {167.14982418547862,0.0}
After merging 2 nodes, ClassDistr: {424.3470446813698,1116.0}
After merging 2 nodes, ClassDistr: {845.0,1129.0}
After merging 2 nodes, ClassDistr: {257.0,1697.0}
After merging 2 nodes, ClassDistr: {161.0,1851.0}
After merging 2 nodes, ClassDistr: {1156.0,846.0}
After merging 2 nodes, ClassDistr: {622.0,1339.0}
After merging 2 nodes, ClassDistr: {7860.0,793.0719014793361}
After merging 2 nodes, ClassDistr: {0.0,1378.9280985206638}
After merging 2 nodes, ClassDistr: {5360.0,6771.0}
After merging 2 nodes, ClassDistr: {963.0,982.0}
After merging 2 nodes, ClassDistr: {1939.0,53.0}
After merging 2 nodes, ClassDistr: {345.0,1702.0}
After merging 2 nodes, ClassDistr: {1200.0,777.0}
After merging 2 nodes, ClassDistr: {95.0,1852.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,6.0}
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {261.8501758145214,2356.0}{108.0,576.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {424.3470446813698,1116.0}{25.0,281.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {845.0,1129.0}{90.0,87.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {257.0,1697.0}{23.0,185.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {161.0,1851.0}{14.0,189.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1156.0,846.0}{114.0,93.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {622.0,1339.0}{65.0,119.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {261.8501758145214,2356.0}{109.0,601.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {424.3470446813698,1116.0}{18.0,290.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {845.0,1129.0}{82.0,134.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {257.0,1697.0}{26.0,168.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {161.0,1851.0}{10.0,174.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1156.0,846.0}{119.0,87.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {622.0,1339.0}{70.0,151.0}
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

After merging 2 nodes, ClassDistr: {261.8501758145214,2356.0}
After merging 2 nodes, ClassDistr: {167.14982418547862,0.0}
After merging 2 nodes, ClassDistr: {424.3470446813698,1116.0}
After merging 2 nodes, ClassDistr: {845.0,1129.0}
After merging 2 nodes, ClassDistr: {257.0,1697.0}
After merging 2 nodes, ClassDistr: {161.0,1851.0}
After merging 2 nodes, ClassDistr: {1156.0,846.0}
After merging 2 nodes, ClassDistr: {622.0,1339.0}
After merging 2 nodes, ClassDistr: {7860.0,793.0719014793361}
After merging 2 nodes, ClassDistr: {0.0,1378.9280985206638}
After merging 2 nodes, ClassDistr: {5360.0,6771.0}
After merging 2 nodes, ClassDistr: {963.0,982.0}
After merging 2 nodes, ClassDistr: {1939.0,53.0}
After merging 2 nodes, ClassDistr: {345.0,1702.0}
After merging 2 nodes, ClassDistr: {1200.0,777.0}
After merging 2 nodes, ClassDistr: {95.0,1852.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,6.0}
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {261.8501758145214,2356.0}{217.0,1177.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {424.3470446813698,1116.0}{43.0,571.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {845.0,1129.0}{172.0,221.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {257.0,1697.0}{49.0,353.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {161.0,1851.0}{24.0,363.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1156.0,846.0}{233.0,180.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {622.0,1339.0}{135.0,270.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {261.8501758145214,2356.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {424.3470446813698,1116.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {845.0,1129.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {257.0,1697.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {161.0,1851.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1156.0,846.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {622.0,1339.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {261.8501758145214,2356.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {424.3470446813698,1116.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {845.0,1129.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {257.0,1697.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {161.0,1851.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1156.0,846.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {622.0,1339.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {261.8501758145214,2356.0}{217.0,1177.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {424.3470446813698,1116.0}{43.0,571.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {845.0,1129.0}{172.0,221.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {257.0,1697.0}{49.0,353.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {161.0,1851.0}{24.0,363.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1156.0,846.0}{233.0,180.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {622.0,1339.0}{135.0,270.0}
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

After merging 2 nodes, ClassDistr: {478.8501758145214,3533.0}
After merging 2 nodes, ClassDistr: {167.14982418547862,0.0}
After merging 2 nodes, ClassDistr: {467.3470446813698,1687.0}
After merging 2 nodes, ClassDistr: {1017.0,1350.0}
After merging 2 nodes, ClassDistr: {306.0,2050.0}
After merging 2 nodes, ClassDistr: {185.0,2214.0}
After merging 2 nodes, ClassDistr: {1389.0,1026.0}
After merging 2 nodes, ClassDistr: {757.0,1609.0}
After merging 2 nodes, ClassDistr: {9446.0,1232.0719014793362}
After merging 2 nodes, ClassDistr: {0.0,1378.9280985206638}
After merging 2 nodes, ClassDistr: {6196.0,7862.0}
After merging 2 nodes, ClassDistr: {1154.0,1180.0}
After merging 2 nodes, ClassDistr: {2336.0,61.0}
After merging 2 nodes, ClassDistr: {399.0,2052.0}
After merging 2 nodes, ClassDistr: {1446.0,971.0}
After merging 2 nodes, ClassDistr: {124.0,2224.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,7.0}
HoeffdingTreeModel: addOnWeight = 12034.0
Best suggestion: FeatureSplit, merit=0.10406844011933714, NominalMultiwayTestfeature[2] 5
Distribution From Split: class [0]= {464.0,1965.0}
Distribution From Split: class [1]= {880.0,1503.0}
Distribution From Split: class [2]= {1278.0,1102.0}
Distribution From Split: class [3]= {1784.0,636.0}
Distribution From Split: class [4]= {899.0,1523.0}
HoeffdingTreeModel: {29,21,21,11}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {478.8501758145214,3533.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {467.3470446813698,1687.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1017.0,1350.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {306.0,2050.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {185.0,2214.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1389.0,1026.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {757.0,1609.0}{0.0,0.0}
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

Total instances: 57051.41599753274
Accuracy 0.765
FileReader RDD count:10000
==============================
Counter: 90000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {478.8501758145214,3533.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {467.3470446813698,1687.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1017.0,1350.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {306.0,2050.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {185.0,2214.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1389.0,1026.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {757.0,1609.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {478.8501758145214,3533.0}{107.0,597.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {467.3470446813698,1687.0}{18.0,271.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1017.0,1350.0}{84.0,95.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {306.0,2050.0}{28.0,178.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {185.0,2214.0}{17.0,177.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1389.0,1026.0}{125.0,83.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {757.0,1609.0}{82.0,132.0}
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

After merging 2 nodes, ClassDistr: {478.8501758145214,3533.0}
After merging 2 nodes, ClassDistr: {167.14982418547862,0.0}
After merging 2 nodes, ClassDistr: {467.3470446813698,1687.0}
After merging 2 nodes, ClassDistr: {1017.0,1350.0}
After merging 2 nodes, ClassDistr: {306.0,2050.0}
After merging 2 nodes, ClassDistr: {185.0,2214.0}
After merging 2 nodes, ClassDistr: {1389.0,1026.0}
After merging 2 nodes, ClassDistr: {757.0,1609.0}
After merging 2 nodes, ClassDistr: {9446.0,1232.0719014793362}
After merging 2 nodes, ClassDistr: {0.0,1378.9280985206638}
After merging 2 nodes, ClassDistr: {464.0,1965.0}
After merging 2 nodes, ClassDistr: {880.0,1503.0}
After merging 2 nodes, ClassDistr: {1278.0,1102.0}
After merging 2 nodes, ClassDistr: {1784.0,636.0}
After merging 2 nodes, ClassDistr: {899.0,1523.0}
After merging 2 nodes, ClassDistr: {1154.0,1180.0}
After merging 2 nodes, ClassDistr: {2336.0,61.0}
After merging 2 nodes, ClassDistr: {399.0,2052.0}
After merging 2 nodes, ClassDistr: {1446.0,971.0}
After merging 2 nodes, ClassDistr: {124.0,2224.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,7.0}
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {478.8501758145214,3533.0}{107.0,597.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {467.3470446813698,1687.0}{18.0,271.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1017.0,1350.0}{84.0,95.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {306.0,2050.0}{28.0,178.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {185.0,2214.0}{17.0,177.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1389.0,1026.0}{125.0,83.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {757.0,1609.0}{82.0,132.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {478.8501758145214,3533.0}{107.0,597.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,1.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {467.3470446813698,1687.0}{20.0,277.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1017.0,1350.0}{93.0,116.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {306.0,2050.0}{20.0,187.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {185.0,2214.0}{12.0,174.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1389.0,1026.0}{112.0,89.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {757.0,1609.0}{64.0,148.0}
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

After merging 2 nodes, ClassDistr: {478.8501758145214,3533.0}
After merging 2 nodes, ClassDistr: {167.14982418547862,0.0}
After merging 2 nodes, ClassDistr: {467.3470446813698,1687.0}
After merging 2 nodes, ClassDistr: {1017.0,1350.0}
After merging 2 nodes, ClassDistr: {306.0,2050.0}
After merging 2 nodes, ClassDistr: {185.0,2214.0}
After merging 2 nodes, ClassDistr: {1389.0,1026.0}
After merging 2 nodes, ClassDistr: {757.0,1609.0}
After merging 2 nodes, ClassDistr: {9446.0,1232.0719014793362}
After merging 2 nodes, ClassDistr: {0.0,1378.9280985206638}
After merging 2 nodes, ClassDistr: {464.0,1965.0}
After merging 2 nodes, ClassDistr: {880.0,1503.0}
After merging 2 nodes, ClassDistr: {1278.0,1102.0}
After merging 2 nodes, ClassDistr: {1784.0,636.0}
After merging 2 nodes, ClassDistr: {899.0,1523.0}
After merging 2 nodes, ClassDistr: {1154.0,1180.0}
After merging 2 nodes, ClassDistr: {2336.0,61.0}
After merging 2 nodes, ClassDistr: {399.0,2052.0}
After merging 2 nodes, ClassDistr: {1446.0,971.0}
After merging 2 nodes, ClassDistr: {124.0,2224.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,7.0}
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {478.8501758145214,3533.0}{214.0,1194.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,1.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {467.3470446813698,1687.0}{38.0,548.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1017.0,1350.0}{177.0,211.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {306.0,2050.0}{48.0,365.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {185.0,2214.0}{29.0,351.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1389.0,1026.0}{237.0,172.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {757.0,1609.0}{146.0,280.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {478.8501758145214,3533.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {467.3470446813698,1687.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1017.0,1350.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {306.0,2050.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {185.0,2214.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1389.0,1026.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {757.0,1609.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {478.8501758145214,3533.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {467.3470446813698,1687.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1017.0,1350.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {306.0,2050.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {185.0,2214.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1389.0,1026.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {757.0,1609.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {478.8501758145214,3533.0}{214.0,1194.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,0.0}{0.0,1.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {467.3470446813698,1687.0}{38.0,548.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1017.0,1350.0}{177.0,211.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {306.0,2050.0}{48.0,365.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {185.0,2214.0}{29.0,351.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1389.0,1026.0}{237.0,172.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {757.0,1609.0}{146.0,280.0}
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

After merging 2 nodes, ClassDistr: {692.8501758145214,4727.0}
After merging 2 nodes, ClassDistr: {167.14982418547862,1.0}
After merging 2 nodes, ClassDistr: {505.3470446813698,2235.0}
After merging 2 nodes, ClassDistr: {1194.0,1561.0}
After merging 2 nodes, ClassDistr: {354.0,2415.0}
After merging 2 nodes, ClassDistr: {214.0,2565.0}
After merging 2 nodes, ClassDistr: {1626.0,1198.0}
After merging 2 nodes, ClassDistr: {903.0,1889.0}
After merging 2 nodes, ClassDistr: {11025.0,1692.0719014793362}
After merging 2 nodes, ClassDistr: {1.0,1378.9280985206638}
After merging 2 nodes, ClassDistr: {524.0,2292.0}
After merging 2 nodes, ClassDistr: {1038.0,1771.0}
After merging 2 nodes, ClassDistr: {1490.0,1261.0}
After merging 2 nodes, ClassDistr: {2075.0,740.0}
After merging 2 nodes, ClassDistr: {1045.0,1775.0}
After merging 2 nodes, ClassDistr: {1351.0,1389.0}
After merging 2 nodes, ClassDistr: {2704.0,76.0}
After merging 2 nodes, ClassDistr: {454.0,2390.0}
After merging 2 nodes, ClassDistr: {1678.0,1126.0}
After merging 2 nodes, ClassDistr: {140.0,2611.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,7.0}
HoeffdingTreeModel: addOnWeight = 2357.0
HoeffdingTreeModel: {29,21,21,11}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {692.8501758145214,4727.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,1.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {505.3470446813698,2235.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1194.0,1561.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {354.0,2415.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {214.0,2565.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1626.0,1198.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {903.0,1889.0}{0.0,0.0}
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

Total instances: 67051.41599753273
Accuracy 0.765
FileReader RDD count:10000
==============================
Counter: 100000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {692.8501758145214,4727.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,1.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {505.3470446813698,2235.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1194.0,1561.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {354.0,2415.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {214.0,2565.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1626.0,1198.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {903.0,1889.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {692.8501758145214,4727.0}{113.0,577.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,1.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {505.3470446813698,2235.0}{18.0,282.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1194.0,1561.0}{86.0,101.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {354.0,2415.0}{27.0,166.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {214.0,2565.0}{8.0,190.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1626.0,1198.0}{121.0,61.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {903.0,1889.0}{73.0,132.0}
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

After merging 2 nodes, ClassDistr: {692.8501758145214,4727.0}
After merging 2 nodes, ClassDistr: {167.14982418547862,1.0}
After merging 2 nodes, ClassDistr: {505.3470446813698,2235.0}
After merging 2 nodes, ClassDistr: {1194.0,1561.0}
After merging 2 nodes, ClassDistr: {354.0,2415.0}
After merging 2 nodes, ClassDistr: {214.0,2565.0}
After merging 2 nodes, ClassDistr: {1626.0,1198.0}
After merging 2 nodes, ClassDistr: {903.0,1889.0}
After merging 2 nodes, ClassDistr: {11025.0,1692.0719014793362}
After merging 2 nodes, ClassDistr: {1.0,1378.9280985206638}
After merging 2 nodes, ClassDistr: {524.0,2292.0}
After merging 2 nodes, ClassDistr: {1038.0,1771.0}
After merging 2 nodes, ClassDistr: {1490.0,1261.0}
After merging 2 nodes, ClassDistr: {2075.0,740.0}
After merging 2 nodes, ClassDistr: {1045.0,1775.0}
After merging 2 nodes, ClassDistr: {1351.0,1389.0}
After merging 2 nodes, ClassDistr: {2704.0,76.0}
After merging 2 nodes, ClassDistr: {454.0,2390.0}
After merging 2 nodes, ClassDistr: {1678.0,1126.0}
After merging 2 nodes, ClassDistr: {140.0,2611.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,7.0}
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {692.8501758145214,4727.0}{113.0,577.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,1.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {505.3470446813698,2235.0}{18.0,282.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1194.0,1561.0}{86.0,101.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {354.0,2415.0}{27.0,166.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {214.0,2565.0}{8.0,190.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1626.0,1198.0}{121.0,61.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {903.0,1889.0}{73.0,132.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {692.8501758145214,4727.0}{101.0,618.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,1.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {505.3470446813698,2235.0}{21.0,274.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1194.0,1561.0}{66.0,113.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {354.0,2415.0}{23.0,170.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {214.0,2565.0}{19.0,170.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1626.0,1198.0}{110.0,86.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {903.0,1889.0}{56.0,142.0}
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

After merging 2 nodes, ClassDistr: {692.8501758145214,4727.0}
After merging 2 nodes, ClassDistr: {167.14982418547862,1.0}
After merging 2 nodes, ClassDistr: {505.3470446813698,2235.0}
After merging 2 nodes, ClassDistr: {1194.0,1561.0}
After merging 2 nodes, ClassDistr: {354.0,2415.0}
After merging 2 nodes, ClassDistr: {214.0,2565.0}
After merging 2 nodes, ClassDistr: {1626.0,1198.0}
After merging 2 nodes, ClassDistr: {903.0,1889.0}
After merging 2 nodes, ClassDistr: {11025.0,1692.0719014793362}
After merging 2 nodes, ClassDistr: {1.0,1378.9280985206638}
After merging 2 nodes, ClassDistr: {524.0,2292.0}
After merging 2 nodes, ClassDistr: {1038.0,1771.0}
After merging 2 nodes, ClassDistr: {1490.0,1261.0}
After merging 2 nodes, ClassDistr: {2075.0,740.0}
After merging 2 nodes, ClassDistr: {1045.0,1775.0}
After merging 2 nodes, ClassDistr: {1351.0,1389.0}
After merging 2 nodes, ClassDistr: {2704.0,76.0}
After merging 2 nodes, ClassDistr: {454.0,2390.0}
After merging 2 nodes, ClassDistr: {1678.0,1126.0}
After merging 2 nodes, ClassDistr: {140.0,2611.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,7.0}
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {692.8501758145214,4727.0}{214.0,1195.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,1.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {505.3470446813698,2235.0}{39.0,556.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1194.0,1561.0}{152.0,214.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {354.0,2415.0}{50.0,336.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {214.0,2565.0}{27.0,360.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1626.0,1198.0}{231.0,147.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {903.0,1889.0}{129.0,274.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {692.8501758145214,4727.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,1.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {505.3470446813698,2235.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1194.0,1561.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {354.0,2415.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {214.0,2565.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1626.0,1198.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {903.0,1889.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {692.8501758145214,4727.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,1.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {505.3470446813698,2235.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1194.0,1561.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {354.0,2415.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {214.0,2565.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1626.0,1198.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {903.0,1889.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {692.8501758145214,4727.0}{214.0,1195.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,1.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {505.3470446813698,2235.0}{39.0,556.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1194.0,1561.0}{152.0,214.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {354.0,2415.0}{50.0,336.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {214.0,2565.0}{27.0,360.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1626.0,1198.0}{231.0,147.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {903.0,1889.0}{129.0,274.0}
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

After merging 2 nodes, ClassDistr: {906.8501758145214,5922.0}
After merging 2 nodes, ClassDistr: {167.14982418547862,1.0}
After merging 2 nodes, ClassDistr: {544.3470446813699,2791.0}
After merging 2 nodes, ClassDistr: {1346.0,1775.0}
After merging 2 nodes, ClassDistr: {404.0,2751.0}
After merging 2 nodes, ClassDistr: {241.0,2925.0}
After merging 2 nodes, ClassDistr: {1857.0,1345.0}
After merging 2 nodes, ClassDistr: {1032.0,2163.0}
After merging 2 nodes, ClassDistr: {12636.0,2167.0719014793362}
After merging 2 nodes, ClassDistr: {2.0,1378.9280985206638}
After merging 2 nodes, ClassDistr: {607.0,2610.0}
After merging 2 nodes, ClassDistr: {1191.0,2017.0}
After merging 2 nodes, ClassDistr: {1727.0,1439.0}
After merging 2 nodes, ClassDistr: {2342.0,837.0}
After merging 2 nodes, ClassDistr: {1180.0,2023.0}
After merging 2 nodes, ClassDistr: {1553.0,1597.0}
After merging 2 nodes, ClassDistr: {3082.0,87.0}
After merging 2 nodes, ClassDistr: {518.0,2715.0}
After merging 2 nodes, ClassDistr: {1944.0,1291.0}
After merging 2 nodes, ClassDistr: {163.0,2996.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,7.0}
HoeffdingTreeModel: addOnWeight = 6150.0
Best suggestion: FeatureSplit, merit=0.00875964973992982, NumericBinaryTest(false) feature[9] = 0.6999897484377254
Distribution From Split: class [0]= {5768.489959646972,1050.9282431764977}
Distribution From Split: class [1]= {0.0,323.0717568235022}
HoeffdingTreeModel: {31,22,22,11}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6998122671377988
          
       if [feature 6 numeric 0] < 0.9999712170017597
              Leaf weight = {906.8501758145214,5922.0}{0.0,0.0}
       if [feature 6 numeric 1] >= 0.9999712170017597
              Leaf weight = {167.14982418547862,1.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6998122671377988
          Leaf weight = {544.3470446813699,2791.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {1346.0,1775.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {404.0,2751.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {241.0,2925.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1857.0,1345.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {1032.0,2163.0}{0.0,0.0}
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

Total instances: 69390.83405570037
Accuracy 0.723
FileReader RDD count:10000
==============================
Counter: 110000
Chunk: 10000
Over 100.000 instances
Running time = 106.845848169
Reading file stopped.
