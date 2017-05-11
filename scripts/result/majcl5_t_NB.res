../data/randomtreedata.arff
Learning Node Type: 1
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

After merging 2 nodes, ClassDistr: {1328.9310471486347,5904.0}
After merging 2 nodes, ClassDistr: {2767.0689528513653,0.0}
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
Accuracy 0.745
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
Accuracy 0.730
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
This root: 
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
HoeffdingTreeModel: addOnWeight = 6029.0
Best suggestion: FeatureSplit, merit=0.11095576467304036, NominalMultiwayTestfeature[2] 5
Distribution From Split: class [0]= {233.0,1007.0}
Distribution From Split: class [1]= {444.0,761.0}
Distribution From Split: class [2]= {623.0,530.0}
Distribution From Split: class [3]= {927.0,309.0}
Distribution From Split: class [4]= {456.0,739.0}
HoeffdingTreeModel: {23,18,18,11}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {1001.0,7063.0}{0.0,0.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {233.0,1007.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {444.0,761.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {623.0,530.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {927.0,309.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {456.0,739.0}{0.0,0.0}
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

Total instances: 36786.06895285137
Accuracy 0.795
FileReader RDD count:10000
==============================
Counter: 60000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {1001.0,7063.0}{0.0,0.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {233.0,1007.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {444.0,761.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {623.0,530.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {927.0,309.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {456.0,739.0}{0.0,0.0}
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
      Leaf weight = {1001.0,7063.0}{119.0,838.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {233.0,1007.0}{30.0,173.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {444.0,761.0}{65.0,135.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {623.0,530.0}{115.0,98.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {927.0,309.0}{160.0,64.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {456.0,739.0}{70.0,130.0}
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

After merging 2 nodes, ClassDistr: {1001.0,7063.0}
After merging 2 nodes, ClassDistr: {496.0,703.0}
After merging 2 nodes, ClassDistr: {158.0,994.0}
After merging 2 nodes, ClassDistr: {97.0,1111.0}
After merging 2 nodes, ClassDistr: {688.0,489.0}
After merging 2 nodes, ClassDistr: {359.0,846.0}
After merging 2 nodes, ClassDistr: {6164.0,1781.0}
After merging 2 nodes, ClassDistr: {233.0,1007.0}
After merging 2 nodes, ClassDistr: {444.0,761.0}
After merging 2 nodes, ClassDistr: {623.0,530.0}
After merging 2 nodes, ClassDistr: {927.0,309.0}
After merging 2 nodes, ClassDistr: {456.0,739.0}
After merging 2 nodes, ClassDistr: {591.0,603.0}
After merging 2 nodes, ClassDistr: {1134.0,33.0}
After merging 2 nodes, ClassDistr: {216.0,1036.0}
After merging 2 nodes, ClassDistr: {742.0,469.0}
After merging 2 nodes, ClassDistr: {62.0,1146.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,5.0}
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {1001.0,7063.0}{119.0,838.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {233.0,1007.0}{30.0,173.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {444.0,761.0}{65.0,135.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {623.0,530.0}{115.0,98.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {927.0,309.0}{160.0,64.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {456.0,739.0}{70.0,130.0}
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
      Leaf weight = {1001.0,7063.0}{125.0,885.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {233.0,1007.0}{33.0,163.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {444.0,761.0}{89.0,122.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {623.0,530.0}{115.0,98.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {927.0,309.0}{156.0,63.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {456.0,739.0}{63.0,128.0}
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

After merging 2 nodes, ClassDistr: {1001.0,7063.0}
After merging 2 nodes, ClassDistr: {496.0,703.0}
After merging 2 nodes, ClassDistr: {158.0,994.0}
After merging 2 nodes, ClassDistr: {97.0,1111.0}
After merging 2 nodes, ClassDistr: {688.0,489.0}
After merging 2 nodes, ClassDistr: {359.0,846.0}
After merging 2 nodes, ClassDistr: {6164.0,1781.0}
After merging 2 nodes, ClassDistr: {233.0,1007.0}
After merging 2 nodes, ClassDistr: {444.0,761.0}
After merging 2 nodes, ClassDistr: {623.0,530.0}
After merging 2 nodes, ClassDistr: {927.0,309.0}
After merging 2 nodes, ClassDistr: {456.0,739.0}
After merging 2 nodes, ClassDistr: {591.0,603.0}
After merging 2 nodes, ClassDistr: {1134.0,33.0}
After merging 2 nodes, ClassDistr: {216.0,1036.0}
After merging 2 nodes, ClassDistr: {742.0,469.0}
After merging 2 nodes, ClassDistr: {62.0,1146.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,5.0}
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {1001.0,7063.0}{244.0,1723.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {233.0,1007.0}{63.0,336.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {444.0,761.0}{154.0,257.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {623.0,530.0}{230.0,196.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {927.0,309.0}{316.0,127.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {456.0,739.0}{133.0,258.0}
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
      Leaf weight = {1001.0,7063.0}{0.0,0.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {233.0,1007.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {444.0,761.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {623.0,530.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {927.0,309.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {456.0,739.0}{0.0,0.0}
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
      Leaf weight = {1001.0,7063.0}{0.0,0.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {233.0,1007.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {444.0,761.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {623.0,530.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {927.0,309.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {456.0,739.0}{0.0,0.0}
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
      Leaf weight = {1001.0,7063.0}{244.0,1723.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {233.0,1007.0}{63.0,336.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {444.0,761.0}{154.0,257.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {623.0,530.0}{230.0,196.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {927.0,309.0}{316.0,127.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {456.0,739.0}{133.0,258.0}
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

After merging 2 nodes, ClassDistr: {1245.0,8786.0}
After merging 2 nodes, ClassDistr: {681.0,904.0}
After merging 2 nodes, ClassDistr: {212.0,1319.0}
After merging 2 nodes, ClassDistr: {128.0,1474.0}
After merging 2 nodes, ClassDistr: {902.0,684.0}
After merging 2 nodes, ClassDistr: {494.0,1100.0}
After merging 2 nodes, ClassDistr: {7826.0,2182.0}
After merging 2 nodes, ClassDistr: {296.0,1343.0}
After merging 2 nodes, ClassDistr: {598.0,1018.0}
After merging 2 nodes, ClassDistr: {853.0,726.0}
After merging 2 nodes, ClassDistr: {1243.0,436.0}
After merging 2 nodes, ClassDistr: {589.0,997.0}
After merging 2 nodes, ClassDistr: {773.0,793.0}
After merging 2 nodes, ClassDistr: {1527.0,45.0}
After merging 2 nodes, ClassDistr: {283.0,1400.0}
After merging 2 nodes, ClassDistr: {966.0,607.0}
After merging 2 nodes, ClassDistr: {81.0,1499.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,6.0}
HoeffdingTreeModel: addOnWeight = 8033.0
Best suggestion: FeatureSplit, merit=0.23744615578319045, NumericBinaryTest(false) feature[8] = 0.6999404200192979
Distribution From Split: class [0]= {518.6887524421978,8421.119134555689}
Distribution From Split: class [1]= {468.3112475578022,0.0}
HoeffdingTreeModel: {25,19,19,11}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {518.6887524421978,8421.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {468.3112475578022,0.0}{0.0,0.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {296.0,1343.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {598.0,1018.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {853.0,726.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1243.0,436.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {589.0,997.0}{0.0,0.0}
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

Total instances: 46163.188087407056
Accuracy 0.730
FileReader RDD count:10000
==============================
Counter: 70000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {518.6887524421978,8421.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {468.3112475578022,0.0}{0.0,0.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {296.0,1343.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {598.0,1018.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {853.0,726.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1243.0,436.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {589.0,997.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {518.6887524421978,8421.119134555689}{117.0,604.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {468.3112475578022,0.0}{17.0,297.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {296.0,1343.0}{39.0,148.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {598.0,1018.0}{65.0,130.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {853.0,726.0}{117.0,109.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1243.0,436.0}{115.0,58.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {589.0,997.0}{98.0,123.0}
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

After merging 2 nodes, ClassDistr: {518.6887524421978,8421.119134555689}
After merging 2 nodes, ClassDistr: {468.3112475578022,0.0}
After merging 2 nodes, ClassDistr: {681.0,904.0}
After merging 2 nodes, ClassDistr: {212.0,1319.0}
After merging 2 nodes, ClassDistr: {128.0,1474.0}
After merging 2 nodes, ClassDistr: {902.0,684.0}
After merging 2 nodes, ClassDistr: {494.0,1100.0}
After merging 2 nodes, ClassDistr: {7826.0,2182.0}
After merging 2 nodes, ClassDistr: {296.0,1343.0}
After merging 2 nodes, ClassDistr: {598.0,1018.0}
After merging 2 nodes, ClassDistr: {853.0,726.0}
After merging 2 nodes, ClassDistr: {1243.0,436.0}
After merging 2 nodes, ClassDistr: {589.0,997.0}
After merging 2 nodes, ClassDistr: {773.0,793.0}
After merging 2 nodes, ClassDistr: {1527.0,45.0}
After merging 2 nodes, ClassDistr: {283.0,1400.0}
After merging 2 nodes, ClassDistr: {966.0,607.0}
After merging 2 nodes, ClassDistr: {81.0,1499.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,6.0}
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {518.6887524421978,8421.119134555689}{117.0,604.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {468.3112475578022,0.0}{17.0,297.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {296.0,1343.0}{39.0,148.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {598.0,1018.0}{65.0,130.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {853.0,726.0}{117.0,109.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1243.0,436.0}{115.0,58.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {589.0,997.0}{98.0,123.0}
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

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {518.6887524421978,8421.119134555689}{102.0,581.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {468.3112475578022,0.0}{25.0,267.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {296.0,1343.0}{54.0,173.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {598.0,1018.0}{76.0,115.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {853.0,726.0}{108.0,80.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1243.0,436.0}{138.0,61.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {589.0,997.0}{80.0,121.0}
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

After merging 2 nodes, ClassDistr: {518.6887524421978,8421.119134555689}
After merging 2 nodes, ClassDistr: {468.3112475578022,0.0}
After merging 2 nodes, ClassDistr: {681.0,904.0}
After merging 2 nodes, ClassDistr: {212.0,1319.0}
After merging 2 nodes, ClassDistr: {128.0,1474.0}
After merging 2 nodes, ClassDistr: {902.0,684.0}
After merging 2 nodes, ClassDistr: {494.0,1100.0}
After merging 2 nodes, ClassDistr: {7826.0,2182.0}
After merging 2 nodes, ClassDistr: {296.0,1343.0}
After merging 2 nodes, ClassDistr: {598.0,1018.0}
After merging 2 nodes, ClassDistr: {853.0,726.0}
After merging 2 nodes, ClassDistr: {1243.0,436.0}
After merging 2 nodes, ClassDistr: {589.0,997.0}
After merging 2 nodes, ClassDistr: {773.0,793.0}
After merging 2 nodes, ClassDistr: {1527.0,45.0}
After merging 2 nodes, ClassDistr: {283.0,1400.0}
After merging 2 nodes, ClassDistr: {966.0,607.0}
After merging 2 nodes, ClassDistr: {81.0,1499.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,6.0}
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {518.6887524421978,8421.119134555689}{219.0,1185.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {468.3112475578022,0.0}{42.0,564.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {296.0,1343.0}{93.0,321.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {598.0,1018.0}{141.0,245.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {853.0,726.0}{225.0,189.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1243.0,436.0}{253.0,119.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {589.0,997.0}{178.0,244.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {518.6887524421978,8421.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {468.3112475578022,0.0}{0.0,0.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {296.0,1343.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {598.0,1018.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {853.0,726.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1243.0,436.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {589.0,997.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {518.6887524421978,8421.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {468.3112475578022,0.0}{0.0,0.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {296.0,1343.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {598.0,1018.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {853.0,726.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1243.0,436.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {589.0,997.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {518.6887524421978,8421.119134555689}{219.0,1185.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {468.3112475578022,0.0}{42.0,564.0}
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
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {296.0,1343.0}{93.0,321.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {598.0,1018.0}{141.0,245.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {853.0,726.0}{225.0,189.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1243.0,436.0}{253.0,119.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {589.0,997.0}{178.0,244.0}
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

After merging 2 nodes, ClassDistr: {737.6887524421978,9606.119134555689}
After merging 2 nodes, ClassDistr: {510.3112475578022,564.0}
After merging 2 nodes, ClassDistr: {845.0,1129.0}
After merging 2 nodes, ClassDistr: {257.0,1697.0}
After merging 2 nodes, ClassDistr: {161.0,1851.0}
After merging 2 nodes, ClassDistr: {1156.0,846.0}
After merging 2 nodes, ClassDistr: {622.0,1339.0}
After merging 2 nodes, ClassDistr: {9441.0,2610.0}
After merging 2 nodes, ClassDistr: {389.0,1664.0}
After merging 2 nodes, ClassDistr: {739.0,1263.0}
After merging 2 nodes, ClassDistr: {1078.0,915.0}
After merging 2 nodes, ClassDistr: {1496.0,555.0}
After merging 2 nodes, ClassDistr: {767.0,1241.0}
After merging 2 nodes, ClassDistr: {963.0,982.0}
After merging 2 nodes, ClassDistr: {1939.0,53.0}
After merging 2 nodes, ClassDistr: {345.0,1702.0}
After merging 2 nodes, ClassDistr: {1200.0,777.0}
After merging 2 nodes, ClassDistr: {95.0,1852.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,6.0}
HoeffdingTreeModel: addOnWeight = 1546.0
Best suggestion: FeatureSplit, merit=0.4811972157232218, NominalMultiwayTestfeature[0] 5
Distribution From Split: class [0]= {228.0,70.0}
Distribution From Split: class [1]= {236.0,67.0}
Distribution From Split: class [2]= {273.0,43.0}
Distribution From Split: class [3]= {10.0,300.0}
Distribution From Split: class [4]= {16.0,303.0}
HoeffdingTreeModel: {30,23,23,13}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {737.6887524421978,9606.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {510.3112475578022,564.0}{0.0,0.0}
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
      Leaf weight = {9441.0,2610.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {389.0,1664.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {739.0,1263.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1078.0,915.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1496.0,555.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {767.0,1241.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {228.0,70.0}{0.0,0.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {236.0,67.0}{0.0,0.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {273.0,43.0}{0.0,0.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {10.0,300.0}{0.0,0.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {16.0,303.0}{0.0,0.0}
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

Total instances: 55764.188087407056
Accuracy 0.846
FileReader RDD count:10000
==============================
Counter: 80000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {737.6887524421978,9606.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {510.3112475578022,564.0}{0.0,0.0}
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
      Leaf weight = {9441.0,2610.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {389.0,1664.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {739.0,1263.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1078.0,915.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1496.0,555.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {767.0,1241.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {228.0,70.0}{0.0,0.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {236.0,67.0}{0.0,0.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {273.0,43.0}{0.0,0.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {10.0,300.0}{0.0,0.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {16.0,303.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {737.6887524421978,9606.119134555689}{109.0,601.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {510.3112475578022,564.0}{18.0,290.0}
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
      Leaf weight = {9441.0,2610.0}{790.0,225.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {389.0,1664.0}{38.0,143.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {739.0,1263.0}{68.0,114.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1078.0,915.0}{100.0,94.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1496.0,555.0}{140.0,33.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {767.0,1241.0}{59.0,129.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {228.0,70.0}{32.0,9.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {236.0,67.0}{24.0,7.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {273.0,43.0}{35.0,7.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {10.0,300.0}{3.0,34.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {16.0,303.0}{4.0,46.0}
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

After merging 2 nodes, ClassDistr: {737.6887524421978,9606.119134555689}
After merging 2 nodes, ClassDistr: {510.3112475578022,564.0}
After merging 2 nodes, ClassDistr: {845.0,1129.0}
After merging 2 nodes, ClassDistr: {257.0,1697.0}
After merging 2 nodes, ClassDistr: {161.0,1851.0}
After merging 2 nodes, ClassDistr: {1156.0,846.0}
After merging 2 nodes, ClassDistr: {622.0,1339.0}
After merging 2 nodes, ClassDistr: {9441.0,2610.0}
After merging 2 nodes, ClassDistr: {389.0,1664.0}
After merging 2 nodes, ClassDistr: {739.0,1263.0}
After merging 2 nodes, ClassDistr: {1078.0,915.0}
After merging 2 nodes, ClassDistr: {1496.0,555.0}
After merging 2 nodes, ClassDistr: {767.0,1241.0}
After merging 2 nodes, ClassDistr: {228.0,70.0}
After merging 2 nodes, ClassDistr: {236.0,67.0}
After merging 2 nodes, ClassDistr: {273.0,43.0}
After merging 2 nodes, ClassDistr: {10.0,300.0}
After merging 2 nodes, ClassDistr: {16.0,303.0}
After merging 2 nodes, ClassDistr: {1939.0,53.0}
After merging 2 nodes, ClassDistr: {345.0,1702.0}
After merging 2 nodes, ClassDistr: {1200.0,777.0}
After merging 2 nodes, ClassDistr: {95.0,1852.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,6.0}
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {737.6887524421978,9606.119134555689}{109.0,601.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {510.3112475578022,564.0}{18.0,290.0}
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
      Leaf weight = {9441.0,2610.0}{790.0,225.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {389.0,1664.0}{38.0,143.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {739.0,1263.0}{68.0,114.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1078.0,915.0}{100.0,94.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1496.0,555.0}{140.0,33.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {767.0,1241.0}{59.0,129.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {228.0,70.0}{32.0,9.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {236.0,67.0}{24.0,7.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {273.0,43.0}{35.0,7.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {10.0,300.0}{3.0,34.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {16.0,303.0}{4.0,46.0}
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

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {737.6887524421978,9606.119134555689}{108.0,576.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {510.3112475578022,564.0}{25.0,281.0}
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
      Leaf weight = {9441.0,2610.0}{796.0,214.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {389.0,1664.0}{37.0,158.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {739.0,1263.0}{73.0,126.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1078.0,915.0}{100.0,93.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1496.0,555.0}{148.0,48.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {767.0,1241.0}{73.0,153.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {228.0,70.0}{37.0,7.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {236.0,67.0}{27.0,10.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {273.0,43.0}{25.0,6.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {10.0,300.0}{1.0,41.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {16.0,303.0}{3.0,31.0}
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

After merging 2 nodes, ClassDistr: {737.6887524421978,9606.119134555689}
After merging 2 nodes, ClassDistr: {510.3112475578022,564.0}
After merging 2 nodes, ClassDistr: {845.0,1129.0}
After merging 2 nodes, ClassDistr: {257.0,1697.0}
After merging 2 nodes, ClassDistr: {161.0,1851.0}
After merging 2 nodes, ClassDistr: {1156.0,846.0}
After merging 2 nodes, ClassDistr: {622.0,1339.0}
After merging 2 nodes, ClassDistr: {9441.0,2610.0}
After merging 2 nodes, ClassDistr: {389.0,1664.0}
After merging 2 nodes, ClassDistr: {739.0,1263.0}
After merging 2 nodes, ClassDistr: {1078.0,915.0}
After merging 2 nodes, ClassDistr: {1496.0,555.0}
After merging 2 nodes, ClassDistr: {767.0,1241.0}
After merging 2 nodes, ClassDistr: {228.0,70.0}
After merging 2 nodes, ClassDistr: {236.0,67.0}
After merging 2 nodes, ClassDistr: {273.0,43.0}
After merging 2 nodes, ClassDistr: {10.0,300.0}
After merging 2 nodes, ClassDistr: {16.0,303.0}
After merging 2 nodes, ClassDistr: {1939.0,53.0}
After merging 2 nodes, ClassDistr: {345.0,1702.0}
After merging 2 nodes, ClassDistr: {1200.0,777.0}
After merging 2 nodes, ClassDistr: {95.0,1852.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,6.0}
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {737.6887524421978,9606.119134555689}{217.0,1177.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {510.3112475578022,564.0}{43.0,571.0}
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
      Leaf weight = {9441.0,2610.0}{1586.0,439.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {389.0,1664.0}{75.0,301.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {739.0,1263.0}{141.0,240.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1078.0,915.0}{200.0,187.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1496.0,555.0}{288.0,81.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {767.0,1241.0}{132.0,282.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {228.0,70.0}{69.0,16.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {236.0,67.0}{51.0,17.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {273.0,43.0}{60.0,13.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {10.0,300.0}{4.0,75.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {16.0,303.0}{7.0,77.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {737.6887524421978,9606.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {510.3112475578022,564.0}{0.0,0.0}
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
      Leaf weight = {9441.0,2610.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {389.0,1664.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {739.0,1263.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1078.0,915.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1496.0,555.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {767.0,1241.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {228.0,70.0}{0.0,0.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {236.0,67.0}{0.0,0.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {273.0,43.0}{0.0,0.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {10.0,300.0}{0.0,0.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {16.0,303.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {737.6887524421978,9606.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {510.3112475578022,564.0}{0.0,0.0}
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
      Leaf weight = {9441.0,2610.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {389.0,1664.0}{0.0,0.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {739.0,1263.0}{0.0,0.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1078.0,915.0}{0.0,0.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1496.0,555.0}{0.0,0.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {767.0,1241.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {228.0,70.0}{0.0,0.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {236.0,67.0}{0.0,0.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {273.0,43.0}{0.0,0.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {10.0,300.0}{0.0,0.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {16.0,303.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {737.6887524421978,9606.119134555689}{217.0,1177.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {510.3112475578022,564.0}{43.0,571.0}
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
      Leaf weight = {9441.0,2610.0}{1586.0,439.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 2 nominal 0] == 0
          Leaf weight = {389.0,1664.0}{75.0,301.0}
     if [feature 2 nominal 1] == 1
          Leaf weight = {739.0,1263.0}{141.0,240.0}
     if [feature 2 nominal 2] == 2
          Leaf weight = {1078.0,915.0}{200.0,187.0}
     if [feature 2 nominal 3] == 3
          Leaf weight = {1496.0,555.0}{288.0,81.0}
     if [feature 2 nominal 4] == 4
          Leaf weight = {767.0,1241.0}{132.0,282.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {228.0,70.0}{69.0,16.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {236.0,67.0}{51.0,17.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {273.0,43.0}{60.0,13.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {10.0,300.0}{4.0,75.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {16.0,303.0}{7.0,77.0}
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

After merging 2 nodes, ClassDistr: {954.6887524421978,10783.119134555689}
After merging 2 nodes, ClassDistr: {553.3112475578022,1135.0}
After merging 2 nodes, ClassDistr: {1017.0,1350.0}
After merging 2 nodes, ClassDistr: {306.0,2050.0}
After merging 2 nodes, ClassDistr: {185.0,2214.0}
After merging 2 nodes, ClassDistr: {1389.0,1026.0}
After merging 2 nodes, ClassDistr: {757.0,1609.0}
After merging 2 nodes, ClassDistr: {11027.0,3049.0}
After merging 2 nodes, ClassDistr: {464.0,1965.0}
After merging 2 nodes, ClassDistr: {880.0,1503.0}
After merging 2 nodes, ClassDistr: {1278.0,1102.0}
After merging 2 nodes, ClassDistr: {1784.0,636.0}
After merging 2 nodes, ClassDistr: {899.0,1523.0}
After merging 2 nodes, ClassDistr: {297.0,86.0}
After merging 2 nodes, ClassDistr: {287.0,84.0}
After merging 2 nodes, ClassDistr: {333.0,56.0}
After merging 2 nodes, ClassDistr: {14.0,375.0}
After merging 2 nodes, ClassDistr: {23.0,380.0}
After merging 2 nodes, ClassDistr: {2336.0,61.0}
After merging 2 nodes, ClassDistr: {399.0,2052.0}
After merging 2 nodes, ClassDistr: {1446.0,971.0}
After merging 2 nodes, ClassDistr: {124.0,2224.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,7.0}
HoeffdingTreeModel: addOnWeight = 1227.0
Best suggestion: FeatureSplit, merit=0.5283207462875497, NumericBinaryTest(false) feature[8] = 0.8987298371954409
Distribution From Split: class [0]= {438.6183030461176,149.77177308483695}
Distribution From Split: class [1]= {4.381696953882374,634.2282269151631}
HoeffdingTreeModel: {32,24,24,13}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {954.6887524421978,10783.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {553.3112475578022,1135.0}{0.0,0.0}
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
      Leaf weight = {11027.0,3049.0}{0.0,0.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {438.6183030461176,149.77177308483695}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {4.381696953882374,634.2282269151631}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {297.0,86.0}{0.0,0.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {287.0,84.0}{0.0,0.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {333.0,56.0}{0.0,0.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {14.0,375.0}{0.0,0.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {23.0,380.0}{0.0,0.0}
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

Total instances: 64569.188087407056
Accuracy 0.852
FileReader RDD count:10000
==============================
Counter: 90000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {954.6887524421978,10783.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {553.3112475578022,1135.0}{0.0,0.0}
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
      Leaf weight = {11027.0,3049.0}{0.0,0.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {438.6183030461176,149.77177308483695}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {4.381696953882374,634.2282269151631}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {297.0,86.0}{0.0,0.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {287.0,84.0}{0.0,0.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {333.0,56.0}{0.0,0.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {14.0,375.0}{0.0,0.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {23.0,380.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {954.6887524421978,10783.119134555689}{107.0,597.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {553.3112475578022,1135.0}{18.0,271.0}
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
      Leaf weight = {11027.0,3049.0}{811.0,221.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {438.6183030461176,149.77177308483695}{55.0,117.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {4.381696953882374,634.2282269151631}{9.0,4.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {297.0,86.0}{32.0,9.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {287.0,84.0}{32.0,7.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {333.0,56.0}{33.0,3.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {14.0,375.0}{1.0,41.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {23.0,380.0}{3.0,39.0}
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

After merging 2 nodes, ClassDistr: {954.6887524421978,10783.119134555689}
After merging 2 nodes, ClassDistr: {553.3112475578022,1135.0}
After merging 2 nodes, ClassDistr: {1017.0,1350.0}
After merging 2 nodes, ClassDistr: {306.0,2050.0}
After merging 2 nodes, ClassDistr: {185.0,2214.0}
After merging 2 nodes, ClassDistr: {1389.0,1026.0}
After merging 2 nodes, ClassDistr: {757.0,1609.0}
After merging 2 nodes, ClassDistr: {11027.0,3049.0}
After merging 2 nodes, ClassDistr: {464.0,1965.0}
After merging 2 nodes, ClassDistr: {880.0,1503.0}
After merging 2 nodes, ClassDistr: {1278.0,1102.0}
After merging 2 nodes, ClassDistr: {1784.0,636.0}
After merging 2 nodes, ClassDistr: {438.6183030461176,149.77177308483695}
After merging 2 nodes, ClassDistr: {4.381696953882374,634.2282269151631}
After merging 2 nodes, ClassDistr: {297.0,86.0}
After merging 2 nodes, ClassDistr: {287.0,84.0}
After merging 2 nodes, ClassDistr: {333.0,56.0}
After merging 2 nodes, ClassDistr: {14.0,375.0}
After merging 2 nodes, ClassDistr: {23.0,380.0}
After merging 2 nodes, ClassDistr: {2336.0,61.0}
After merging 2 nodes, ClassDistr: {399.0,2052.0}
After merging 2 nodes, ClassDistr: {1446.0,971.0}
After merging 2 nodes, ClassDistr: {124.0,2224.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,7.0}
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {954.6887524421978,10783.119134555689}{107.0,597.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {553.3112475578022,1135.0}{18.0,271.0}
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
      Leaf weight = {11027.0,3049.0}{811.0,221.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {438.6183030461176,149.77177308483695}{55.0,117.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {4.381696953882374,634.2282269151631}{9.0,4.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {297.0,86.0}{32.0,9.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {287.0,84.0}{32.0,7.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {333.0,56.0}{33.0,3.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {14.0,375.0}{1.0,41.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {23.0,380.0}{3.0,39.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {954.6887524421978,10783.119134555689}{107.0,598.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {553.3112475578022,1135.0}{20.0,277.0}
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
      Leaf weight = {11027.0,3049.0}{769.0,239.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {438.6183030461176,149.77177308483695}{62.0,128.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {4.381696953882374,634.2282269151631}{20.0,3.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {297.0,86.0}{30.0,12.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {287.0,84.0}{35.0,5.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {333.0,56.0}{31.0,4.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {14.0,375.0}{0.0,46.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {23.0,380.0}{0.0,43.0}
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

After merging 2 nodes, ClassDistr: {954.6887524421978,10783.119134555689}
After merging 2 nodes, ClassDistr: {553.3112475578022,1135.0}
After merging 2 nodes, ClassDistr: {1017.0,1350.0}
After merging 2 nodes, ClassDistr: {306.0,2050.0}
After merging 2 nodes, ClassDistr: {185.0,2214.0}
After merging 2 nodes, ClassDistr: {1389.0,1026.0}
After merging 2 nodes, ClassDistr: {757.0,1609.0}
After merging 2 nodes, ClassDistr: {11027.0,3049.0}
After merging 2 nodes, ClassDistr: {464.0,1965.0}
After merging 2 nodes, ClassDistr: {880.0,1503.0}
After merging 2 nodes, ClassDistr: {1278.0,1102.0}
After merging 2 nodes, ClassDistr: {1784.0,636.0}
After merging 2 nodes, ClassDistr: {438.6183030461176,149.77177308483695}
After merging 2 nodes, ClassDistr: {4.381696953882374,634.2282269151631}
After merging 2 nodes, ClassDistr: {297.0,86.0}
After merging 2 nodes, ClassDistr: {287.0,84.0}
After merging 2 nodes, ClassDistr: {333.0,56.0}
After merging 2 nodes, ClassDistr: {14.0,375.0}
After merging 2 nodes, ClassDistr: {23.0,380.0}
After merging 2 nodes, ClassDistr: {2336.0,61.0}
After merging 2 nodes, ClassDistr: {399.0,2052.0}
After merging 2 nodes, ClassDistr: {1446.0,971.0}
After merging 2 nodes, ClassDistr: {124.0,2224.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,7.0}
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {954.6887524421978,10783.119134555689}{214.0,1195.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {553.3112475578022,1135.0}{38.0,548.0}
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
      Leaf weight = {11027.0,3049.0}{1580.0,460.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {438.6183030461176,149.77177308483695}{117.0,245.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {4.381696953882374,634.2282269151631}{29.0,7.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {297.0,86.0}{62.0,21.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {287.0,84.0}{67.0,12.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {333.0,56.0}{64.0,7.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {14.0,375.0}{1.0,87.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {23.0,380.0}{3.0,82.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {954.6887524421978,10783.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {553.3112475578022,1135.0}{0.0,0.0}
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
      Leaf weight = {11027.0,3049.0}{0.0,0.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {438.6183030461176,149.77177308483695}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {4.381696953882374,634.2282269151631}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {297.0,86.0}{0.0,0.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {287.0,84.0}{0.0,0.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {333.0,56.0}{0.0,0.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {14.0,375.0}{0.0,0.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {23.0,380.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {954.6887524421978,10783.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {553.3112475578022,1135.0}{0.0,0.0}
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
      Leaf weight = {11027.0,3049.0}{0.0,0.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {438.6183030461176,149.77177308483695}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {4.381696953882374,634.2282269151631}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {297.0,86.0}{0.0,0.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {287.0,84.0}{0.0,0.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {333.0,56.0}{0.0,0.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {14.0,375.0}{0.0,0.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {23.0,380.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {954.6887524421978,10783.119134555689}{214.0,1195.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {553.3112475578022,1135.0}{38.0,548.0}
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
      Leaf weight = {11027.0,3049.0}{1580.0,460.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {438.6183030461176,149.77177308483695}{117.0,245.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {4.381696953882374,634.2282269151631}{29.0,7.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {297.0,86.0}{62.0,21.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {287.0,84.0}{67.0,12.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {333.0,56.0}{64.0,7.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {14.0,375.0}{1.0,87.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {23.0,380.0}{3.0,82.0}
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

After merging 2 nodes, ClassDistr: {1168.6887524421977,11978.119134555689}
After merging 2 nodes, ClassDistr: {591.3112475578022,1683.0}
After merging 2 nodes, ClassDistr: {1194.0,1561.0}
After merging 2 nodes, ClassDistr: {354.0,2415.0}
After merging 2 nodes, ClassDistr: {214.0,2565.0}
After merging 2 nodes, ClassDistr: {1626.0,1198.0}
After merging 2 nodes, ClassDistr: {903.0,1889.0}
After merging 2 nodes, ClassDistr: {12607.0,3509.0}
After merging 2 nodes, ClassDistr: {524.0,2292.0}
After merging 2 nodes, ClassDistr: {1038.0,1771.0}
After merging 2 nodes, ClassDistr: {1490.0,1261.0}
After merging 2 nodes, ClassDistr: {2075.0,740.0}
After merging 2 nodes, ClassDistr: {555.6183030461176,394.7717730848369}
After merging 2 nodes, ClassDistr: {33.381696953882376,641.2282269151631}
After merging 2 nodes, ClassDistr: {359.0,107.0}
After merging 2 nodes, ClassDistr: {354.0,96.0}
After merging 2 nodes, ClassDistr: {397.0,63.0}
After merging 2 nodes, ClassDistr: {15.0,462.0}
After merging 2 nodes, ClassDistr: {26.0,462.0}
After merging 2 nodes, ClassDistr: {2704.0,76.0}
After merging 2 nodes, ClassDistr: {454.0,2390.0}
After merging 2 nodes, ClassDistr: {1678.0,1126.0}
After merging 2 nodes, ClassDistr: {140.0,2611.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,7.0}
HoeffdingTreeModel: addOnWeight = 2357.0
HoeffdingTreeModel: {32,24,24,13}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {1168.6887524421977,11978.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {591.3112475578022,1683.0}{0.0,0.0}
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
      Leaf weight = {12607.0,3509.0}{0.0,0.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {555.6183030461176,394.7717730848369}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {33.381696953882376,641.2282269151631}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {359.0,107.0}{0.0,0.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {354.0,96.0}{0.0,0.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {397.0,63.0}{0.0,0.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {15.0,462.0}{0.0,0.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {26.0,462.0}{0.0,0.0}
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

Total instances: 74569.18808740705
Accuracy 0.871
FileReader RDD count:10000
==============================
Counter: 100000
Chunk: 10000
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {1168.6887524421977,11978.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {591.3112475578022,1683.0}{0.0,0.0}
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
      Leaf weight = {12607.0,3509.0}{0.0,0.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {555.6183030461176,394.7717730848369}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {33.381696953882376,641.2282269151631}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {359.0,107.0}{0.0,0.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {354.0,96.0}{0.0,0.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {397.0,63.0}{0.0,0.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {15.0,462.0}{0.0,0.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {26.0,462.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {1168.6887524421977,11978.119134555689}{101.0,618.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {591.3112475578022,1683.0}{21.0,274.0}
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
      Leaf weight = {12607.0,3509.0}{787.0,245.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {555.6183030461176,394.7717730848369}{53.0,109.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {33.381696953882376,641.2282269151631}{12.0,8.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {359.0,107.0}{33.0,10.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {354.0,96.0}{33.0,12.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {397.0,63.0}{24.0,9.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {15.0,462.0}{1.0,50.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {26.0,462.0}{2.0,36.0}
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

After merging 2 nodes, ClassDistr: {1168.6887524421977,11978.119134555689}
After merging 2 nodes, ClassDistr: {591.3112475578022,1683.0}
After merging 2 nodes, ClassDistr: {1194.0,1561.0}
After merging 2 nodes, ClassDistr: {354.0,2415.0}
After merging 2 nodes, ClassDistr: {214.0,2565.0}
After merging 2 nodes, ClassDistr: {1626.0,1198.0}
After merging 2 nodes, ClassDistr: {903.0,1889.0}
After merging 2 nodes, ClassDistr: {12607.0,3509.0}
After merging 2 nodes, ClassDistr: {524.0,2292.0}
After merging 2 nodes, ClassDistr: {1038.0,1771.0}
After merging 2 nodes, ClassDistr: {1490.0,1261.0}
After merging 2 nodes, ClassDistr: {2075.0,740.0}
After merging 2 nodes, ClassDistr: {555.6183030461176,394.7717730848369}
After merging 2 nodes, ClassDistr: {33.381696953882376,641.2282269151631}
After merging 2 nodes, ClassDistr: {359.0,107.0}
After merging 2 nodes, ClassDistr: {354.0,96.0}
After merging 2 nodes, ClassDistr: {397.0,63.0}
After merging 2 nodes, ClassDistr: {15.0,462.0}
After merging 2 nodes, ClassDistr: {26.0,462.0}
After merging 2 nodes, ClassDistr: {2704.0,76.0}
After merging 2 nodes, ClassDistr: {454.0,2390.0}
After merging 2 nodes, ClassDistr: {1678.0,1126.0}
After merging 2 nodes, ClassDistr: {140.0,2611.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,7.0}
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {1168.6887524421977,11978.119134555689}{101.0,618.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {591.3112475578022,1683.0}{21.0,274.0}
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
      Leaf weight = {12607.0,3509.0}{787.0,245.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {555.6183030461176,394.7717730848369}{53.0,109.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {33.381696953882376,641.2282269151631}{12.0,8.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {359.0,107.0}{33.0,10.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {354.0,96.0}{33.0,12.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {397.0,63.0}{24.0,9.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {15.0,462.0}{1.0,50.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {26.0,462.0}{2.0,36.0}
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

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {1168.6887524421977,11978.119134555689}{113.0,577.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {591.3112475578022,1683.0}{18.0,282.0}
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
      Leaf weight = {12607.0,3509.0}{825.0,230.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {555.6183030461176,394.7717730848369}{57.0,126.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {33.381696953882376,641.2282269151631}{13.0,5.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {359.0,107.0}{32.0,4.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {354.0,96.0}{36.0,8.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {397.0,63.0}{37.0,8.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {15.0,462.0}{1.0,30.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {26.0,462.0}{3.0,41.0}
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

After merging 2 nodes, ClassDistr: {1168.6887524421977,11978.119134555689}
After merging 2 nodes, ClassDistr: {591.3112475578022,1683.0}
After merging 2 nodes, ClassDistr: {1194.0,1561.0}
After merging 2 nodes, ClassDistr: {354.0,2415.0}
After merging 2 nodes, ClassDistr: {214.0,2565.0}
After merging 2 nodes, ClassDistr: {1626.0,1198.0}
After merging 2 nodes, ClassDistr: {903.0,1889.0}
After merging 2 nodes, ClassDistr: {12607.0,3509.0}
After merging 2 nodes, ClassDistr: {524.0,2292.0}
After merging 2 nodes, ClassDistr: {1038.0,1771.0}
After merging 2 nodes, ClassDistr: {1490.0,1261.0}
After merging 2 nodes, ClassDistr: {2075.0,740.0}
After merging 2 nodes, ClassDistr: {555.6183030461176,394.7717730848369}
After merging 2 nodes, ClassDistr: {33.381696953882376,641.2282269151631}
After merging 2 nodes, ClassDistr: {359.0,107.0}
After merging 2 nodes, ClassDistr: {354.0,96.0}
After merging 2 nodes, ClassDistr: {397.0,63.0}
After merging 2 nodes, ClassDistr: {15.0,462.0}
After merging 2 nodes, ClassDistr: {26.0,462.0}
After merging 2 nodes, ClassDistr: {2704.0,76.0}
After merging 2 nodes, ClassDistr: {454.0,2390.0}
After merging 2 nodes, ClassDistr: {1678.0,1126.0}
After merging 2 nodes, ClassDistr: {140.0,2611.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,7.0}
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {1168.6887524421977,11978.119134555689}{214.0,1195.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {591.3112475578022,1683.0}{39.0,556.0}
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
      Leaf weight = {12607.0,3509.0}{1612.0,475.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {555.6183030461176,394.7717730848369}{110.0,235.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {33.381696953882376,641.2282269151631}{25.0,13.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {359.0,107.0}{65.0,14.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {354.0,96.0}{69.0,20.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {397.0,63.0}{61.0,17.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {15.0,462.0}{2.0,80.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {26.0,462.0}{5.0,77.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {1168.6887524421977,11978.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {591.3112475578022,1683.0}{0.0,0.0}
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
      Leaf weight = {12607.0,3509.0}{0.0,0.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {555.6183030461176,394.7717730848369}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {33.381696953882376,641.2282269151631}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {359.0,107.0}{0.0,0.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {354.0,96.0}{0.0,0.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {397.0,63.0}{0.0,0.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {15.0,462.0}{0.0,0.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {26.0,462.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {1168.6887524421977,11978.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {591.3112475578022,1683.0}{0.0,0.0}
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
      Leaf weight = {12607.0,3509.0}{0.0,0.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {555.6183030461176,394.7717730848369}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {33.381696953882376,641.2282269151631}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {359.0,107.0}{0.0,0.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {354.0,96.0}{0.0,0.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {397.0,63.0}{0.0,0.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {15.0,462.0}{0.0,0.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {26.0,462.0}{0.0,0.0}
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
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {1168.6887524421977,11978.119134555689}{214.0,1195.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {591.3112475578022,1683.0}{39.0,556.0}
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
      Leaf weight = {12607.0,3509.0}{1612.0,475.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {555.6183030461176,394.7717730848369}{110.0,235.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {33.381696953882376,641.2282269151631}{25.0,13.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {359.0,107.0}{65.0,14.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {354.0,96.0}{69.0,20.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {397.0,63.0}{61.0,17.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {15.0,462.0}{2.0,80.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {26.0,462.0}{5.0,77.0}
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

After merging 2 nodes, ClassDistr: {1382.6887524421977,13173.119134555689}
After merging 2 nodes, ClassDistr: {630.3112475578022,2239.0}
After merging 2 nodes, ClassDistr: {1346.0,1775.0}
After merging 2 nodes, ClassDistr: {404.0,2751.0}
After merging 2 nodes, ClassDistr: {241.0,2925.0}
After merging 2 nodes, ClassDistr: {1857.0,1345.0}
After merging 2 nodes, ClassDistr: {1032.0,2163.0}
After merging 2 nodes, ClassDistr: {14219.0,3984.0}
After merging 2 nodes, ClassDistr: {607.0,2610.0}
After merging 2 nodes, ClassDistr: {1191.0,2017.0}
After merging 2 nodes, ClassDistr: {1727.0,1439.0}
After merging 2 nodes, ClassDistr: {2342.0,837.0}
After merging 2 nodes, ClassDistr: {665.6183030461176,629.7717730848369}
After merging 2 nodes, ClassDistr: {58.381696953882376,654.2282269151631}
After merging 2 nodes, ClassDistr: {424.0,121.0}
After merging 2 nodes, ClassDistr: {423.0,116.0}
After merging 2 nodes, ClassDistr: {458.0,80.0}
After merging 2 nodes, ClassDistr: {17.0,542.0}
After merging 2 nodes, ClassDistr: {31.0,539.0}
After merging 2 nodes, ClassDistr: {3082.0,87.0}
After merging 2 nodes, ClassDistr: {518.0,2715.0}
After merging 2 nodes, ClassDistr: {1944.0,1291.0}
After merging 2 nodes, ClassDistr: {163.0,2996.0}
After merging 2 nodes, ClassDistr: {2770.0689528513653,7.0}
HoeffdingTreeModel: addOnWeight = 16184.0
Best suggestion: FeatureSplit, merit=0.15579190305634816, NumericBinaryTest(false) feature[9] = 0.7000097673050265
Distribution From Split: class [0]= {15179.304836738002,2822.0294869191534}
Distribution From Split: class [1]= {0.0,723.9705130808464}
HoeffdingTreeModel: {34,25,25,13}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.6999404200192979
          Leaf weight = {1382.6887524421977,13173.119134555689}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.6999404200192979
          Leaf weight = {630.3112475578022,2239.0}{0.0,0.0}
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
      
     if [feature 9 numeric 0] < 0.7000097673050265
          Leaf weight = {15179.304836738002,2822.0294869191534}{0.0,0.0}
     if [feature 9 numeric 1] >= 0.7000097673050265
          Leaf weight = {0.0,723.9705130808464}{0.0,0.0}
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
          
       if [feature 8 numeric 0] < 0.8987298371954409
              Leaf weight = {665.6183030461176,629.7717730848369}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.8987298371954409
              Leaf weight = {58.381696953882376,654.2282269151631}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      
     if [feature 1 nominal 0] == 0
          
       if [feature 0 nominal 0] == 0
              Leaf weight = {424.0,121.0}{0.0,0.0}
       if [feature 0 nominal 1] == 1
              Leaf weight = {423.0,116.0}{0.0,0.0}
       if [feature 0 nominal 2] == 2
              Leaf weight = {458.0,80.0}{0.0,0.0}
       if [feature 0 nominal 3] == 3
              Leaf weight = {17.0,542.0}{0.0,0.0}
       if [feature 0 nominal 4] == 4
              Leaf weight = {31.0,539.0}{0.0,0.0}
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

Total instances: 85091.49292414506
Accuracy 0.855
FileReader RDD count:10000
==============================
Counter: 110000
Chunk: 10000
Over 100.000 instances
Running time = 102.085658148
Reading file stopped.
