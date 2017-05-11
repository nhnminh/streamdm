../data/randomtreedata.arff
Learning Node Type: 0
Learning Node Type: 0
File reading gets started.
FileReader RDD count:10000
==============================
Counter: 10000
Chunk: 10000
TrySplit: false
This root: Leaf weight = {0.0,0.0}{0.0,0.0}

That root: Leaf weight = {0.0,0.0}{2089.0,2911.0}

ActiveLearningNode merge!
CheckActive: true
Why???
TrySplit: false
This root: Leaf weight = {0.0,0.0}{2089.0,2911.0}

That root: Leaf weight = {0.0,0.0}{2007.0,2993.0}

ActiveLearningNode merge!
CheckActive: true
Why???
tmodel: Hoeffding Tree Model description:
Leaf weight = {0.0,0.0}{4096.0,5904.0}

Before merge: Hoeffding Tree Model description:
Leaf weight = {0.0,0.0}{0.0,0.0}

TrySplit: true
This root: Leaf weight = {0.0,0.0}{0.0,0.0}

That root: Leaf weight = {0.0,0.0}{4096.0,5904.0}

ActiveLearningNode merge!
CheckActive: true
Why???
HoeffdingTreeModel: addOnWeight = 10000.0
HoeffdingTreeModel: {3,2,2,2}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  Leaf weight = {1328.9310471486347,5904.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2767.0689528513653,0.0}{0.0,0.0}

Accuracy 0.591
FileReader RDD count:10000
==============================
Counter: 20000
Chunk: 10000
TrySplit: false
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

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
TrySplit: false
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

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
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

TrySplit: true
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

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
HoeffdingTreeModel: addOnWeight = 9998.0
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

Accuracy 0.690
FileReader RDD count:10000
==============================
Counter: 30000
Chunk: 10000
TrySplit: false
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

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
TrySplit: false
This root: 
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

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
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

TrySplit: true
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

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
HoeffdingTreeModel: addOnWeight = 2046.0
HoeffdingTreeModel: {10,7,7,7}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {485.0,3556.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1186.0,2768.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {3066.0,888.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {267.4138173190388,1134.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {1852.0,2125.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,3.0}{0.0,0.0}

Accuracy 0.689
FileReader RDD count:10000
==============================
Counter: 40000
Chunk: 10000
TrySplit: false
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {485.0,3556.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1186.0,2768.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {3066.0,888.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {267.4138173190388,1134.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {1852.0,2125.0}{0.0,0.0}
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {267.4138173190388,1134.0}{455.0,554.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {1852.0,2125.0}{460.0,518.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,3.0}{0.0,0.0}

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
TrySplit: false
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {485.0,3556.0}{120.0,897.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1186.0,2768.0}{291.0,693.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {3066.0,888.0}{788.0,224.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {267.4138173190388,1134.0}{455.0,554.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {1852.0,2125.0}{460.0,518.0}
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {267.4138173190388,1134.0}{449.0,519.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {1852.0,2125.0}{444.0,573.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,3.0}{1.0,0.0}

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {485.0,3556.0}{246.0,1768.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1186.0,2768.0}{599.0,1382.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {3066.0,888.0}{1578.0,454.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {267.4138173190388,1134.0}{904.0,1073.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {1852.0,2125.0}{904.0,1091.0}
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {267.4138173190388,1134.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {1852.0,2125.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,3.0}{0.0,0.0}

TrySplit: true
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      Leaf weight = {485.0,3556.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {1186.0,2768.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {3066.0,888.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {267.4138173190388,1134.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {1852.0,2125.0}{0.0,0.0}
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {267.4138173190388,1134.0}{904.0,1073.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {1852.0,2125.0}{904.0,1091.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2768.0689528513653,3.0}{1.0,0.0}

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
HoeffdingTreeModel: addOnWeight = 4057.0
HoeffdingTreeModel: {12,8,8,9}
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {1171.4138173190388,2207.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {2756.0,3216.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2769.0689528513653,3.0}{0.0,0.0}

Accuracy 0.637
FileReader RDD count:10000
==============================
Counter: 50000
Chunk: 10000
TrySplit: false
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {1171.4138173190388,2207.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {2756.0,3216.0}{0.0,0.0}
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {1171.4138173190388,2207.0}{442.0,563.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {2756.0,3216.0}{447.0,561.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2769.0689528513653,3.0}{1.0,1.0}

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
TrySplit: false
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {1171.4138173190388,2207.0}{442.0,563.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {2756.0,3216.0}{447.0,561.0}
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {1171.4138173190388,2207.0}{425.0,576.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {2756.0,3216.0}{471.0,573.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2769.0689528513653,3.0}{0.0,1.0}

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {1171.4138173190388,2207.0}{867.0,1139.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {2756.0,3216.0}{918.0,1134.0}
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {1171.4138173190388,2207.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {2756.0,3216.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2769.0689528513653,3.0}{0.0,0.0}

TrySplit: true
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {1171.4138173190388,2207.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {2756.0,3216.0}{0.0,0.0}
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {1171.4138173190388,2207.0}{867.0,1139.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {2756.0,3216.0}{918.0,1134.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2769.0689528513653,3.0}{1.0,2.0}

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
HoeffdingTreeModel: addOnWeight = 1395.0
HoeffdingTreeModel: {14,9,9,11}
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2038.4138173190388,3346.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {3674.0,4350.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,5.0}{0.0,0.0}

Accuracy 0.667
FileReader RDD count:10000
==============================
Counter: 60000
Chunk: 10000
TrySplit: false
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2038.4138173190388,3346.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {3674.0,4350.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,5.0}{0.0,0.0}

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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2038.4138173190388,3346.0}{456.0,574.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {3674.0,4350.0}{434.0,529.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,5.0}{0.0,0.0}

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
TrySplit: false
This root: 
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2038.4138173190388,3346.0}{456.0,574.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {3674.0,4350.0}{434.0,529.0}
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2038.4138173190388,3346.0}{440.0,599.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,1.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {3674.0,4350.0}{451.0,528.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,5.0}{0.0,1.0}

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2038.4138173190388,3346.0}{896.0,1173.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,1.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {3674.0,4350.0}{885.0,1057.0}
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2038.4138173190388,3346.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {3674.0,4350.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,5.0}{0.0,0.0}

TrySplit: true
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2038.4138173190388,3346.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {3674.0,4350.0}{0.0,0.0}
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
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2038.4138173190388,3346.0}{896.0,1173.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,0.0}{0.0,1.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {3674.0,4350.0}{885.0,1057.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,5.0}{0.0,1.0}

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
HoeffdingTreeModel: addOnWeight = 284.0
HoeffdingTreeModel: {16,10,10,12}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {1.9159056998150392,389.31385630059935}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {20.08409430018496,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {312.4506635520296,1119.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2934.4138173190386,4519.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,1.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {4559.0,5407.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

Accuracy 0.682
FileReader RDD count:10000
==============================
Counter: 70000
Chunk: 10000
TrySplit: false
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {1.9159056998150392,389.31385630059935}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {20.08409430018496,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {312.4506635520296,1119.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2934.4138173190386,4519.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,1.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {4559.0,5407.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{110.0,453.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {1.9159056998150392,389.31385630059935}{4.0,70.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {20.08409430018496,0.0}{3.0,81.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {312.4506635520296,1119.0}{17.0,297.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{291.0,699.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{780.0,229.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2934.4138173190386,4519.0}{434.0,565.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,1.0}{0.0,3.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {4559.0,5407.0}{468.0,496.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
TrySplit: false
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{110.0,453.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {1.9159056998150392,389.31385630059935}{4.0,70.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {20.08409430018496,0.0}{3.0,81.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {312.4506635520296,1119.0}{17.0,297.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{291.0,699.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{780.0,229.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2934.4138173190386,4519.0}{434.0,565.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,1.0}{0.0,3.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {4559.0,5407.0}{468.0,496.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{95.0,448.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {1.9159056998150392,389.31385630059935}{5.0,65.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {20.08409430018496,0.0}{2.0,67.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {312.4506635520296,1119.0}{25.0,268.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{333.0,682.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{835.0,199.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2934.4138173190386,4519.0}{456.0,549.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,1.0}{0.0,1.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {4559.0,5407.0}{444.0,526.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{205.0,901.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {1.9159056998150392,389.31385630059935}{9.0,135.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {20.08409430018496,0.0}{5.0,148.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {312.4506635520296,1119.0}{42.0,565.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{624.0,1381.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{1615.0,428.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2934.4138173190386,4519.0}{890.0,1114.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,1.0}{0.0,4.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {4559.0,5407.0}{912.0,1022.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

Before merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {1.9159056998150392,389.31385630059935}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {20.08409430018496,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {312.4506635520296,1119.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2934.4138173190386,4519.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,1.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {4559.0,5407.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

TrySplit: true
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {1.9159056998150392,389.31385630059935}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {20.08409430018496,0.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {312.4506635520296,1119.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2934.4138173190386,4519.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,1.0}{0.0,0.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {4559.0,5407.0}{0.0,0.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

That root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {268.07465056682827,2468.6555004735615}{205.0,901.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {1.9159056998150392,389.31385630059935}{9.0,135.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {20.08409430018496,0.0}{5.0,148.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {312.4506635520296,1119.0}{42.0,565.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3004.0,6859.0}{624.0,1381.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {7826.0,2182.0}{1615.0,428.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {2934.4138173190386,4519.0}{890.0,1114.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,1.0}{0.0,4.0}
   if [feature 4 nominal 4] == 4
      Leaf weight = {4559.0,5407.0}{912.0,1022.0}
 if [feature 5 numeric 1] >= 0.9999057216873717
  Leaf weight = {2770.0689528513653,6.0}{0.0,0.0}

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
HoeffdingTreeModel: addOnWeight = 9908.0
HoeffdingTreeModel: {21,14,14,12}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {473.07465056682827,3369.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {10.915905699815038,524.3138563005994}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {25.08409430018496,148.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {354.4506635520296,1684.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {9441.0,2610.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {3824.4138173190386,5633.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,5.0}{0.0,0.0}
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

Accuracy 0.735
FileReader RDD count:10000
==============================
Counter: 80000
Chunk: 10000
TrySplit: false
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {473.07465056682827,3369.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {10.915905699815038,524.3138563005994}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {25.08409430018496,148.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {354.4506635520296,1684.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {9441.0,2610.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {3824.4138173190386,5633.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,5.0}{0.0,0.0}
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
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {10.915905699815038,524.3138563005994}{6.0,54.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {25.08409430018496,148.0}{1.0,62.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {354.4506635520296,1684.0}{25.0,281.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{306.0,673.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {9441.0,2610.0}{796.0,214.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {3824.4138173190386,5633.0}{431.0,576.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,5.0}{0.0,2.0}
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

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
TrySplit: false
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {473.07465056682827,3369.6555004735615}{101.0,460.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {10.915905699815038,524.3138563005994}{6.0,54.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {25.08409430018496,148.0}{1.0,62.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {354.4506635520296,1684.0}{25.0,281.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{306.0,673.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {9441.0,2610.0}{796.0,214.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {3824.4138173190386,5633.0}{431.0,576.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,5.0}{0.0,2.0}
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
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {10.915905699815038,524.3138563005994}{6.0,71.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {25.08409430018496,148.0}{4.0,70.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {354.4506635520296,1684.0}{18.0,290.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{307.0,714.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {9441.0,2610.0}{790.0,225.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {3824.4138173190386,5633.0}{405.0,512.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,5.0}{0.0,1.0}
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

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {473.07465056682827,3369.6555004735615}{200.0,920.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {10.915905699815038,524.3138563005994}{12.0,125.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {25.08409430018496,148.0}{5.0,132.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {354.4506635520296,1684.0}{43.0,571.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{613.0,1387.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {9441.0,2610.0}{1586.0,439.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {3824.4138173190386,5633.0}{836.0,1088.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,5.0}{0.0,3.0}
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
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {10.915905699815038,524.3138563005994}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {25.08409430018496,148.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {354.4506635520296,1684.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {9441.0,2610.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {3824.4138173190386,5633.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,5.0}{0.0,0.0}
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

TrySplit: true
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {473.07465056682827,3369.6555004735615}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {10.915905699815038,524.3138563005994}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {25.08409430018496,148.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {354.4506635520296,1684.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {9441.0,2610.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {3824.4138173190386,5633.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,5.0}{0.0,0.0}
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
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {10.915905699815038,524.3138563005994}{12.0,125.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {25.08409430018496,148.0}{5.0,132.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {354.4506635520296,1684.0}{43.0,571.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {3628.0,8240.0}{613.0,1387.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {9441.0,2610.0}{1586.0,439.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          Leaf weight = {3824.4138173190386,5633.0}{836.0,1088.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,5.0}{0.0,3.0}
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

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
HoeffdingTreeModel: addOnWeight = 9980.0
HoeffdingTreeModel: {26,18,18,12}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {673.0746505668283,4289.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {22.91590569981504,649.3138563005994}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {30.08409430018496,280.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {397.4506635520296,2255.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {11027.0,3049.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {389.0,1618.0}{0.0,0.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {721.0,1261.0}{0.0,0.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1074.0,919.0}{0.0,0.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1452.0,530.0}{0.0,0.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {757.0,1259.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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

Accuracy 0.759
FileReader RDD count:10000
==============================
Counter: 90000
Chunk: 10000
TrySplit: false
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {673.0746505668283,4289.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {22.91590569981504,649.3138563005994}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {30.08409430018496,280.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {397.4506635520296,2255.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {11027.0,3049.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {389.0,1618.0}{0.0,0.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {721.0,1261.0}{0.0,0.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1074.0,919.0}{0.0,0.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1452.0,530.0}{0.0,0.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {757.0,1259.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {22.91590569981504,649.3138563005994}{7.0,71.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {30.08409430018496,280.0}{2.0,64.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {397.4506635520296,2255.0}{18.0,271.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{336.0,665.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {11027.0,3049.0}{811.0,221.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {389.0,1618.0}{28.0,158.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {721.0,1261.0}{76.0,138.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1074.0,919.0}{107.0,79.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1452.0,530.0}{154.0,52.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {757.0,1259.0}{64.0,121.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
TrySplit: false
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {673.0746505668283,4289.655500473562}{98.0,462.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {22.91590569981504,649.3138563005994}{7.0,71.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {30.08409430018496,280.0}{2.0,64.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {397.4506635520296,2255.0}{18.0,271.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{336.0,665.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {11027.0,3049.0}{811.0,221.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {389.0,1618.0}{28.0,158.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {721.0,1261.0}{76.0,138.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1074.0,919.0}{107.0,79.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1452.0,530.0}{154.0,52.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {757.0,1259.0}{64.0,121.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {22.91590569981504,649.3138563005994}{9.0,74.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {30.08409430018496,280.0}{2.0,68.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {397.4506635520296,2255.0}{20.0,277.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{301.0,714.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {11027.0,3049.0}{769.0,239.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {389.0,1618.0}{32.0,169.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {721.0,1261.0}{82.0,130.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1074.0,919.0}{105.0,80.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1452.0,530.0}{137.0,52.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {757.0,1259.0}{82.0,131.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {673.0746505668283,4289.655500473562}{194.0,918.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {22.91590569981504,649.3138563005994}{16.0,145.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {30.08409430018496,280.0}{4.0,132.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {397.4506635520296,2255.0}{38.0,548.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{637.0,1379.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {11027.0,3049.0}{1580.0,460.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {389.0,1618.0}{60.0,327.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {721.0,1261.0}{158.0,268.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1074.0,919.0}{212.0,159.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1452.0,530.0}{291.0,104.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {757.0,1259.0}{146.0,252.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {22.91590569981504,649.3138563005994}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {30.08409430018496,280.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {397.4506635520296,2255.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {11027.0,3049.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {389.0,1618.0}{0.0,0.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {721.0,1261.0}{0.0,0.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1074.0,919.0}{0.0,0.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1452.0,530.0}{0.0,0.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {757.0,1259.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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

TrySplit: true
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {673.0746505668283,4289.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {22.91590569981504,649.3138563005994}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {30.08409430018496,280.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {397.4506635520296,2255.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {11027.0,3049.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {389.0,1618.0}{0.0,0.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {721.0,1261.0}{0.0,0.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1074.0,919.0}{0.0,0.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1452.0,530.0}{0.0,0.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {757.0,1259.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {22.91590569981504,649.3138563005994}{16.0,145.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {30.08409430018496,280.0}{4.0,132.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {397.4506635520296,2255.0}{38.0,548.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4241.0,9627.0}{637.0,1379.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {11027.0,3049.0}{1580.0,460.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {389.0,1618.0}{60.0,327.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {721.0,1261.0}{158.0,268.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1074.0,919.0}{212.0,159.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1452.0,530.0}{291.0,104.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {757.0,1259.0}{146.0,252.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
HoeffdingTreeModel: addOnWeight = 804.0
HoeffdingTreeModel: {26,18,18,12}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {867.0746505668283,5207.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {38.91590569981504,794.3138563005994}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {34.08409430018496,412.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {435.4506635520296,2803.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {12607.0,3509.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {449.0,1945.0}{0.0,0.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {879.0,1529.0}{0.0,0.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1286.0,1078.0}{0.0,0.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1743.0,634.0}{0.0,0.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {903.0,1511.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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

Accuracy 0.759
FileReader RDD count:10000
==============================
Counter: 100000
Chunk: 10000
TrySplit: false
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {867.0746505668283,5207.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {38.91590569981504,794.3138563005994}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {34.08409430018496,412.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {435.4506635520296,2803.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {12607.0,3509.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {449.0,1945.0}{0.0,0.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {879.0,1529.0}{0.0,0.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1286.0,1078.0}{0.0,0.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1743.0,634.0}{0.0,0.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {903.0,1511.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {38.91590569981504,794.3138563005994}{6.0,58.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {34.08409430018496,412.0}{3.0,70.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {435.4506635520296,2803.0}{18.0,282.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{315.0,650.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {12607.0,3509.0}{825.0,230.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {449.0,1945.0}{31.0,166.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {879.0,1529.0}{71.0,120.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1286.0,1078.0}{124.0,101.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1743.0,634.0}{110.0,50.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {903.0,1511.0}{70.0,131.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
TrySplit: false
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {867.0746505668283,5207.655500473562}{104.0,449.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {38.91590569981504,794.3138563005994}{6.0,58.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {34.08409430018496,412.0}{3.0,70.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {435.4506635520296,2803.0}{18.0,282.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{315.0,650.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {12607.0,3509.0}{825.0,230.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {449.0,1945.0}{31.0,166.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {879.0,1529.0}{71.0,120.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1286.0,1078.0}{124.0,101.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1743.0,634.0}{110.0,50.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {903.0,1511.0}{70.0,131.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {38.91590569981504,794.3138563005994}{8.0,57.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {34.08409430018496,412.0}{6.0,88.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {435.4506635520296,2803.0}{21.0,274.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{274.0,681.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {12607.0,3509.0}{787.0,245.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {449.0,1945.0}{52.0,152.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {879.0,1529.0}{82.0,126.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1286.0,1078.0}{113.0,77.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1743.0,634.0}{157.0,47.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {903.0,1511.0}{65.0,117.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
tmodel: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {867.0746505668283,5207.655500473562}{191.0,922.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {38.91590569981504,794.3138563005994}{14.0,115.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {34.08409430018496,412.0}{9.0,158.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {435.4506635520296,2803.0}{39.0,556.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{589.0,1331.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {12607.0,3509.0}{1612.0,475.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {449.0,1945.0}{83.0,318.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {879.0,1529.0}{153.0,246.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1286.0,1078.0}{237.0,178.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1743.0,634.0}{267.0,97.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {903.0,1511.0}{135.0,248.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {38.91590569981504,794.3138563005994}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {34.08409430018496,412.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {435.4506635520296,2803.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {12607.0,3509.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {449.0,1945.0}{0.0,0.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {879.0,1529.0}{0.0,0.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1286.0,1078.0}{0.0,0.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1743.0,634.0}{0.0,0.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {903.0,1511.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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

TrySplit: true
This root: 
 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {867.0746505668283,5207.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {38.91590569981504,794.3138563005994}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {34.08409430018496,412.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {435.4506635520296,2803.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {12607.0,3509.0}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {449.0,1945.0}{0.0,0.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {879.0,1529.0}{0.0,0.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1286.0,1078.0}{0.0,0.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1743.0,634.0}{0.0,0.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {903.0,1511.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {38.91590569981504,794.3138563005994}{14.0,115.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {34.08409430018496,412.0}{9.0,158.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {435.4506635520296,2803.0}{39.0,556.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {4878.0,11006.0}{589.0,1331.0}
   if [feature 4 nominal 2] == 2
      Leaf weight = {12607.0,3509.0}{1612.0,475.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {449.0,1945.0}{83.0,318.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {879.0,1529.0}{153.0,246.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1286.0,1078.0}{237.0,178.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {1743.0,634.0}{267.0,97.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {903.0,1511.0}{135.0,248.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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

ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
ActiveLearningNode merge!
CheckActive: true
Why???
HoeffdingTreeModel: addOnWeight = 16184.0
HoeffdingTreeModel: {28,19,19,12}
After merge: Hoeffding Tree Model description:

 if [feature 5 numeric 0] < 0.9999057216873717
  
   if [feature 4 nominal 0] == 0
      
     if [feature 8 numeric 0] < 0.699756786990496
          
       if [feature 8 numeric 0] < 0.5597368462536156
              Leaf weight = {1058.0746505668283,6129.655500473562}{0.0,0.0}
       if [feature 8 numeric 1] >= 0.5597368462536156
              
         if [feature 5 numeric 0] < 0.4985355671116213
                  Leaf weight = {52.91590569981504,909.3138563005994}{0.0,0.0}
         if [feature 5 numeric 1] >= 0.4985355671116213
                  Leaf weight = {43.08409430018496,570.0}{0.0,0.0}
     if [feature 8 numeric 1] >= 0.699756786990496
          Leaf weight = {474.4506635520296,3359.0}{0.0,0.0}
   if [feature 4 nominal 1] == 1
      Leaf weight = {5467.0,12337.0}{0.0,0.0}
   if [feature 4 nominal 2] == 2
      
     if [feature 9 numeric 0] < 0.7000097673050265
          Leaf weight = {15179.304836738002,2822.0294869191534}{0.0,0.0}
     if [feature 9 numeric 1] >= 0.7000097673050265
          Leaf weight = {0.0,723.9705130808464}{0.0,0.0}
   if [feature 4 nominal 3] == 3
      
     if [feature 5 numeric 0] < 0.9994316035286038
          
       if [feature 2 nominal 0] == 0
              Leaf weight = {532.0,2263.0}{0.0,0.0}
       if [feature 2 nominal 1] == 1
              Leaf weight = {1032.0,1775.0}{0.0,0.0}
       if [feature 2 nominal 2] == 2
              Leaf weight = {1523.0,1256.0}{0.0,0.0}
       if [feature 2 nominal 3] == 3
              Leaf weight = {2010.0,731.0}{0.0,0.0}
       if [feature 2 nominal 4] == 4
              Leaf weight = {1038.0,1759.0}{0.0,0.0}
     if [feature 5 numeric 1] >= 0.9994316035286038
          Leaf weight = {644.5861826809612,8.0}{0.0,0.0}
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

Accuracy 0.715
FileReader RDD count:10000
==============================
Counter: 110000
Chunk: 10000
Over 100.000 instances
Running time = 108.911365905
Reading file stopped.
