# replacements = {'class1,':'0', 'class2,':'1'}
filename = 'majcl5_t'
path = '/Users/minhnguyen/StreamingAlgo/StreamDM/streamDM/scripts/result/'
infile = path + filename + '.res'
outfile = path + filename + '.accuracy'

with open(infile) as infile, open(outfile, 'w') as outfile:
    sum = 0.0
    counter = 0
    for line in infile:
    	accuracy = ''
    	
        if 'Accuracy ' in line:
        	accuracy = line[9:]
        	sum += float(accuracy[:len(accuracy)-1])
        	counter += 1
        outfile.write(accuracy)
    print(sum/counter)


