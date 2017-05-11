  # replacements = {'class1,':'0', 'class2,':'1'}
import sys, getopt

def main(argv):
   inputfile = ''
   outputfile = ''
   try:
      opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
   except getopt.GetoptError:
      print 'test.py -i <inputfile> -o <outputfile>'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print 'test.py -i <inputfile> -o <outputfile>'
         sys.exit()
      elif opt in ("-i", "--ifile"):
         inputfile = arg
      elif opt in ("-o", "--ofile"):
         outputfile = arg
   print 'Input file is "', inputfile
   print 'Output file is "', outputfile

   path = '/Users/minhnguyen/StreamingAlgo/StreamDM/streamDM/scripts/result/'
   inputpath = path + inputfile + '.res'
   outputpath = path  + outputfile + '.accuracy'

   with open(inputpath) as infile, open(outputpath, 'w') as outfile:
    sum = 0.0
    counter = 0
    for line in infile:
        accuracy = ''
        # compute the average accuracy
        if 'Accuracy ' in line:
            accuracy = line[9:]
            sum += float(accuracy[:len(accuracy)-1])
            counter += 1
        outfile.write(accuracy)
    average = sum/counter
    outfile.write("Average: " +str(average))    
    print "Average accuracy: " , sum/counter


if __name__ == "__main__":
   main(sys.argv[1:])






