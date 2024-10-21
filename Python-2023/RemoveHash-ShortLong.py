import os

def DeleteFiles():
    OutputFileShort=open('/Users/dhanashreenangre/Desktop/studies/BIS/W3SVC1/Outputshort.csv', 'w')
    OutputFileLong=open('/Users/dhanashreenangre/Desktop/studies/BIS/W3SVC1/Outputlong.csv', 'w')


def CleanHash(filename):
    type=filename[-3:len(filename)]
    if (type=="log"):
    
        OutputFileShort=open('/Users/dhanashreenangre/Desktop/studies/BIS/W3SVC1/Outputshort.csv', 'a')
        OutputFileLong=open('/Users/dhanashreenangre/Desktop/studies/BIS/W3SVC1/Outputlong.csv', 'a')

        InFile = open('/Users/dhanashreenangre/Desktop/studies/BIS/W3SVC1/'+filename,'r')
        print(filename)
    
        Lines= InFile.readlines()
        for line in Lines:
            if (line[0]!="#"):
                Split=line.split(" ")
                
                if (len(Split)==14):
                   
                   OutputFileShort.write(line.replace(" ", ","))
                else:
                   if (len(Split)==18):
                       OutputFileLong.write(line.replace(" ", ","))
                   else:
                       print ("Fault "+str(len(Split)))
                
                

arr=os.listdir("/Users/dhanashreenangre/Desktop/studies/BIS/W3SVC1/")
DeleteFiles()
for f in arr:
    CleanHash(f)
