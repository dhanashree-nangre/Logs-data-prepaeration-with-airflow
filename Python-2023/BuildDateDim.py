
InFile = open('/Users/andy/Dropbox/DataEngineering/2022-jan/ac52048-jan/Assignment/W3SVC1/OutFact1.txt', 'r')
OutputFile=open('/Users/andy/Dropbox/DataEngineering/2022-jan/ac52048-jan/Assignment/W3SVC1/DimDate.txt', 'w')

Lines= InFile.readlines()
for line in Lines:
    Split=line.split(",")
    Out=Split[0]+"\n"
    OutputFile.write(Out)