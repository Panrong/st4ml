import sys
import glob
if( len(sys.argv)) < 2:
	print("Usage: \n >> python combine.py <path to result folder>")
	sys.exit()
path = sys.argv[1]
if(path[-1] == "/"): path = path[:-1]
file_list = glob.glob(path + "/*.csv")
f1 = open(path + "/combined.csv", 'w')
for (idx, i) in enumerate(file_list):
    with open(i) as f2:
        if(idx == 0):
            for l in f2.readlines():
                f1.write(l)
        else:
            for l in f2.readlines()[1:]:
                f1.write(l)
f1.close()
print("Combination done. The combined file is %s/combined.csv"%path)
