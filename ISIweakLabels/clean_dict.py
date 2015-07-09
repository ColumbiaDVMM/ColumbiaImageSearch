__author__ = 'svebor'
import re
import pickle

visualvaluesdict=pickle.load(open("visualvaluesdict.p","r"))
setvisualkeys=pickle.load(open("setvisualkeys.p","r"))
listvisualkeys=list(setvisualkeys)
cleanvisualvaluesdict=dict.fromkeys(listvisualkeys)
for visualkey in setvisualkeys:
    cleanvisualvaluesdict[visualkey]=set()

for key in range(len(listvisualkeys)):
    this_values=visualvaluesdict[listvisualkeys[key]]
    for one_val in this_values:
        if listvisualkeys[key]=='person_build_feature':
            m = re.search('brstrong.*', one_val)
            if m:
                cleanval=re.sub(m.group(0), '', one_val)
            else:
                cleanval=one_val
        else:
            cleanval=one_val
        cleanvisualvaluesdict[listvisualkeys[key]]=cleanvisualvaluesdict[listvisualkeys[key]].union([cleanval])

print cleanvisualvaluesdict
pickle.dump(cleanvisualvaluesdict,open( "cleanvisualvaluesdict.p", "wb" ))

