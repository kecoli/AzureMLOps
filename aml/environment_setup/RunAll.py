# CGA AML model template
# Author: CGA
# Version: 0.1
# Last modified Date: 02012020



from __future__ import print_function
import sys, os, time
import subprocess
from subprocess import check_output, CalledProcessError

###################################
## get model to deploy
###################################
model = sys.argv[1]
env = sys.argv[2]
svcpw = sys.argv[3]

cmd = 'python ./' + str(model) + '/aml_service/Run.py ' + env + ' ' + svcpw
print("cmd is {0}".format(cmd))

try:   
    output = subprocess.check_output(cmd, shell=True)
    print('Python Run.py Output: ', output)
    print('Call RunAll.py Completed {0}'.format(cmd))
except:
    print("Deployment Failed")
    raise 
