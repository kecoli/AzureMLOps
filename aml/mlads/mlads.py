###########  Feb 23, 2020 klei ##################
##  Common python modules  ##
#################################################

import sys,os,argparse
import heapq 
import pandas as pd
import numpy as np



###########  Feb 23, 2020 klei ###################
##  Parsing parameter from AML python template  ##
##################################################

parser = argparse.ArgumentParser()
parser.add_argument('--input_path', type=str, dest='input_path', help='data folder mounting point')
parser.add_argument('--output_path', type=str, dest='output_path', help='data folder mounting point')
parser.add_argument('--param1', type=str, dest='datekey', help='pipeline parameter')
args = parser.parse_args()

print('cwd:',os.getcwd())

input_path = args.input_path
print('input_path folder:', input_path)
print('os.listdir(input_path):',os.listdir(input_path))

output_path = args.output_path
os.makedirs(output_path, exist_ok=True)
print('output_path folder:', output_path)
print('os.listdir(output_path):',os.listdir(output_path))

datekey = args.datekey
print(datekey)


###########  Feb 23, 2020 klei ##################
##  Create and save files under "outputs path" 
##  only if you want to read the file from AML web portal
#################################################
# outputs_path = "./outputs"
# os.makedirs(outputs_path, exist_ok=True)
# os.makedirs(output_path, exist_ok=True)



###########  Feb 23, 2020 klei ##################
##  Main body of the model scripts
##  For Monitor Azure ML experiment runs and metrics
##  https://docs.microsoft.com/en-us/azure/machine-learning/how-to-track-experiments
#################################################

data = pd.read_csv(input_path+'/mlads_1.tsv' , sep='\t',header=None)

print(data.head())
 

###########  Feb 23, 2020 klei ##################
##  Write output files under output_path  ##
#################################################
data.to_csv(output_path + '/mlads_output.tsv', header=True, sep='\t', index=False)




