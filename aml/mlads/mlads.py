###########  Feb 23, 2020 klei ##################
##  Common python modules  ##
#################################################
import sys,os,argparse
import heapq 
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from surprise import NormalPredictor, Reader, Dataset, accuracy, SVD, SVDpp, KNNBasic, CoClustering, SlopeOne
from surprise.model_selection import train_test_split


###########  Feb 23, 2020 klei ###################
##  Parsing parameter from AML python template  ##
##################################################

parser = argparse.ArgumentParser()
parser.add_argument('--input_path', type=str, dest='input_path', help='data folder mounting point')
parser.add_argument('--output_path', type=str, dest='output_path', help='data folder mounting point')
parser.add_argument('--param1', type=str, dest='dateKey', help='pipeline parameter')
args = parser.parse_args()

print('cwd:',os.getcwd())

input_path = args.input_path
print('input_path folder:', input_path)
print('os.listdir(input_path):',os.listdir(input_path))

output_path = args.output_path
os.makedirs(output_path, exist_ok=True)
print('output_path folder:', output_path)
print('os.listdir(output_path):',os.listdir(output_path))

dateKey = args.dateKey
print(dateKey)


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

data = pd.read_csv(input_path+'/mlads_1.tsv' , sep='\t',header=0)

# data = pd.read_csv('mlads_1.tsv' , sep='\t',header=0)

scaler = StandardScaler()

data['Rating'] = scaler.fit_transform(np.array(data['NormalizedUnits_UsageDateRate']).reshape(-1,1))

print(data.head())

df = data.rename({'SubscriptionIndex': 'customer_unique_id', 'Rating':'estimator', 'ServiceName':'productId'},axis=1)


reader = Reader(rating_scale=(df['estimator'].min(), df['estimator'].max()))
                
data = Dataset.load_from_df(df[['customer_unique_id', 'productId', 'estimator']], reader)

train_set, test_set = train_test_split(data, test_size=0.2, random_state=1)


def model(train_set, test_set):
    params = {'n_factors': 3, 'n_epochs': 50, 'lr_all': 0.01, 
              'reg_all': 0.1} 
    
    svdpp = SVDpp(n_factors=params['n_factors'], 
                    n_epochs=params['n_epochs'],
                    lr_all=params['lr_all'], 
                    reg_all=params['reg_all'])
    svdpp.fit(train_set)
    
    predictions = svdpp.test(test_set)
    rmse = accuracy.rmse(predictions,verbose=False)
            
    return predictions, rmse
    
predictions, _ = model(train_set,test_set)

output = pd.DataFrame(predictions)
output = output.sort_values(by=['uid','est'],ascending=[1,0])
output = pd.DataFrame({"DateKey":dateKey,"SubscriptionIndex":output["uid"],"Recommendation":output["iid"],"Score":output["est"]})

###########  Feb 23, 2020 klei ##################
##  Write output files under output_path  ##
#################################################
output.to_csv(output_path + '/mlads_output.tsv', header=True, sep='\t', index=False)




