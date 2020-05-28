import azureml.core
import azureml.core.compute
import os
import json
from azureml.core import *
from azureml.core.compute import *
from azureml.core import Workspace
from azureml.core.datastore import Datastore
from azureml.core.authentication import ServicePrincipalAuthentication
from azureml.core.environment import Environment
from azureml.core.conda_dependencies import CondaDependencies
from azureml.core.dataset import Dataset
from azureml.train.estimator import Estimator
from azureml.core import runconfig
from azureml.pipeline.core.graph import PipelineParameter
from azureml.pipeline.steps import PythonScriptStep
from azureml.pipeline.steps import RScriptStep
from azure.keyvault import KeyVaultClient
from azure.common.credentials import ServicePrincipalCredentials
from azureml.core.authentication import InteractiveLoginAuthentication
from azureml.data.data_reference import DataReference
from azureml.pipeline.core import PipelineData
from azureml.pipeline.core import Pipeline
from configparser import ConfigParser
from azureml.core.compute import ComputeTarget, DataFactoryCompute
from azureml.exceptions import ComputeTargetException
from azureml.pipeline.steps import DataTransferStep

print("Azure ML SDK Version: ", azureml.core.VERSION)
print("Current paht:", os.getcwd())

def InitAML(model_name, env, svcpw, interactive=False,create_ws=False):

    print("Environment is  ", env)
    
    configFilePath = "./environment_setup/Config/config_" + env + ".ini"
    configFile = ConfigParser()
    configFile.read(configFilePath)

    svc_pr_pd = svcpw
    tenant_id = configFile.get('PARAMS','tenant_id')
    service_principal_id = configFile.get('PARAMS','service_principal_id')
    subscription_id = configFile.get('PARAMS','subscription_id')
    resource_group = configFile.get('PARAMS','resource_group')
    blobname = configFile.get('PARAMS','BlobName')        
    workspace_name = configFile.get('PARAMS','WorkSpace')
    data_factory_name = configFile.get('PARAMS','Data_factory_name')
    location = configFile.get('PARAMS','location')

    fp = './' + model_name + '/aml_service/setup.ini'
    conf = ConfigParser()
    conf.read(fp)

    AML_COMPUTE_CLUSTER_NAME = conf.get('PARAMS','AML_COMPUTE_CLUSTER_NAME')
    AML_COMPUTE_CLUSTER_MIN_NODES = conf.get('PARAMS','AML_COMPUTE_CLUSTER_MIN_NODES')
    AML_COMPUTE_CLUSTER_MAX_NODES = conf.get('PARAMS','AML_COMPUTE_CLUSTER_MAX_NODES')
    AML_COMPUTE_CLUSTER_SKU = conf.get('PARAMS','AML_COMPUTE_CLUSTER_SKU')

    if interactive:
        auth = InteractiveLoginAuthentication(tenant_id=tenant_id)

    else: 
        auth = ServicePrincipalAuthentication(
            tenant_id=tenant_id,
            service_principal_id=service_principal_id,
            service_principal_password=svc_pr_pd)
   
    subscription_id = subscription_id
    resource_group = resource_group

    try:
        ws = Workspace(            
            subscription_id=subscription_id,
            resource_group=resource_group,
            workspace_name=workspace_name,
            auth=auth
            )
        print('Library configuration succeeded')
    except:
        if  create_ws:
            ws = Workspace.create(name=workspace_name,
                                  auth=auth,
                                  subscription_id=subscription_id,
                                  resource_group=resource_group,
                                  create_resource_group=False,
                                  location=location
                     )
            print('Workspace not found and is created')
        else: 
            print('Workspace not found and not created')
    
    print('workspace_name:',ws.name, '\nworkspace_location:',ws.location, '\nworkspace_resource_group:',ws.resource_group, sep='\t')
    
    
    # choose a name for your cluster
    compute_name = os.environ.get("AML_COMPUTE_CLUSTER_NAME", AML_COMPUTE_CLUSTER_NAME)
   
    if compute_name in ws.compute_targets:
        compute_target = ws.compute_targets[compute_name]
        if compute_target and type(compute_target) is AmlCompute:
            print('found compute target. just use it. ' + compute_name)
    else:
        print('creating a new compute target...')
        compute_min_nodes = os.environ.get("AML_COMPUTE_CLUSTER_MIN_NODES", AML_COMPUTE_CLUSTER_MIN_NODES)
        compute_max_nodes = os.environ.get("AML_COMPUTE_CLUSTER_MAX_NODES", AML_COMPUTE_CLUSTER_MAX_NODES)
    # This example uses CPU VM. For using GPU VM, set SKU to STANDARD_NC6
        vm_size = os.environ.get("AML_COMPUTE_CLUSTER_SKU", AML_COMPUTE_CLUSTER_SKU)
        provisioning_config = AmlCompute.provisioning_configuration(vm_size=vm_size,
                                                                    min_nodes=compute_min_nodes,
                                                                    max_nodes=compute_max_nodes)
    
        # create the cluster
        compute_target = ComputeTarget.create(
            ws, compute_name, provisioning_config)
    
        # can poll for a minimum number of nodes and for a specific timeout.
        # if no min node count is provided it will use the scale settings for the cluster
        compute_target.wait_for_completion(
            show_output=True, min_node_count=None, timeout_in_minutes=20)
    
        # For a more detailed view of current AmlCompute status, use get_status()
        print(compute_target.get_status().serialize())
    
   
    try:
        datastore = Datastore(ws, name = blobname)
        print("Found Blob Datastore with name: %s" % datastore)
    except:
        print("No datastore with name: %s" % blobname)
        sys.exit(-1)

    try:
        data_factory = DataFactoryCompute(ws, data_factory_name)
        print('data_factory ', data_factory)
    except ComputeTargetException as e:
        if 'ComputeTargetNotFound' in e.message:
            print('Data factory Compute not found, creating...')
            provisioning_config = DataFactoryCompute.provisioning_configuration()
            data_factory = ComputeTarget.create(ws, data_factory_name, provisioning_config)
            data_factory.wait_for_completion()
        else:
            print('Data factory Compute not found, Entering Else Section...')
            raise e


    return datastore, compute_target, ws, data_factory
   

def InitAMLKeyVault(model_name, env, svcpw, interactive=False,create_ws=False):

    configFilePath = "./environment_setup/Config/config_" + env + ".ini"
    configFile = ConfigParser()
    configFile.read(configFilePath)
    
    keyVaultUrl = configFile.get('PARAMS','keyVaultUrl')

    datastore, compute_target, ws, data_factory = InitAML(model_name, env, svcpw, interactive=interactive)
    
    return datastore, compute_target, ws, data_factory, keyVaultUrl