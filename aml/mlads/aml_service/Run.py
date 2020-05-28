# CGA AML model template
# Author: CGAML
# Version: 1
# Last modified Date: 02012020
from __future__ import print_function
import sys, os, time
import json
from _ast import If

exec(open("./environment_setup/mlads_util.py").read())


###################################
##  Parameters passed from Azure Devops  
##  env:  'dev' or 'prod'
##  svcprpassword: service principle password, will be shown as ***** 
###################################

## Change isTest to True when you are testing
isTest = True

if isTest:
    env = 'dev'
    svcprpassword = None
    interactive  = True
else:
    env = sys.argv[1]
    svcprpassword = sys.argv[2]
    interactive = False
###################################
##  Setup Model Steps from Here
###################################

model_name = 'mlads'  # should be same as folder name in ADO


###################################
##  Include InitAML Call Always
##  InitAML is loaded from cga_aml_util.py
###################################
datastore, compute_target, ws, data_factory = InitAML(model_name, env, svcprpassword, interactive=interactive)


###################################
##  Input Data 
###################################

input_data_1 = "mlads_input"

blob_input_data_1 = DataReference(
                                    datastore=datastore,
                                    data_reference_name=input_data_1,
                                    path_on_datastore="")


###################################
##  Model Object/Temporary Data 
###################################
# model_data_1 = "trained_model"
# model_path_1 = PipelineData(
#                             model_data_1,
#                             datastore=datastore
#                             )

###################################
##  Output Data 
###################################

output_data_1 = "mlads_output"
blob_output_data_1 =  PipelineData(
                                output_data_1,                     
                                datastore=datastore
                                )


###################################
##  Copy of Output Data 
##  copy data from 'azureml/runid' folder to 'current' folder
###################################

blob_data_ref_1 = DataReference(
    datastore=datastore,
    data_reference_name='copy_output_data',
    path_on_datastore= '/current')



###################################
##  Additional arg to pass 
###################################
Datekey_Param = PipelineParameter(
                                  name="datekey",
                                  default_value="20191101"
                                  )

###################################
##  Python package configuration 
###################################

cd = CondaDependencies.create(pip_packages=['azureml-sdk', 'scikit-learn', 'azureml-dataprep[pandas,fuse]>=1.1.14', 'tensorflow==1.14.0', 'matplotlib','seaborn'])
rc = runconfig.RunConfiguration(conda_dependencies=cd)
rc.environment.docker.enabled = True


###################################
##  Pipeline configuration 
##  PythonScriptStep, RScriptStep, DataTransferStep
##  DataTransferStep is to copy data from 'aml\runid' to 'current' folder
###################################

ModelStep = PythonScriptStep(
                                name="ModelStep",
                                script_name="mlads.py",
                                runconfig=rc,
                                arguments=["--input_path", blob_input_data_1,"--output_path", blob_output_data_1, "--param1", Datekey_Param],
                                inputs=[blob_input_data_1],
                                outputs=[blob_output_data_1],
                                compute_target=compute_target,
                                source_directory='./' + model_name,
                                allow_reuse=False
                                )



transfer_to_adls = DataTransferStep(
    name='transfer_blob',
    source_data_reference=blob_output_data_1,
    source_reference_type='directory',
    destination_data_reference=blob_data_ref_1,
    destination_reference_type='directory',
    compute_target=data_factory)

allsteps = [ModelStep,transfer_to_adls]


###################################
##  Submit pipeline to run
###################################

pipeline_1 = Pipeline(workspace=ws, steps=[allsteps])

if isTest:
    experiment_name = model_name + '_experiment'
    pipeline_run_1 = Experiment(ws, experiment_name).submit(pipeline_1)

    ## if you want to see the real time log from the python console:
    # pipeline_run_1.wait_for_completion()
    print("pipeline built, submitted for testing purpose (isTest=True)")

else: 
    
    print("pipeline built, ready for publish")
    
    ###################################
    ##  Pipeline Publish
    ###################################
    published_pipeline1 = pipeline_1.publish(name=model_name + '_pipeline', description="Published Pipeline Description", continue_on_step_failure=True)
    
    print("pipeline published")
    
    print("pipeline published and Id is {0}".format(published_pipeline1.id))
    
    print("pipeline published and name is {0}".format(published_pipeline1.name))
    
    
    ###################################
    ##  Pipeline Deployment to ADF
    ##  this step is to log the pipeline run id to a file
    ##  that the ADF can recognize the latest pipeline run id
    ###################################
    
    local_Dir = f'./amlmetadata/{model_name}'
    if os.path.isdir(local_Dir):
        print("Directory is here")
    else:
        os.makedirs(local_Dir)
    
    meta_pipeline = {'modelname': model_name, 'latestpipelinerunid': published_pipeline1.id}
    print (meta_pipeline)
    
    with open(f'{local_Dir}/{model_name}_pipeline_id.json','w+') as f:  
        json.dump(meta_pipeline,f)
        
    try:
        datastore.upload(src_dir=f'./amlmetadata/{model_name}', 
                        target_path=f'amlmetadata/{model_name}',
                        overwrite=True, 
                        show_progress=True)
    except:
        print("PipelineId Upload Failed.")
        raise
    
    ###################################
    ## END
    ###################################
    print("pipeline completed")