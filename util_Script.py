import sys, os, builtins
import importlib.util as implib
import util_Path as UP

class ScriptSequence(*args, **kwargs):
    print(args)



# proj ID
# flow ID
# step ID
# script name
# script path
# input file name ID
# input file path
# output file name ID
# output file path



def parse_step_(path, name) -> dict:
	import pandas as pd
	path = Get_Specific_File_Name()
	df = pd.read_excel(path, dtype=str,)
	dictDF = df.to_dict('record')

def import_source(name, path):
    path = UP.Get_Specific_File_Name(name, '.py', path, verbose=False)
    spec = implib.spec_from_file_location(name, path)
    module = implib.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
