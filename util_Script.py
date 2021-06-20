import sys, os, builtins
import importlib.util as iplu
from ast import literal_eval
from pprint import pprint as pp
import pandas as pd
import util_Path as UP


class ScriptSequence:
    EXT = '.xlsx'
    INDEX = 'index'
    LITERALs = ['STEP_ID', 'PARAM']
    UKEYs = ['PROJ_ID', 'FLOW_ID', 'STEP_ID']

    def __init__(self, path, name, sequence=None, **kwargs):
        self._df_index = None
        self._attr_dict = None
        self._seq = dict()
        self._path = ''
        self._name = ''
        self._pathname = ''
        self.path = path
        self.name = name
        self.sequence = sequence
        kwargs.update(self.__dict__)
        self.__dict__ = kwargs

    @property
    def path(self):
        return self._path

    @property
    def name(self):
        return self._name

    @property
    def pathname(self):
        return self._pathname

    @path.setter
    def path(self, value):
        if value and value != self._path:
            self._path = value
            self.pathname = find_specific_file_name(self._name, ScriptSequence.EXT, value, verbose=False)

    @name.setter
    def name(self, value):
        temp, _ = os.path.splitext(value)
        if value and temp != self._name:
            self._name = value
            self.pathname = find_specific_file_name(value, ScriptSequence.EXT, self._path, verbose=False)

    @pathname.setter
    def pathname(self, value):
        if value and value != self._pathname and self._name and self._path:
            self._pathname = value
            print('Pathname loaded')
            self._update_source = True
    # ------------------------------------------------------------------------------------------------------------------

    @property
    def sequence(self):
        return self._seq

    @sequence.setter
    def sequence(self, value):
        # direct assignment will overwrite loading from path
        if value and value != self._seq:
            self._seq = value
    # ------------------------------------------------------------------------------------------------------------------

    @property
    def _update_source(self):
        return self._update_source

    @ _update_source.setter
    def _update_source(self, value):
        # Load from excel for pathname and set attr_dict and index
        if value:
            import pandas as pd
            df = pd.read_excel(self._pathname, sheet_name=0, dtype=str, index_col=None)
            self.index = ScriptSequence.parse_literal_columns(df)
            df_dict = pd.read_excel(self._pathname, sheet_name=None, dtype=str, index_col=None)
            self._attr_dict = {k: ScriptSequence.parse_literal_columns(v) for k, v in df_dict.items()}
            print('Source file loaded')
            self._parse_source = True
    # ------------------------------------------------------------------------------------------------------------------

    @property
    def index(self):
        return self._df_index

    @property
    def _parse_source(self):
        return self._parse_source

    @index.setter
    def index(self, value):
        if value is not None:
            temp = value.loc[:, [ScriptSequence.INDEX] + ScriptSequence.UKEYs]
            self._df_index = temp.set_index(ScriptSequence.INDEX)

    @_parse_source.setter
    def _parse_source(self, value):
        if value is not None:
            # parse DataFrames from imported by pathname
            self._parse_attr()
            # get dicts from DataFrames
            self._df_index = self._df_index.to_dict('index')
            self._df_SCRIPT = self._df_SCRIPT.to_dict('index')
            self._get_path_dicts('_df_INPUTS')
            self._get_path_dicts('_df_OUTPUT')
            # get dicts from DataFrames
            self._combine_sequence()
            print('Source file parsed')
            self._attr_dict = None
    # ------------------------------------------------------------------------------------------------------------------

    def _parse_attr(self):
        # Get attr based on based on attr_name, check if consistent and remove redundant columns
        for attr_name, attr_value in self._attr_dict.items():
            attr_value = attr_value.set_index(ScriptSequence.INDEX)
            if self._check_index(attr_value):
                attr_value = attr_value.drop(columns=ScriptSequence.UKEYs)
                self.__setattr__('_df_' + attr_name, attr_value)
        self._keys = ['_df_' + e for e in self._attr_dict.keys()]

    def _check_index(self, value):
        if self._df_index is not None:
            temp = (self._df_index.loc[:, ScriptSequence.UKEYs] == value.loc[:, ScriptSequence.UKEYs])
            return temp.all().all()
        else:
            return True

    def _get_path_dicts(self, attr_name):
        # Get attr based on based on attr_name, obtain {'main': {'NAME': XXX, 'PATH': XXX}}
        # instead of {'main_NAME': XXX, 'main_PATH': XXX}
        attr_value = self.__getattribute__(attr_name)
        attr_value.columns = pd.MultiIndex.from_tuples(tuple(attr_value.columns.str.split('_')))
        attr_value = attr_value.stack(0).to_dict('index')
        temp = dict()
        for (k1, k2), v in attr_value.items():
            temp.setdefault(k1, dict())
            temp[k1].update({k2: v})
        self.__setattr__(attr_name, temp)

    @staticmethod
    def parse_literal_columns(dataframe):
        labels = dataframe.columns.to_list()
        labels = [e for e in labels if e in ScriptSequence.LITERALs]
        for e in labels:
            dataframe[e] = dataframe[e].apply(lambda x: literal_eval(x))
        return dataframe

    def _combine_sequence(self):
        self._seq = dict()
        for k, v in self._df_index.items():
            self._seq[k] = dict(INDEX=self._df_index[k], SCRIPT=self._df_SCRIPT[k],
                                INPUTS=self._df_INPUTS[k], OUTPUT=self._df_OUTPUT[k])

    def ppcheck(self):
        for k, v in self._seq.items():
            print(k)
            pp(v)

    def execute(self, runtime_object):
        for k, v in self._seq.items():
            UP.stylized_print2(f' SEQUENCE ID: {k}')
            runtime_object.ID = k
            runtime_object.PARAM = v
            runtime_object.execute()


class ScriptRunTime:

    def __init__(self, **kwargs):
        self.MODULE = kwargs['MODULE']
        self.FLOW_ID = kwargs.pop('FLOW_ID', None)
        self.BASE_PATH = kwargs.pop('BASE_PATH', None)
        self.SCRIPT_PATH = kwargs.pop('SCRIPT_PATH', None)
        self.ID = None
        self.PARAM = None
        self.RT_ID = None
        self._SEQ_ID = None
        self._SEQ_PARAM = None

    @property
    def ID(self):
        return self._SEQ_ID

    @ID.setter
    def ID(self, value):
        self._SEQ_ID = value

    @property
    def PARAM(self):
        return self._SEQ_PARAM

    @PARAM.setter
    def PARAM(self, kwargs):
        if isinstance(kwargs, dict):
            self._SEQ_PARAM = kwargs
            self._get_seq_attr('SCRIPT', kwargs)
            self._parse_runtime_index('INDEX', kwargs)
            self._parse_runtime_input('INPUTS', kwargs)
            self._parse_runtime_output('OUTPUT', kwargs)

    def _get_seq_attr(self, attr_name, kwargs):
        self.__setattr__(attr_name, kwargs[attr_name])

    def _parse_runtime_index(self, attr_name, kwargs):
        attr_value = kwargs[attr_name].copy()
        indexes = map(str, list(kwargs[attr_name].values()))
        self.RT_ID = '\''.join(indexes)
        attr_value.update({'RT_ID': self.RT_ID})
        self.__setattr__(attr_name, attr_value)

    def _parse_runtime_input(self, attr_name, kwargs):
        attr_value = kwargs[attr_name]
        for k, v in kwargs[attr_name].items():
            name, ext = os.path.splitext(v['NAME'])
            path = v['PATH']
            # find the specific file
            print(UP.Style.UDLBLD + f'INPUT file ["{k}"]' + UP.Style.END + f': "{name}" with extension "{ext}".')
            pathname = find_specific_file_name(name, ext, path, compulsory=True)
            attr_value[k].update({'PATH_NAME': pathname})
        self.__setattr__(attr_name, attr_value)

    def _parse_runtime_output(self, attr_name, kwargs):
        attr_value = kwargs[attr_name]
        for k, v in kwargs[attr_name].items():
            name, ext = os.path.splitext(v['NAME'])
            path = v['PATH']
            # Create directory if path not exist
            print(UP.Style.UDLBLD + f'OUTPUT file ["{k}"]' + UP.Style.END + f': "{name}" with extension "{ext}".')
            UP.check_file_path_exist([path])
            oput_join = os.path.join(path)
            # parse the output name
            pathname = UP.get_output_name(oput_join, name, ext, self.RT_ID)
            print(f'File created: "{UP.get_relative_path(pathname)}"\n')
            # delete if output file exist
            if not builtins.GLOBAL_VERBOSE:
                print(UP.Style.ITL + f'Builtin Verbose: {builtins.GLOBAL_VERBOSE}' + UP.Style.END)
            UP.delete_similar_outputs(oput_join, name, ext, builtins.GLOBAL_VERBOSE)
            attr_value[k].update({'PATH_NAME': pathname})
        self.__setattr__(attr_name, attr_value)

    def execute(self):
        if not self.FLOW_ID or self.FLOW_ID == self.INDEX['FLOW_ID']:
            name = self.SCRIPT['NAME']
            path = self.SCRIPT['PATH']

            UP.stylized_print1(f'"{self.RT_ID}: {name}" Start')

            print(UP.Style.UDLBLD + 'Listing script param(s)' + UP.Style.END + ':')
            pp(self.SCRIPT['PARAM'])
            print()
            self.MODULE.PROJ_PARAMS = self.SCRIPT['PARAM']
            self.MODULE.PROJ_INPUTS = self.INPUTS
            self.MODULE.PROJ_OUTPUT = self.OUTPUT
            path_name = find_specific_file_name(name, '.py', path, verbose=False)
            _, name_ext = os.path.split(path_name)
            spec = iplu.spec_from_file_location(name_ext, path_name)
            module = iplu.module_from_spec(spec)
            spec.loader.exec_module(module)

            UP.stylized_print1(f'"{self.RT_ID}: {name}" Done')


def find_specific_file_name(file_target, ext_target, path_target='.', file_hint=None, compulsory=True, verbose=True):
    """Return filename (string) of one specific file based on 'Generic Name' and its extension.
    The script exit when multiple file or no file found, if the file is flagged as compulsory.
    Additional hint can be provided if there maybe multi files with filename specified."""
    path_found = []
    hint_str = f' with "{file_hint}"' if file_hint else ''
    for path in os.listdir(os.path.join(path_target)):
        name, ext = os.path.splitext(path)
        if file_target in name and ext == ext_target and '~' not in name:
            if (file_hint and file_hint in name) or not file_hint:
                path_found.append(os.path.join(path_target, path))

    if len(path_found) == 1:
        if verbose:
            print(f'File found: "{UP.get_relative_path(path_found[0])}"')
        return path_found[0]
    elif compulsory:
        if len(path_found) == 0:
            raise FileNotFoundError(f'"{file_target}{ext_target}" in "{path_target}"{hint_str}')
        elif len(path_found) > 0:
            print(f'"{file_target}{ext_target}" in "{path_target}"{hint_str}')
            pp(path_found, width=200)
            raise FileNotFoundError('Multiple files found')
        sys.exit()
    else:
        print(f'No specific file found: "{file_target}{ext_target}" in "{path_target}"{hint_str}')
        return None


def import_source(name, path):
    path = UP.Get_Specific_File_Name(name, '.py', path, verbose=False)
    spec = iplu.spec_from_file_location(name, path)
    module = iplu.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)

# def _recurring_seq_attr(self, kwargs, parent_key='', sep='_'):
#     def flatten(d, parent_key, sep):
#         items = []
#         for k, v in d.items():
#             new_key = parent_key + sep + k if parent_key else k
#             if isinstance(v, collections.MutableMapping):
#                 items.extend(flatten(v, new_key, sep=sep).items())
#             else:
#                 items.append((new_key, v))
#         return dict(items)
#
#     for k, v in flatten(kwargs, parent_key, sep).items():
#         self.__setattr__(k, v)