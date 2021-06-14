import sys, os, builtins
import importlib.util as iplu
from ast import literal_eval
from pprint import pprint as pp
import pandas as pd
import util_Path as UP


class ScriptSequence:
    check = ['PROJ_ID', 'FLOW_ID', 'STEP_ID']

    def __init__(self, **kwargs):
        self.path = None
        self.name = None
        self.sequence = None
        self.seq_attr = kwargs

    @property
    def seq_attr(self):
        return self.seq_attr

    @property
    def _refresh(self):
        return self._inputs

    @seq_attr.setter
    def seq_attr(self, kwargs):
        # set if self have no path or new path is provided
        path = kwargs.get('path', None)
        name = kwargs.get('name', None)
        sequence = kwargs.get('sequence', None)

        # loading excel based on path and name pair
        self.path = path if not self.path or path else self.path
        if not self.name or name:
            self.name, _ = os.path.splitext(name)
        if path or name:
            self._pathname = UP.Get_Specific_File_Name(self.name, '.xlsx', [self.path], verbose=False)
            self._refresh = True

        # direct assignment will overwrite loading from path
        self.sequence = sequence if not isinstance(self.sequence, dict) or sequence else self.sequence

    @_refresh.setter
    def _refresh(self, value):
        if value:
            self._load_sequence_source('SCRIPT')
            self._load_sequence_source('INPUTS')
            self._load_sequence_source('OUTPUT')
            self.INDEX = self.SCRIPT.loc[:, ['index'] + ScriptSequence.check].set_index('index')
            self._check_sequence_source('SCRIPT')
            self._check_sequence_source('INPUTS')
            self._check_sequence_source('OUTPUT')
            self._parse_literal('SCRIPT', 'PARAM')
            self._parse_sequence_paths('INPUTS')
            self._parse_sequence_paths('OUTPUT')
            self._parse_literal('INDEX', 'STEP_ID')
            self._combine_sequence()
            self._refresh = False

    def _load_sequence_source(self, attr_name):
        import pandas as pd
        # Load from excel for each attr_name and set attr based on attr_name
        attr_value = pd.read_excel(self._pathname, sheet_name=attr_name, dtype=str, index_col=None)
        self.__setattr__(attr_name, attr_value)

    def _check_sequence_source(self, attr_name):
        # Get attr based on based on attr_name, check if consistent and remove redundant columns
        check = ScriptSequence.check
        attr_value = self.__getattribute__(attr_name)
        attr_value = attr_value.set_index('index')
        assert (self.INDEX.loc[:, check] == attr_value.loc[:, check]).all().all()
        attr_value = attr_value.drop(columns=check)
        self.__setattr__(attr_name, attr_value)

    def _parse_sequence_paths(self, attr_name):
        # Get attr based on based on attr_name, obtain {'main': {'NAME': XXX, 'PATH': XXX}}
        # instead of {'main_NAME': XXX, 'main_PATH': XXX}
        attr_value = self.__getattribute__(attr_name)
        attr_value.columns = pd.MultiIndex.from_tuples(tuple(attr_value.columns.str.split('_')))
        attr_value = attr_value.stack(0)
        attr_value = attr_value.to_dict('index')
        temp = dict()
        for (k1, k2), v in attr_value.items():
            temp.setdefault(k1, dict())
            temp[k1].update({k2: v})
        attr_value = temp
        self.__setattr__(attr_name, attr_value)

    def _parse_literal(self, attr_name, col_name):
        attr_value = self.__getattribute__(attr_name)
        attr_value[col_name] = attr_value[col_name].apply(lambda x: literal_eval(x))
        self.__setattr__(attr_name, attr_value)

    def _combine_sequence(self):
        self.INDEX = self.INDEX.to_dict('index')
        self.SCRIPT = self.SCRIPT.to_dict('index')
        self.sequence = dict()
        for k, v in self.INDEX.items():
            self.sequence[k] = dict(INDEX=self.INDEX[k], SCRIPT=self.SCRIPT[k],
                                    INPUTS=self.INPUTS[k], OUTPUT=self.OUTPUT[k])

    def ppcheck(self):
        for k, v in self.sequence.items():
            print(k)
            pp(v)

    def execute(self, runtime_object):
        for k, v in self.sequence.items():
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
        self.RT_ID = '_'.join(indexes)
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