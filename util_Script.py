import sys, os, builtins, re
import importlib.util as iplu
from ast import literal_eval
from pprint import pprint as pp
import pandas as pd
import util_Path as UP
import util_Date as UD


class ScriptSequence:
    EXT = '.xlsx'
    INDEX = 'index'
    LITERALs = ['STEP_ID', 'PARAM']
    UNIQUEs = ['PROJ_ID', 'FLOW_ID', 'STEP_ID']

    def __init__(self, path, name, sequence=dict(), verbose=True):
        self.verbose = verbose
        self._df_index = None
        self._attr_dict = None
        self._seq = dict()
        self._path = ''
        self._name = ''
        self._pathname = ''
        self.path = path
        self.name = name
        self.sequence = sequence

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
            self.pathname = find_pathname(self._name, ScriptSequence.EXT, value, verbose=self.verbose)

    @name.setter
    def name(self, value):
        temp, _ = os.path.splitext(value)
        if value and temp != self._name:
            self._name = value
            self.pathname = find_pathname(value, ScriptSequence.EXT, self._path, verbose=self.verbose)

    @pathname.setter
    def pathname(self, value):
        if value and value != self._pathname and self._name and self._path:
            self._pathname = value
            UP.stylized_print_green('Pathname loaded.')
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
            UP.stylized_print_green('Source file loaded.')
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
            temp = value.loc[:, [ScriptSequence.INDEX] + ScriptSequence.UNIQUEs]
            self._df_index = temp.set_index(ScriptSequence.INDEX)

    @_parse_source.setter
    def _parse_source(self, value):
        if value is not None:
            # parse DataFrames from imported by pathname
            self._parse_attribute_dict(self._attr_dict)
            # get dicts from DataFrames
            self._df_index = self._df_index.to_dict('index')
            self._df_SCRIPT = self._df_SCRIPT.to_dict('index')
            self._get_path_dicts('_df_INPUTS')
            self._get_path_dicts('_df_OUTPUT')
            # get dicts from DataFrames
            self._combine_sequence()
            UP.stylized_print_green('Sequence parsed.')
            self._attr_dict = None
    # ------------------------------------------------------------------------------------------------------------------

    def _parse_attribute_dict(self, attr_dict):
        # Get attr based on based on attr_name, check if consistent and remove redundant columns
        for attr_name, attr_value in attr_dict.items():
            attr_value = attr_value.set_index(ScriptSequence.INDEX)
            if self._check_index(attr_value):
                attr_value = attr_value.drop(columns=ScriptSequence.UNIQUEs)
                self.__setattr__('_df_' + attr_name, attr_value)
        self._keys = ['_df_' + e for e in self._attr_dict.keys()]

    def _check_index(self, value):
        if self._df_index is not None:
            temp = (self._df_index.loc[:, ScriptSequence.UNIQUEs] == value.loc[:, ScriptSequence.UNIQUEs])
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

    def execute(self, runtime_object):
        for k, v in self._seq.items():
            UP.stylized_print_cyan(f'SEQUENCE: {k}')
            runtime_object.k = k
            runtime_object.v = v
            runtime_object.execute()

    def check_outputs(self):
        for k, v in self._seq.items():
            UP.stylized_print_cyan(f'SEQUENCE: {k}')
            pp(v)

    def runtime_check_outputs(self, runtime_object):
        for k, v in self._seq.items():
            runtime_object.k = k
            runtime_object.v = v
            runtime_object.check_outputs()


class ScriptRunTime:

    @staticmethod
    def exec_module(name, path, verbose):
        path_name = find_pathname(name, '.py', path, verbose=verbose)
        _, name_ext = os.path.split(path_name)
        spec = iplu.spec_from_file_location(name_ext, path_name)
        module = iplu.module_from_spec(spec)
        spec.loader.exec_module(module)

    def __init__(self, module, base_path=None, script_path=None, specific_flow=None, verbose=True):
        self.verbose = verbose
        self._k_loaded = False
        self._v_loaded = False
        self.k = None
        self.v = None
        self.runtime_index = None
        self.MODULE = module
        self.FLOW_ID = specific_flow
        self.BASE_PATH = base_path
        self.SCRIPT_PATH = script_path

    @property
    def k(self):
        return self._k

    @property
    def v(self):
        return self._v

    @k.setter
    def k(self, value):
        self._k = value
        self.k_loaded = True

    @v.setter
    def v(self, value):
        if isinstance(value, dict):
            self._v = value
            self.v_loaded = True
    # ------------------------------------------------------------------------------------------------------------------

    @property
    def k_loaded(self):
        return None

    @property
    def v_loaded(self):
        return None

    @k_loaded.setter
    def k_loaded(self, value):
        self._k_loaded = value

    @v_loaded.setter
    def v_loaded(self, value):
        self._v_loaded = value

    # ------------------------------------------------------------------------------------------------------------------

    def _load_runtime(self):
        loaded = self._k_loaded and self._v_loaded
        if not loaded:
            raise AttributeError('Runtime attribute not loaded.')
        else:
            self._parse_attributes_dict(self._v)
            self._parse_runtime_index('INDEX')
            self._parse_runtime_input('INPUTS')
            self._parse_runtime_output('OUTPUT')
            self._k_loaded = False
            self._v_loaded = False

    def _parse_attributes_dict(self, attr_dict):
        for attr_name, attr_value in attr_dict.items():
            self.__setattr__(attr_name, attr_value)
        self._keys = [e for e in attr_dict.keys()]

    def _parse_runtime_index(self, attr_name):
        attr_value = self.__getattribute__(attr_name)
        self.runtime_index = '\''.join(map(str, list(attr_value.values())))
        attr_value.update({f'runtime_index': self.runtime_index})
        self.__setattr__(attr_name, attr_value)

    def _parse_runtime_input(self, attr_name):
        attr_value = self.__getattribute__(attr_name)
        for k, v in attr_value.items():
            name, ext = os.path.splitext(v['NAME'])
            path = v['PATH']
            # find the specific file
            pathname = find_pathname(name, ext, path, verbose=self.verbose)
            if pathname:
                UP.stylized_print_section(f'INPUT file ["{k}"]', f': "{UP.get_rel_path(pathname, 3)}".')
            else:
                raise FileNotFoundError(f'File not found: "{name}{ext}" at "{path}"')
            attr_value[k].update({'PATH_NAME': pathname})
        self.__setattr__(attr_name, attr_value)

    def _parse_runtime_output(self, attr_name):
        attr_value = self.__getattribute__(attr_name)
        for k, v in attr_value.items():
            name, ext = os.path.splitext(v['NAME'])
            path = v['PATH']
            # parse pathname from NAME and PATH, create directory if need
            pathname = force_pathname(path, name, ext, self.runtime_index)
            UP.stylized_print_section(f'OUTPUT file ["{k}"]', f': "{UP.get_rel_path(pathname, 3)}".')
            # delete if output file exist
            pathname_list = find_pathname(name, ext, path, return_list=True, verbose=self.verbose)
            if len(pathname_list) > 0:
                remove_pathnames(pathname_list, self.verbose)
            attr_value[k].update({'PATH_NAME': pathname})
        self.__setattr__(attr_name, attr_value)

    def execute(self):
        self._load_runtime()
        name = self.SCRIPT['NAME']
        path = self.SCRIPT['PATH']
        self.MODULE.PROJ_PARAMS = self.SCRIPT['PARAM']
        self.MODULE.PROJ_INPUTS = self.INPUTS
        self.MODULE.PROJ_OUTPUT = self.OUTPUT
        if self.verbose:
            print(UP.Style.UDLBLD + 'Listing script param(s)' + UP.Style.END + ':')
            pp(self.SCRIPT['PARAM'])
            print()
        if not self.FLOW_ID or self.FLOW_ID == self.INDEX['FLOW_ID']:
            UP.stylized_print_blue(f'"{self.runtime_index}: {name}" Start')
            ScriptRunTime.exec_module(name, path, self.verbose)
            UP.stylized_print_blue(f'"{self.runtime_index}: {name}" Done')

    def check_outputs(self):
        UP.stylized_print_blue(f'SEQUENCE ID: {self._k}')
        pp(self.SCRIPT)
        pp(self.INPUTS)
        pp(self.OUTPUT)


def find_pathname(name_target, ext_target, path_target='.', regex_hint=None, return_list=False, verbose=True):
    """
    Find, in os.path.cwd or in specified PATH, a single PATHNAME as concrete matched by NAME and EXT.
    List of PATHNAME can be returned, if return_list is flagged.
    Hint in regex can be provided for additional filter.
    Tuple of (matched_flag, PATHNAME/ List of PATHNAME/ None) is returned
    """
    paths = []
    ctime = []
    hint_str = f' with "{regex_hint}"' if regex_hint else ''
    for path in os.listdir(os.path.join(path_target)):
        name, ext = os.path.splitext(path)
        if name_target in name and ext == ext_target and '~' not in name:
            if not regex_hint or (regex_hint and re.search(regex_hint, name)):
                paths.append(os.path.join(path_target, path))
                ctime.append(os.path.getctime(os.path.join(path_target, path)))

    if return_list:
        return_value = paths
    elif len(paths) == 0:
        return_value = None
    else:
        paths.sort(key=os.path.getctime, reverse=True)
        return_value = paths[0]

    if verbose:
        print(f'Found {len(paths)} ea. of {name_target}{ext_target}{hint_str} in "{UP.get_rel_path(path_target, 3)}".\n')
        if return_list:
            pp(paths, width=200)
    return return_value


def force_pathname(path, name, ext, suffix=None):
    if not os.path.exists(path):
        os.mkdir(path)
        print(f'Directory not found, now created: {path}')
    suffix = suffix + '-' if suffix else None
    return os.path.join(path, f'{suffix}{name}-{UD.Get_Detailed_Time()}{ext}')


def remove_pathnames(pathname_list, verbose=False):
    if verbose:
        print(UP.Style.UDLBLD + 'Listing similar file(s)' + UP.Style.END + ':')
        pp(pathname_list, width=200)

    for e in pathname_list:
        if os.path.isfile(e):
            os.remove(e)
    UP.stylized_print_caution('Similar file(s) are DELETED')



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