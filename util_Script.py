import sys, os, builtins, re
import importlib.util as iplu
from ast import literal_eval
from pprint import pprint as pp
import pandas as pd
import util_Path as UP
import util_Date as UD
from util_Sequence import SequenceByteIO as SBI

class ScriptSequence:
    EXT = '.xlsx'
    # Tabs
    SEQ = 'SEQ'
    PROC = 'SCRIPT'
    IO_I = 'INPUTS'
    IO_O = 'OUTPUT'
    SEQ_NUM = 'SEQ_ID'

    # SEQ Labels
    SEQ_MUTE = ['RUN']
    SEQ_REPR = ['PROJ_ID', 'FLOW_ID', 'STEP_ID']
    SEQ_IDX = SEQ_MUTE + SEQ_REPR
    # Other labels
    LITERALs = ['SEQ_ID', 'RUN', 'STEP_ID', 'PARAM']
    SWITCH = 'RUN'

    @staticmethod
    def parse_literal_columns(dataframe):
        try:
            labels = dataframe.columns.to_list()
            labels = [e for e in labels if e in ScriptSequence.LITERALs]
            for e in labels:
                dataframe[e] = dataframe[e].apply(lambda x: literal_eval(x))
            return dataframe
        except ValueError:
            print(dataframe.columns)
            print(e)
            raise ValueError

    def __init__(self, path, name, sequence=dict(), verbose=True):
        self.verbose = verbose
        self._attr_dict = None
        self._seq = dict()
        self._path = ''
        self._name = ''
        self._pathname = ''
        self._keys = []
        self._df_keys = []
        self._dict_keys = []
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
        self._path = value

    @name.setter
    def name(self, value):
        temp, _ = os.path.splitext(value)
        self._name = temp

    @property
    def sequence(self):
        return self._seq

    @sequence.setter
    def sequence(self, value):
        # direct assignment will overwrite loading from path
        if value and value != self._seq:
            self._seq = value

    # ------------------------------------------------------------------------------------------------------------------

    def parse_pathname(self):
        self._pathname = find_pathname(self._name, self.EXT, self._path, verbose=self.verbose)

    def parse_source(self):
        # Load from excel for pathname and set attr_dict and index
        import pandas as pd
        # parse DataFrames from imported by pathname
        df_dict = pd.read_excel(self._pathname, sheet_name=None, dtype=str, index_col=None)
        self._attr_dict = {k: self.parse_literal_columns(v) for k, v in df_dict.items()}
        UP.stylized_print_green('Source file loaded.')

    # ------------------------------------------------------------------------------------------------------------------

    def parse_attr_keys(self):
        # assign each tab to an attribute
        self._keys = [e for e in self._attr_dict.keys()]
        self._df_keys = ['_df_' + e for e in self._attr_dict.keys()]
        self._dict_keys = ['_dict_' + e for e in self._attr_dict.keys()]

    def parse_attribute(self):
        # Get attr based on based on attr_name, check if consistent and remove redundant columns
        for attr_name, attr_value in self._attr_dict.items():
            attr_value = attr_value.set_index(self.SEQ_NUM)
            self.__setattr__('_df_' + attr_name, attr_value)
        UP.stylized_print_green('Attribute loaded.')

    # ------------------------------------------------------------------------------------------------------------------

    def parse_index(self):
        for e in self._df_keys:
            attr_df = self.__dict__[e]
            if self._safe_init_index(attr_df):
                attr_df_idx = attr_df.loc[:, self.SEQ_IDX]
                attr_df_repr = attr_df.loc[:, self.SEQ_REPR].agg(lambda x: x.astype(str).str.cat(sep='\''), axis=1)
                attr_df_idx = attr_df_idx.assign(REPR_ID=attr_df_repr)
                self.__setattr__('_df_' + self.SEQ, attr_df_idx)
                self.__setattr__(e, attr_df.drop(columns=self.SEQ_IDX))

    def _safe_init_index(self, value):
        try:
            temp = self.__getattribute__('_df_' + self.SEQ)
        except AttributeError:
            temp = None
        if temp is not None:
            temp = (temp.loc[:, self.SEQ_REPR] == value.loc[:, self.SEQ_REPR])
            return temp.all().all()
        else:
            return True

    # ------------------------------------------------------------------------------------------------------------------

    def parse_dicts(self):
        # get dicts from DataFrames
        self._attr_dict_simple(self.SEQ)
        self._attr_dict_simple(self.PROC)
        self._get_path_dicts(self.IO_I)
        self._get_path_dicts(self.IO_O)
        self._combine_sequence()
        UP.stylized_print_green('Sequence Mapper loaded.')

    def _attr_dict_simple(self, attr_df_name):
        attr_dataframe = self.__getattribute__('_df_' + attr_df_name)
        self.__setattr__('_dict_' + attr_df_name, attr_dataframe.to_dict('index'))

    def _get_path_dicts(self, attr_name):
        # Get attr based on based on attr_name, obtain {'main': {'NAME': XXX, 'PATH': XXX}}
        # instead of {'main_NAME': XXX, 'main_PATH': XXX}
        attr_value = self.__getattribute__('_df_' + attr_name)
        attr_value.columns = pd.MultiIndex.from_tuples(tuple(attr_value.columns.str.split('_')))
        attr_value = attr_value.stack(0)
        attr_value = attr_value.to_dict('index')
        temp = dict()
        for (k1, k2), v in attr_value.items():
            temp.setdefault(k1, dict())
            temp[k1].update({k2: v})
        self.__setattr__('_dict_' + attr_name, temp)

    def _combine_sequence(self):
        self._seq = dict()
        for k, v in self.__getattribute__('_dict_' + self.SEQ).items():
            self._seq[k] = {self.SEQ: self.__getattribute__('_dict_' + self.SEQ)[k],
                            self.PROC: self.__getattribute__('_dict_' + self.PROC)[k],
                            self.IO_I: self.__getattribute__('_dict_' + self.IO_I)[k],
                            self.IO_O: self.__getattribute__('_dict_' + self.IO_O)[k]}

    # ------------------------------------------------------------------------------------------------------------------

    def parse_mapper(self):
        self.parse_pathname()
        self.parse_source()
        self.parse_attr_keys()
        self.parse_attribute()
        self.parse_index()
        self.parse_dicts()

    def execute(self, runtime_object):
        for k, v in self._seq.items():
            UP.stylized_print_cyan(f'SEQUENCE: {k}')
            runtime_object.k = k
            runtime_object.v = v
            runtime_object.load_runtime()
            runtime_object.execute()

    def check_outputs(self):
        for k, v in self._seq.items():
            UP.stylized_print_cyan(f'SEQUENCE: {k}')
            pp(v)

    def runtime_check_outputs(self, runtime_object):
        for k, v in self._seq.items():
            runtime_object.k = k
            runtime_object.v = v
            runtime_object.load_runtime()
            runtime_object.check_outputs()


class ScriptRunTime:

    SEQNUM = 'SEQ_ID'
    SEQ = 'SEQ'
    PROC = 'SCRIPT'
    IO_I = 'INPUTS'
    IO_O = 'OUTPUT'

    @staticmethod
    def exec_module(name, path, verbose):
        path_name = find_pathname(name, '.py', path, verbose=verbose)
        _, name_ext = os.path.split(path_name)
        spec = iplu.spec_from_file_location(name_ext, path_name)
        module = iplu.module_from_spec(spec)
        spec.loader.exec_module(module)

    def __init__(self, module, base_path='', script_path='', specific_flow=None, verbose=True, buffer=True):
        self.verbose = verbose
        self.buffer = buffer
        if self.buffer:
            self.container = SBI(self.verbose)
        self._attr_dict = None
        self.k = None
        self.v = None
        self.REPR_ID = None
        self.MODULE = module
        self.FLOW_ID = specific_flow
        self.BASE_PATH = base_path
        self.SCRIPT_PATH = script_path

    @property
    def k(self):
        return self.SEQ_ID

    @property
    def v(self):
        return self._attr_dict

    @k.setter
    def k(self, value):
        self.SEQ_ID = value

    @v.setter
    def v(self, value):
        if isinstance(value, dict):
            self._attr_dict = value
    # ------------------------------------------------------------------------------------------------------------------

    def load_runtime(self):
        self._parse_attributes_dict()
        self._parse_runtime_seq(self.SEQ)
        self._parse_runtime_proc(self.PROC)
        self._parse_runtime_io_i(self.IO_I)
        self._parse_runtime_io_o(self.IO_O)

    def _parse_attributes_dict(self):
        for attr_name, attr_value in self._attr_dict.items():
            self.__setattr__('_' + attr_name, attr_value)
        self._keys = ['_' + e for e in self._attr_dict.keys()]

    def _parse_runtime_seq(self, attr_name):
        attr_value = self.__getattribute__('_' + attr_name)
        self.REPR_ID = attr_value['REPR_ID']
        # attr_value.update({f'REPR_ID': self.REPR_ID})
        self.__setattr__('_' + attr_name, attr_value)

    def _parse_runtime_proc(self, attr_name):
        attr_value = self.__getattribute__('_' + attr_name)
        path = attr_value.get('PATH', '')
        if self.SCRIPT_PATH:
            attr_value['PATH'] = os.path.join(self.SCRIPT_PATH, path)
        self.__setattr__('_' + attr_name, attr_value)

    def _parse_runtime_io_i(self, attr_name):
        attr_value = self.__getattribute__('_' + attr_name)
        for k, v in attr_value.items():
            name, ext = os.path.splitext(v['NAME'])
            path = v['PATH']
            if self.BASE_PATH:
                path = os.path.join(self.BASE_PATH, path)
            # find the specific file
            pathname = find_pathname(name, ext, path, verbose=self.verbose)
            if pathname:
                UP.stylized_print_section(f'INPUT file ["{k}"]', f': "{UP.get_rel_path(pathname, 3)}".')
            else:
                UP.stylized_print_caution(f'INPUT file ["{k}"] not found')
            attr_value[k].update({'PATH_NAME': pathname})
            if self.buffer:
                self._buffer_pathname(attr_value[k])
        self.__setattr__('_' + attr_name, attr_value)

    def _parse_runtime_io_o(self, attr_name):
        attr_value = self.__getattribute__('_' + attr_name)
        for k, v in attr_value.items():
            name, ext = os.path.splitext(v['NAME'])
            path = v['PATH']
            if self.BASE_PATH:
                path = os.path.join(self.BASE_PATH, path)
            # parse pathname from NAME and PATH, create directory if need
            pathname = force_pathname(path, name, ext, self.REPR_ID)
            if pathname:
                UP.stylized_print_section(f'OUTPUT file ["{k}"]', f': "{UP.get_rel_path(pathname, 3)}".')
            else:
                UP.stylized_print_caution(f'OUTPUT file ["{k}"] not found')
            # delete if output file exist
            pathname_list = find_pathname(name, ext, path, return_list=True, verbose=self.verbose)
            if len(pathname_list) > 1:
                pathname_list = pathname_list[1:]
                remove_pathnames(pathname_list, self.verbose)
            attr_value[k].update({'PATH_NAME': pathname})
            if self.buffer:
                self._buffer_pathname(attr_value[k])
        self.__setattr__('_' + attr_name, attr_value)

    def _buffer_pathname(self, pathname_container):
        self.container.byteio_override(pathname_container, 'PATH_NAME')

    def execute(self):
        temp = self.__dict__
        self.MODULE.PROJ_PARAMS = temp['_' + self.PROC]['PARAM']
        self.MODULE.PROJ_INPUTS = temp['_' + self.IO_I]
        self.MODULE.PROJ_OUTPUT = temp['_' + self.IO_O]
        name = temp['_' + self.PROC]['NAME']
        path = temp['_' + self.PROC]['PATH']
        if self.verbose:
            print(UP.Style.UDLBLD + 'Listing script param(s)' + UP.Style.END + ':')
            pp(temp['_' + self.PROC]['PARAM'])
            print()
        check_flow = temp['_' + self.SEQ]['FLOW_ID']
        check_run = temp['_' + self.SEQ]['RUN']
        if check_run and (not self.FLOW_ID or self.FLOW_ID == check_flow):
            UP.stylized_print_blue(f'"{self.REPR_ID}: {name}" Start')
            ScriptRunTime.exec_module(name, path, self.verbose)
            UP.stylized_print_blue(f'"{self.REPR_ID}: {name}" Done')

    def check_outputs(self):
        temp = self.__dict__
        check_flow = temp['_' + self.SEQ]['FLOW_ID']
        check_run = temp['_' + self.SEQ]['RUN']
        if check_run and (not self.FLOW_ID or self.FLOW_ID == check_flow):
            UP.stylized_print_blue(f'SEQUENCE ID: {self.SEQ_ID}')
            print('Sequence')
            pp(temp['_' + self.SEQ])
            print('\nProcess')
            pp(temp['_' + self.PROC])
            print('\nInput')
            pp(temp['_' + self.IO_I])
            print('\nOutput')
            pp(temp['_' + self.IO_O])
            print()


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
    paths.sort(key=os.path.getctime, reverse=True)

    print(paths)

    if return_list:
        return_value = paths
    elif len(paths) == 0:
        return_value = None
    else:
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

