import importlib.util as iplu
import inspect
import os
from ast import literal_eval
from pprint import pprint as pp

import pandas as pd

import util_Path as UP
from util_Logging import LogWarp as LW
from util_PathName import PathName as PN


class ScriptSequence:
    EXT = '.xlsx'

    # Hardcode mapping
    # Tabs
    INDX = 'INDEX'
    PROC = 'SCRIPT'
    IO_I = 'INPUTS'
    IO_O = 'OUTPUT'
    ID_SEQ = 'SEQ_ID'
    ID_UNIQ = 'UNIQ_ID'

    # Hardcode grouping
    # SEQ Labels
    SWITCH = 'RUN'
    SEQ_MUTE = ['RUN']
    SEQ_UNIQ = ['PROJ_ID', 'FLOW_ID', 'STEP_ID']
    SEQ_IDX = SEQ_MUTE + SEQ_UNIQ + ['NAME_ID']
    # Other labels
    LITERALs = ['SEQ_ID', 'RUN', 'STEP_ID', 'PARAM']
    IO_PATH = 'PATH'
    IO_NAME = 'NAME'

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

    def __init__(self, path, name, verbose=True, log=None):
        self.verbose = verbose
        self._L = log if log is not None else LW
        self._attr_dict = None
        self._seq = dict()
        self._PN = PN(io='IN', path=path, name=name, ext=self.EXT, verbose=self.verbose)
        self._keys = []
        self._df_keys = []
        self._dict_keys = []
        self.path = path
        self.name = name

    @property
    def sequence(self):
        return self._seq

    @sequence.setter
    def sequence(self, value):
        # direct assignment will overwrite loading from path
        self._seq = value if value else self._seq

    def parse_mapper(self):
        self.parse_source()
        self.parse_attr_keys()
        self.parse_attribute()
        self.parse_index()
        self.parse_dicts()

    # ------------------------------------------------------------------------------------------------------------------

    def parse_source(self):
        # Load from excel for pathname and set attr_dict and index
        # parse DataFrames from imported by pathname
        import pandas as pd
        if self._PN.found:
            df_dict = pd.read_excel(self._PN.pathname, sheet_name=None, dtype=str, index_col=None)
            self._attr_dict = {k: self.parse_literal_columns(v) for k, v in df_dict.items()}
            None if not self._L else self._L.bk(f'Definition file loaded')
            None if not self._L else self._L.wt(f'Definition: {self._PN.relpathname}')
            UP.stylized_print_green('Definition file loaded.')
            # _ = [print(v.columns) for k, v in df_dict.items()]
        else:
            self._PN.give_error_msg()

    # ------------------------------------------------------------------------------------------------------------------

    def parse_attr_keys(self):
        # assign each tab to an attribute
        self._keys = [e for e in self._attr_dict.keys()]
        self._df_keys = ['_df_' + e for e in self._attr_dict.keys()]
        self._dict_keys = ['_dict_' + e for e in self._attr_dict.keys()]

    def parse_attribute(self):
        # Get attr based on based on attr_name, check if consistent and remove redundant columns
        for attr_name, attr_value in self._attr_dict.items():
            temp = attr_value.set_index(self.ID_SEQ)
            self.__setattr__('_df_' + attr_name, temp)
        None if not self._L else self._L.bk('Attribute loaded.')
        UP.stylized_print_green('Attribute loaded.')

    # ------------------------------------------------------------------------------------------------------------------

    def parse_index(self):
        for e in self._df_keys:
            attr_df = self.__getattribute__(e)
            if self._safe_init_index(attr_df):
                attr_df_idx = attr_df.loc[:, self.SEQ_IDX]
                attr_df_idx[self.ID_UNIQ] = attr_df.loc[:, self.SEQ_UNIQ].agg(lambda x: x.astype(str).str.cat(sep='\''), axis=1)
                self.__setattr__('_df_' + self.INDX, attr_df_idx)
                self.__setattr__(e, attr_df.drop(columns=self.SEQ_IDX))

    def _safe_init_index(self, value):
        try:
            temp = self.__getattribute__('_df_' + self.INDX)
        except AttributeError:
            temp = None
        if temp is not None:
            temp = (temp.loc[:, self.SEQ_UNIQ] == value.loc[:, self.SEQ_UNIQ])
            return temp.all().all()
        else:
            return True

    # ------------------------------------------------------------------------------------------------------------------

    def parse_dicts(self):
        # get dicts from DataFrames
        self._attr_dict_simple(self.INDX)
        self._attr_dict_simple(self.PROC)
        self._get_path_dicts(self.IO_I)
        self._get_path_dicts(self.IO_O)
        self._combine_sequence()
        None if not self._L else self._L.bk('Sequence Mapper loaded.')
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
        for k, v in self.__getattribute__('_dict_' + self.INDX).items():
            self._seq[k] = {self.INDX: self.__getattribute__('_dict_' + self.INDX)[k],
                            self.PROC: self.__getattribute__('_dict_' + self.PROC)[k],
                            self.IO_I: self.__getattribute__('_dict_' + self.IO_I)[k],
                            self.IO_O: self.__getattribute__('_dict_' + self.IO_O)[k]}

    def override_sequence(self, keys, value):

        def recursive_drill_and_replace(dict_obj, keys, end_value):
            if len(keys) > 1:
                elem_key = keys.pop(0)
                elem_dict = dict_obj.pop(elem_key)
                resemble = recursive_drill_and_replace(elem_dict, keys, end_value)
                dict_obj[elem_key] = resemble
                return dict_obj
            elif len(keys) == 1:
                dict_obj[keys[0]] = end_value
                return dict_obj

        temp = recursive_drill_and_replace(self._seq, keys, value)
        self._seq = {i: temp[i] for i in sorted(temp)}

    # ------------------------------------------------------------------------------------------------------------------

    def invoke_runtime(self, runtime_object, function_name):
        for k, v in self._seq.items():
            UP.stylized_print_cyan(f'SEQUENCE: {k}')
            runtime_object.k = k
            runtime_object.v = v
            getattr(runtime_object, function_name)()

    def execute(self, runtime_object):
        # invoke function of the same name
        self.invoke_runtime(runtime_object, inspect.currentframe().f_code.co_name)

    def check_outputs(self, runtime_object):
        # invoke function of the same name
        self.invoke_runtime(runtime_object, inspect.currentframe().f_code.co_name)

    def seq_check_outputs(self):
        for k, v in self._seq.items():
            UP.stylized_print_cyan(f'SEQUENCE: {k}')
            pp(v)


class ScriptRunTime:

    ID_SEQ = 'SEQ_ID'
    ID_UNIQ = 'UNIQ_ID'

    INDX = 'INDEX'
    PROC = 'SCRIPT'
    IO_I = 'INPUTS'
    IO_O = 'OUTPUT'
    INDX = '_' + INDX
    PROC = '_' + PROC
    IO_I = '_' + IO_I
    IO_O = '_' + IO_O

    IO_PATH = 'PATH'
    IO_NAME = 'NAME'
    IO_PATHNAME = 'PATHNAME'

    @staticmethod
    def exec_module(name, path, verbose):
        obj_pn = PN(io='IN', path=path, name=name, ext='.py', verbose=verbose)
        pn = obj_pn.pathname
        name_ext = obj_pn.name + obj_pn.ext
        spec = iplu.spec_from_file_location(name_ext, pn)
        module = iplu.module_from_spec(spec)
        _ = module.__dict__
        spec.loader.exec_module(module)

    def __init__(self, module, base_path='', script_path='', specific_flow=None, buffer=None, verbose=True, log=None):
        self.verbose = verbose
        self._L = log if log is not None else LW
        if buffer is not None:
            self.buffered = True
            self.container = buffer
        else:
            self.buffered = False
        self._attr_dict = None
        self.MODULE = module
        self.FLOW_ID = specific_flow
        self.BASE_PATH = base_path
        self.SCRIPT_PATH = script_path

    @property
    def k(self):
        return self.__getattribute__(self.ID_SEQ)

    @property
    def v(self):
        return self._attr_dict

    @k.setter
    def k(self, value):
        self.__setattr__(self.ID_SEQ, value)

    @v.setter
    def v(self, value):
        if isinstance(value, dict):
            self._attr_dict = value

    def _load_runtime(self):
        self._parse_attributes_dict()
        self._parse_runtime_seq(self.INDX)
        self._parse_runtime_proc(self.PROC)
        self._parse_runtime_io(self.IO_I, 'IO_I')
        self._parse_runtime_io(self.IO_O, 'IO_O')

    # ------------------------------------------------------------------------------------------------------------------

    def _parse_attributes_dict(self):
        for attr_name, attr_value in self._attr_dict.items():
            self.__setattr__('_' + attr_name, attr_value)
        self._keys = ['_' + e for e in self._attr_dict.keys()]

    # ------------------------------------------------------------------------------------------------------------------

    def _parse_runtime_seq(self, attr_name):
        attr_value = self.__getattribute__(attr_name)
        self.__setattr__(self.ID_UNIQ, attr_value[self.ID_UNIQ])
        self.__setattr__(attr_name, attr_value)

    def _parse_runtime_proc(self, attr_name):
        attr_value = self.__getattribute__(attr_name)
        path = attr_value.get(self.IO_PATH, '.')
        if self.SCRIPT_PATH:
            path_temp = os.path.abspath(os.path.join(self.SCRIPT_PATH, path))
            attr_value[self.IO_PATH] = path_temp

        self.__setattr__(attr_name, attr_value)

    # ------------------------------------------------------------------------------------------------------------------

    def _parse_runtime_io(self, attr_name, io):
        attr_value = self.__getattribute__(attr_name)
        for k, v in attr_value.items():
            path, name = self._parse_base_path(v, self.BASE_PATH)
            # find the specific file
            obj_pn = PN(io=io, path=path, name=name, prefix=self.UNIQ_ID, regex_hint=self.UNIQ_ID, verbose=self.verbose)
            if self.buffered:
                obj_pn.extend_candidates('SequenceByteIO', self.container)
            pathname = obj_pn.pathname
            obj_pn.remove_obsoletes(1)
            self._L.wt(f'{io}: {obj_pn.count} ea. of "{obj_pn.name}{obj_pn.ext}" at "{obj_pn.relpath}"')
            self._L.wt(f'{io}: Concrete pathname: {obj_pn._relpath(obj_pn.pathname, 4)}')
            if self.verbose:
                if obj_pn.found:
                    UP.stylized_print_section(f'{io} file ["{k}"]', f': "{obj_pn.relpath}".')
                else:
                    UP.stylized_print_caution(f'{io} file ["{k}"] not found')

            attr_value[k].update({self.IO_PATHNAME: pathname})
            if self.buffered:
                self._buffer_override_pathname(attr_value[k])
        self.__setattr__(attr_name, attr_value)

    def _parse_base_path(self, input_dict, base_path):
        path = input_dict[self.IO_PATH]
        name = input_dict[self.IO_NAME]
        if not os.path.isdir(path):
            self._L.wt(f'Add "{path}" to "{base_path}"')
            path = os.path.abspath(os.path.join(base_path, path))
            self._L.wt(f'Result: "{path}"')
        return path, name

    def _buffer_override_pathname(self, pathname_container):
        self.container.byteio_override(pathname_container, self.IO_PATHNAME)

    # ------------------------------------------------------------------------------------------------------------------

    def execute(self):
        self._load_runtime()
        temp = self.__dict__
        check_flow = temp[self.INDX]['FLOW_ID']
        check_run = temp[self.INDX]['RUN']
        if check_run and (not self.FLOW_ID or self.FLOW_ID == check_flow):
            self.MODULE.PROJ_PARAMS = temp[self.PROC]['PARAM']
            self.MODULE.PROJ_INPUTS = temp[self.IO_I]
            self.MODULE.PROJ_OUTPUT = temp[self.IO_O]
            if self.verbose:
                print(UP.Style.UDLBLD + 'Listing script param(s)' + UP.Style.END + ':')
                pp(temp[self.PROC]['PARAM'])
                print()
            name = temp[self.PROC][self.IO_NAME]
            repr_sequence = f'{self.k}: "{self.__getattribute__(self.ID_UNIQ)}: {name}"'
            self._L.bk(repr_sequence)
            UP.stylized_print_blue(f'{repr_sequence}" Start')
            ScriptRunTime.exec_module(name, temp[self.PROC][self.IO_PATH], self.verbose)
            self._L.nl()
            UP.stylized_print_blue(f'{repr_sequence}" End')

    def check_outputs(self):
        self._load_runtime()
        temp = self.__dict__
        check_flow = temp[self.INDX]['FLOW_ID']
        check_run = temp[self.INDX]['RUN']
        if check_run and (not self.FLOW_ID or self.FLOW_ID == check_flow):
            name = temp[self.PROC][self.IO_NAME]
            repr_sequence = f'{self.k}: "{self.__getattribute__(self.ID_UNIQ)}: {name}"'
            self._L.bk(repr_sequence)
            UP.stylized_print_blue(f'{repr_sequence}" Start')
            print('___Sequence___')
            pp(temp[self.INDX])
            print('\n___Process___')
            pp(temp[self.PROC])
            print('\n___Input___')
            pp(temp[self.IO_I])
            print('\n___Output___')
            pp(temp[self.IO_O])
            print()
            # if self.buffered:
            #     print(self.container)
            self._L.nl()
            UP.stylized_print_blue(f'{repr_sequence}" Done')


# Deprecated
def find_pathname(name_target, ext_target, path_target='.', regex_hint=None, return_list=False, verbose=True):
    pass
    # """
    # Find, in os.path.cwd or in specified PATH, a single PATHNAME as concrete matched by NAME and EXT.
    # List of PATHNAME can be returned, if return_list is flagged.
    # Hint in regex can be provided for additional filter.
    # PATHNAME or List of PATHNAME or None is returned
    # """
    # paths = []
    # ctime = []
    # hint_str = f' with "{regex_hint}"' if regex_hint else ''
    # for path in os.listdir(os.path.join(path_target)):
    #     name, ext = os.path.splitext(path)
    #     if name_target in name and ext == ext_target and '~' not in name:
    #         if not regex_hint or (regex_hint and re.search(regex_hint, name)):
    #             paths.append(os.path.join(path_target, path))
    #             ctime.append(os.path.getctime(os.path.join(path_target, path)))
    # paths.sort(key=os.path.getctime, reverse=True)
    #
    # if return_list:
    #     return_value = paths
    # elif len(paths) == 0:
    #     return_value = None
    # else:
    #     return_value = paths[0]
    #
    # if verbose:
    #     print(f'Found {len(paths)} ea. of {name_target}{ext_target}{hint_str} in "{UP.get_rel_path(path_target, 3)}".\n')
    #     if return_list:
    #         pp(paths, width=200)
    # return return_value


def find_pathname_dict(name_target, ext_target, pathname_dict, regex_hint=None, verbose=True):
    pass
    # """
    # Find, in specified dictionary, a single PATHNAME as concrete matched by NAME and EXT.
    # List of PATHNAME can be returned, if return_list is flagged.
    # Hint in regex can be provided for additional filter.
    # PATHNAME or None or False is returned
    # """
    # paths = []
    # hint_str = f' with "{regex_hint}"' if regex_hint else ''
    # for path in pathname_dict.keys():
    #     name, ext = os.path.splitext(path)
    #     if name_target in name and ext == ext_target and '~' not in name:
    #         if not regex_hint or (regex_hint and re.search(regex_hint, name)):
    #             paths.append(path)
    #
    # if len(paths) > 1:
    #     return_value = False
    # elif len(paths) == 0:
    #     return_value = None
    # else:
    #     return_value = paths[0]
    # if verbose:
    #     print(f'Found {len(paths)} ea. of {name_target}{ext_target}{hint_str} in dictionary.\n')
    # return return_value


def force_pathname(path, name, ext, suffix=None):
    pass
    # if not os.path.exists(path):
    #     os.mkdir(path)
    #     print(f'Directory not found, now created: {path}')
    # suffix = suffix + '-' if suffix else None
    # return os.path.join(path, f'{suffix}{name}-{UD.Get_Detailed_Time()}{ext}')


def remove_pathnames(pathname_list, verbose=False):
    pass
    # if verbose:
    #     print(UP.Style.UDLBLD + 'Listing similar file(s)' + UP.Style.END + ':')
    #     pp(pathname_list, width=200)
    #
    # for e in pathname_list:
    #     if os.path.isfile(e):
    #         os.remove(e)
    # UP.stylized_print_caution('Similar file(s) are DELETED')

