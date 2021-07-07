import os, re, pathlib, sys
from pprint import pprint as pp
import util_Path as UP
from util_Logging import LogWarp as LW
import util_Date as UD


class PathName:
    # PathLib
    # status: path find, directory, file etc
    # find pathname, return pathname, walk thro directory

    # class_method_dict = {'_split_ext': os.path.splitext,
    #                      '_isdir': os.path.isdir,
    #                      '_mkdir': os.mkdir,
    #                      '_join': os.path.join,
    #                      '_exist': os.path.exists}
    # self.__dict__.update(self.class_method_dict)

    @staticmethod
    def give_error_msg():
        print(UP.Style().RED + f'Status: No file found' + UP.Style().END)

    @ staticmethod
    def _isfile(pathname):
        return os.path.isfile(pathname)

    @staticmethod
    def _abs(path):
        return os.path.abspath(path)

    @staticmethod
    def _split_ext(name):
        return os.path.splitext(name)

    @staticmethod
    def _isdir(path):
        return os.path.isdir(path)

    @staticmethod
    def _mkdir(path):
        return os.mkdir(path)

    @staticmethod
    def _basename(path):
        return os.path.basename(path)

    @staticmethod
    def _listdir(path):
        return os.listdir(path)

    @staticmethod
    def _join(*args):
        return os.path.join(*args)

    @staticmethod
    def _exist(pathname):
        return os.path.exists(pathname)

    @staticmethod
    def _getctime(path):
        return os.path.getctime(path)

    @staticmethod
    def _relpath(path, levels):
        temp = os.path.join(path, *list([os.pardir] * levels))
        return os.path.relpath(path, temp)

    def __init__(self, io, path, name, ext=None, suffix=None, regex_hint=None, verbose=False, log=None):
        self.verbose = verbose
        self._L = log if log is not None else LW
        if io in ['in', 'In', 'IN', 'i', 'I', 1, 'IO_I']:
            self.io = 1
        if io in ['out', 'Out', 'OUT', 'o', 'O', 0, 'IO_O']:
            self.io = 0
        self._path = path
        self._path_walker = dict()
        self._sufx = suffix
        self._name = name
        self._ext = ext
        self._hint = regex_hint
        self._is_forced_path = False
        self._is_forced_walker = False

    @property
    def _is_implicit_ext(self):
        return True if self._split_ext(self._name)[1] else False

    @property
    def _has_pathname_walker(self):
        # TODO correct?
        result = False
        try:
            result = False if len(self.pathname_walker) == 0 else True
        except TypeError:
            LW.wt(f'Empty in "{self.relpath}"')
            pass
        return result

    @property
    def _has_filtered_walker(self):
        # TODO correct?
        result = False
        try:
            result = False if len(self._filtered_walker) == 0 else True
        except TypeError:
            LW.wt(f'"{self.name}{self.ext}" not found in "{self.relpath}"')
            pass
        return result

    @property
    def found(self):
        return self._has_filtered_walker

    @property
    def name(self):
        return self._split_ext(self._name)[0] if self._is_implicit_ext else self._name

    @property
    def ext(self):
        ext = self._split_ext(self._name)[1] if self._is_implicit_ext else self._ext
        return '.' + ext if (ext and ext[0] != '.') else ext

    @property
    def path(self):
        path = self._path
        if not self._isdir(path):
            if self.io == 0:
                self._mkdir(path)
                self._is_forced_path = True
            else:
                path = '.'
        return path

    @property
    def relpath(self):
        return self._relpath(self.path, 4)

    @ property
    def outpath(self):
        return self._join(self.path, f'{self._sufx}-{self.name}-{UD.Get_Detailed_Time()}{self.ext}')

    @property
    def pathname_walker(self):
        if self.io == 1:
            walker = [self._join(self.path, e) for e in self._listdir(self.path)]
        else:
            # TODO correct?
            walker = [self._join(self.path, e) for e in self._listdir(self.path)]
        if self._is_forced_walker:
            walker = set(walker).union(self._path_walker)
        return walker

    def set_pathname_walker(self, mode, value):
        self._path_walker = None
        if mode == 'SequenceByteIO' and value.container:
            self._is_forced_walker = True
            self._path_walker = [k for k in value.container.keys() if self.path in k]

    @property
    def _filtered_walker(self):
        """
        Find a single PATHNAME as concrete matched by PATH, NAME and EXT.
        Hint in regex can be provided for additional filter.
        PATHNAME or None is returned
        """
        name_filtered = []
        hint_filtered = []
        if self._has_pathname_walker:
            for pn in self.pathname_walker:
                if (self.path in pn) and (self.name in pn) and (self.ext == self._split_ext(pn)[1]) and ('~$' not in pn):
                    name_filtered.append(pn)

            if self._hint is not None and len(name_filtered) > 1:
                for e in name_filtered:
                    if re.search(self._hint, self._basename(e)):
                        hint_filtered.append(e)
                hint_filtered = [e for e in name_filtered if re.search(self._hint, self._basename(e))]

            filtered = hint_filtered if len(hint_filtered) > 0 else name_filtered

            if self._is_forced_walker:
                filtered.sort(reverse=True)
            else:
                filtered.sort(key=os.path.getctime, reverse=True)
            return filtered

    @property
    def pathnames(self):
        return self._filtered_walker if self.found else None

    @property
    def count(self):
        return len(self._filtered_walker) if self.found else 0

    @property
    def pathname(self):
        if self.io == 0:
            return self.outpath
        if self.io == 1:
            return self._filtered_walker[0] if self.found else None

    def __str__(self):
        str_hint = f'with hint "{self._hint}"' if self._hint else ''
        str_io = '"IN" operation' if self.io else '"OUT" operation'
        print(f'Name: "{self.name + self.ext}"{str_hint} for {str_io}')
        print(f'Path: "{self.relpath}"')
        print('1: Extension is provided in NAME') if self._is_implicit_ext else None
        print('2: Directory is created by PATH') if self._is_forced_path else None
        print('3: Pathname Walker is overridden') if self._is_forced_walker else None
        if self.found:
            print(f'Status: Found {self.count} ea.')
        else:
            self.give_error_msg()
        return '\n'

    def force_unique(self, number):
        if self.count > number:
            pn_list = self.pathnames[number:]
            for e in pn_list:
                if self._isfile(e):
                    os.remove(e)
                    self._L.wt(f'Deleted: {e}')
