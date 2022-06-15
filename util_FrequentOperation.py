import copy
import inspect
import itertools as itt
import pandas as pd
import util_Path as UP


class MapperCreator:
    """
        Default ID is provided for 'tab_col', 'key_cols', 'value_col'
        Additional ID includes 'stack_col', 'default_col'
    """
    repr_count = 5

    # TODO different mapper class, abstract class and methods

    def __init__(self, dataframe: pd.DataFrame, **kwargs):
        """
            Default ID is provided for 'tab_col', 'key_cols', 'value_col'
            Additional ID includes 'stack_col', 'default_col'
        """
        self.df = dataframe
        self._df_hidden = dataframe
        self.verbose = int(kwargs.setdefault('verbose', 0))
        self.tab = kwargs.setdefault('tab_col', 'strTab')
        self.key = kwargs.setdefault('key_cols', 'strKey')
        self.val = kwargs.setdefault('value_col', 'strVal')
        self.stck = kwargs.setdefault('stack_col', 'strStk')
        self.dflt = kwargs.setdefault('default_col', 'strDef')

        # TODO downgrade key cols to one col only
        # display(self.df.loc[:, self.key].squeeze())

        if isinstance(kwargs['key_cols'], list):
            self.is_key_list = True
        else:
            self.is_key_list = False
        self._has_default_col()
        self._has_stack_col()
        self._validate_input()
        self._check_keys()

    def __repr__(self):
        if self.verbose:
            temp = self.df.head(self.verbose)
        else:
            temp = self.df.sample(MapperCreator.repr_count).sort_index()

        try:
            display(temp)
        except:
            print(temp)
        return self.__str__()

    def __str__(self):
        repr_string = f'Tab: {self.tab}, Key: {self.key}, Val: {self.val}, 2D Key: {self.is_key_list}'
        if self._has_default:
            repr_string += f', Def: {self.dflt}'
        return repr_string

    def show(self):
        print(repr(self))

    def restore(self):
        self.__init__(self._df_hidden)
        print('Mapper restored to initial state.')

    # TODO downgrade key cols to one col only
    def _reduce_key_dimension(self):
        pass

    def _has_default_col(self):
        self._has_default = False
        try:
            _ = self.df[self.dflt]
            self._has_default = True
        except KeyError:
            pass

    def _has_stack_col(self):
        self._has_stack = False
        try:
            _ = self.df[self.stck]
            self._has_stack = True
        except KeyError:
            pass

    def _validate_input(self):
        if self.is_key_list:
            list_id = [self.tab, *self.key, self.val]
        else:
            list_id = [self.tab, self.key, self.val]
        for e in list_id:
            assert e in self.df.columns, f'ID "{e}" not found.'

    def _check_keys(self):
        temp = self.df.loc[:, [self.tab, self.key]].duplicated(keep=False)
        if temp.any():
            print(f'Warning: Duplicates found in Mapper Keys.')
            if self.verbose:
                print(f'\n{self.df.loc[:, [self.tab, self.key]][temp]}')

    def swap_fields(self, field_one, field_two):
        attr_one = self.__getattribute__(field_one)
        attr_two = self.__getattribute__(field_two)
        # TODO remove when 2D field downgrade is done
        assert not self.is_key_list, 'Swapping fields for mapper with "2D Key" is not supported.'
        print(f'Prev mapper fields: {self.df.columns.to_list()}')
        temp = self.df.rename(columns={attr_one: attr_two, attr_two: attr_one})
        print(f'Curr mapper fields: {temp.columns.to_list()}')
        self.df = temp.reindex(columns=self.df.columns)
        print(f'"{attr_one}" is swapped with "{attr_two}".\n')
        if self.verbose:
            self.__repr__()

    # TODO refactoring extract methods
    def _groupby_dict(self):
        pass
    def _groupby_list(self):
        pass
    def _groupby_itertuples(self):
        pass

    def rename_mapper(self):
        return {key: df.set_index(self.key).to_dict()[self.val]
                for key, df in self.df.groupby(self.tab, sort=False)}

    # TODO downgrade key cols to one col only
    # TODO merge 2DKey
    # def merge_2DKey(self):
    #     self.df['strKey'] = self.df['strKey']

    def reindex_mapper(self):
        if self.is_key_list:
            return {k: [*df[self.key].itertuples(False, None)] for k, df in self.df.groupby(self.tab, sort=False)}
        else:
            return {k: df[self.key].to_list() for k, df in self.df.groupby(self.tab, sort=False)}

    def default_mapper(self):
        temp_df = self.df[~self.df[self.dflt].isna()]
        return {k: df.set_index(self.key).to_dict()[self.dflt] for k, df in temp_df.groupby(self.tab, sort=False)}

    def stack_mapper(self):
        temp_df = self.df[~self.df[self.stck].isna()]
        output = {}
        for key, df in temp_df.groupby(self.tab, sort=False):
            output[key] = ({k_stk: v_stk[self.key].to_list() for k_stk, v_stk in df.groupby(self.stck, sort=False)})
        return output

    def reindex_list(self):
        return [*{k: None for k in self[self.val].to_list()}.keys()]


class SubsetMapperCreator(MapperCreator):
    """
    Default ID is provided for 'tab_col', 'subset_col', 'key_cols', 'value_col'.
    Provide 'index_col' for generating subsetting mapper.
    """
    def __init__(self, dataframe: pd.DataFrame, **kwargs):
        super().__init__(dataframe, **kwargs)
        self.df_sub = copy.deepcopy(self.df)
        self.sub = kwargs.setdefault('sub_col', 'strSub')
        self._have_idx = False
        try:
            self.idx = kwargs['uniIdxCol']
            if not isinstance(self.idx, list):
                self.idx = [self.idx]
            self._have_idx = True
            self._remove_idx()
        except KeyError:
            self.idx = None

    def __repr__(self):
        super().__repr__()
        return self.__str__()

    def __str__(self):
        temp = super().__str__()
        return temp + f'\nSub: {self.sub}, Universal Index Column Identifier: {self.idx}'

    def _expand_col_selection(self):
        # TODO if selected cols is tuple expand it so to select two cols
        pass

    def _remove_idx(self):
        if self._have_idx:
            self.df_sub = self.df_sub[~self.df_sub[self.key].isin(self.idx)]
            print(f'Universal Index Column Identifier "{self.idx}" removed from mapper object.\n')

    def subset_mapper(self):
        if self._have_idx:
            temp = {'uniIdxCol': self.idx}
            for (kSub, kTab), df in self.df_sub.groupby([self.sub, self.tab], sort=False):
                grouped = df[self.key]
                column_list = grouped[~(grouped.isin(self.idx))].to_list()
                temp.setdefault(kSub, dict()).setdefault(kTab, column_list)
        else:
            print('No Universal Index Column Identifier passed.\n')
            temp = None
        return temp

    def simple_subset_mapper(self):
        return self.df_sub.to_dict('index')


class DataFrameDict(dict):

    @staticmethod
    def get_rolling_count(input_df, column=None, use_index=False):
        if use_index:
            target_series = pd.Series(input_df.index)
        else:
            target_series = pd.Series(input_df[column])
        return tuple(str(x.value_counts()[x.iloc[-1]]) for idx, x in enumerate(target_series.expanding()))

    def __init__(self, *args, **kwargs):
        init_by = kwargs.pop('init_by') if 'init_by' in kwargs.keys() else []
        init_include = kwargs.pop('init_include') if 'init_include' in kwargs.keys() else False
        init_silence = kwargs.pop('init_silence') if 'init_silence' in kwargs.keys() else True
        super().__init__(*args, **kwargs)
        if len(self.keys()) > 1:
            self._init_filter(init_by, init_include)
        print(f'DFD contains: {[*self.keys()]}\n') if not init_silence else None
        self.isFilled = all([True if type(v) in ['str', 'int', 'float'] else False for v in self.values()])
        print(f'DataFrameDict initiated with {self}.\n') if self.isFilled and self else None

    # TODO just need to reset the index to ensure index is not duplicated
    def _check_idx(self):
        pass

    def _init_filter(self, by, include=True):
        original_keys = set(self.keys())
        if not by:
            by = original_keys
            include = True
        retain_targets = list(original_keys.intersection(by) if include else original_keys.difference(by))
        for e in original_keys:
            if e not in retain_targets:
                self.pop(e)
        assert len(self.keys()) > 0, 'No tabs in excel is selected, check "subset_sheets" and associate params.'

    @property
    def shape(self):
        shape_dict = {}
        for k, v in self.items():
            if isinstance(v, pd.DataFrame):
                shape = v.shape
            elif isinstance(v, pd.Series):
                shape = (v.shape[0], 1)
            else:
                shape = (0, 0)
            shape_dict[k] = shape
        return pd.DataFrame(shape_dict).T

    def total_shape(self, axis=0):
        if axis == 0:
            return self.shape[0].sum(), max(self.shape[1])
        else:
            return max(self.shape[0]), self.shape[1].sum()

    def sort(self):
        return DataFrameDict({k: self[k] for k in sorted(self.keys())})

    def read_csv(self, file_path='.', *args, **kwargs):
        assert self.isFilled, 'DataFrameDict has no initiating params left.'
        for k, v in self.items():
            self[k] = pd.read_csv(UP.Get_Specific_File_Name(v, '.csv', file_path), *args, **kwargs)
        self.isFilled = False

    def rename(self, mapper: dict):
        for k, v in self.items():
            try:
                v.columns = v.columns.to_flat_index()
                self[k] = v.rename(columns=mapper[k])
            except KeyError:
                print(f'KeyError: Dict key "{k}" passed.')
                continue

    def reindex(self, mapper: dict, drop=False):
        for k, v in self.items():
            try:
                self[k] = v.reindex(columns=mapper[k])
            except KeyError:
                if drop:
                    self[k] = None
                else:
                    print(f'KeyError: Dict key "{k}" passed.')
                continue

    def assign_default(self, mapper: dict):
        for k, v in mapper.items():
            try:
                self[k].loc[:, [*v.keys()]] = [*v.values()]
            except KeyError:
                print(f'KeyError: Mapper key "{k}" passed.')
                continue

    def stack(self, mapper: dict, var_name='variable', dropna=True):
        for k_map, v_map in mapper.items():
            if len(v_map.items()) != 1:
                print('Warning: "stack" unpivot to 1 column only, multiple unpivot for 1 dataframe is ignored.')
                print('i.e.:', k_map, v_map, '\n')
            try:
                val_cols = list(v_map.values())[0]
                id_cols = list(self[k_map].columns[~self[k_map].columns.isin(val_cols)])
                value_name = list(v_map.keys())[0]
                self[k_map] = self[k_map].melt(value_vars=val_cols, value_name=value_name,
                                               id_vars=id_cols, var_name=var_name)
                if dropna:
                    isna_series = self[k_map].loc[:, value_name].isna()
                    self[k_map] = self[k_map][~isna_series]
            except KeyError:
                print(f'KeyError: Mapper key "{k_map}" passed.')
                continue

    def complex_stack(self, mapper: dict, var_name='variable'):
        temp = {}
        for k_map, v_map in mapper.items():
            temp_print = []
            print(k_map, v_map)
            try:
                all_cols = list(itt.chain.from_iterable(v_map.values()))

                id_cols = list(self[k_map].columns[~self[k_map].columns.isin(all_cols)])
                self[k_map] = self[k_map].reset_index(drop=True)

                for k_stk, v_stk in v_map.items():
                    output = self[k_map].melt(id_vars=id_cols, var_name=var_name, value_vars=v_stk, value_name=k_stk)
                    if len(list(v_map.keys())) == 1:
                        self[k_map] = output
                    else:
                        temp[(k_map, k_stk)] = output
                        temp_print = []

                if len(list(v_map.keys())) == 1:
                    print(f'Key "{k_map}" is replaced by melted DataFrame.')
                else:
                    print(f'Key "{k_map}" melted into "{len(list(v_map.keys()))}" item(s).')

            except KeyError:
                print(f'KeyError: Mapper key "{k_map}" passed.')
                continue

        if len(list(temp.keys())) > 0:
            print(f'Dict with Keys "{list(temp.keys())}" is returned.')

        return temp

    # TODO Maybe not require uniIdxCol?
    # TODO just need to reset the index to ensure index is not duplicated
    def mapper_subset(self, mapper: dict, isIndexed=True) -> dict:
        outer = dict()
        uniIdxCol = mapper.pop('uniIdxCol')
        if not isIndexed:
            for k, v in self.items():
                try:
                    self[k] = v.set_index(uniIdxCol)
                except KeyError:
                    print(f'Warning: "{k}" does not have Universal Index Columns "{uniIdxCol}".\n')
        for kMap, vMap in mapper.items():
            inner = DataFrameDict(self)
            inner.reindex(vMap, drop=True)
            outer[kMap] = pd.concat(inner.values(), axis=1).reset_index()
        return DataFrameDict(outer)

    def subset(self, mapper: dict):
        assert set(self.keys()).issuperset(set(mapper.keys()))
        for k in set(self.keys()).difference(set(mapper.keys())):
            del self[k]

    # TODO need to standardise mapper columns
    def simple_subset(self, mapper: dict, fill_default=False) -> dict:
        """
        strTab: old tab name, strSub: new tab name, strKey: old col name, strVal: new col name
        if fill_default: , strKey: new col name, strVal: value in the column
        """
        not_found = []
        new_dict = {}
        for e in mapper.values():
            try:
                new_dict.setdefault(e['strSub'], pd.DataFrame())
                # Get number of columns, to insert at the end of all columns
                end_col_idx = new_dict[e['strSub']].shape[1]

                if fill_default and not (e['strKey'] in self[e['strTab']].columns):
                    new_dict[e['strSub']].insert(end_col_idx, e['strKey'], e['strVal'], True)
                else:
                    new_dict[e['strSub']].insert(end_col_idx, e['strVal'], self[e['strTab']][e['strKey']], True)
            except KeyError:
                not_found.append(e)
        if not_found:
            print(f'Warning: The following are not found: {not_found}')
        output_dict = new_dict.copy()
        return output_dict.copy()

    def split_n_stack(self, index_col: str, split_col_target: str, split_col_name: str, stack_col_name: str):
        for k, v in self.items():
            try:
                temp = v[index_col].to_frame().join(v[split_col_target].str.split(',', expand=True))
                temp = temp.melt(id_vars=index_col, var_name=split_col_name, value_name=stack_col_name)
                self[k] = temp.dropna()
            except KeyError:
                print(f'KeyError: Dict key "{k}" passed.')
                continue

    @classmethod
    def filter_valid_args(cls, func, kwargs_dict):
        func_args = inspect.getfullargspec(func)[0]
        valid_args = set(func_args).intersection(set(kwargs_dict.keys()))
        return {k: kwargs_dict[k] for k in valid_args}

    @classmethod
    def filter_invalid_args(cls, func, kwargs_dict):
        func_args = inspect.getfullargspec(func)[0]
        invalid_args = set(kwargs_dict.keys()).difference(set(func_args))
        return {k: kwargs_dict[k] for k in invalid_args}

    def merge(self, mapper: dict):
        # TODO finish
        for seq, v in mapper.items():
            source_list = list(v['source'].values())
            output_list = list(v['output'].values())
            main, supp, *_ = source_list
            output, *_ = output_list
            valid_kwargs = DataFrameDict.filter_valid_args(pd.DataFrame.merge, v['kwargs'])
            temp = self[main].merge(self[supp], suffixes=(None, '_!@#%&'), **valid_kwargs).filter(regex='^(?!.*_!@#%&)')
            temp = temp.drop_duplicates().reset_index(drop=True)
            self.update({output: temp})

    def dropna(self, **kwargs):
        for k, v in self.items():
            try:
                self[k] = v.dropna(**kwargs)
            except KeyError:
                print(f'KeyError: Dict key "{k}" passed.')
                continue

    def append(self, have_keys=False, **kwargs):
        if have_keys:
            keys = self.keys()
        else:
            keys = None
        temp = pd.concat(self.values(), keys=keys, **kwargs)
        if have_keys:
            temp = temp.reset_index(level=0).rename({'level_0': 'Keys'})
        self['Append'] = temp.sort_index()

    # TODO
    def organise_index(self, index_col: list = None, reset_index: bool = True, drop_reset: bool = True, **kwargs):
        for k in self.keys():
            try:
                # print(v.columns)
                if reset_index:
                    self[k] = self[k].reset_index(drop=drop_reset)
                # print(v.columns)
                if index_col:
                    self[k] = self[k].set_index(index_col, **kwargs)
                # print(v.columns)
            except KeyError:
                continue

    def iter_func(self, func, axis=None, sheets: list = None, labels: list = None, verbose: bool = False,
                  *args, **kwargs):
        if verbose:
            string_kwargs = kwargs if kwargs else 'No kwargs'
            print(f'Applying func "{func.__name__}" on DataFrameDict with "{string_kwargs}"')
        for k, v in self.items():
            if sheets and k not in sheets:
                continue
            if not axis:
                if labels:
                    labels_found = [e for e in v.columns.to_list() if e in labels]
                    if verbose:
                        print(f'Applying to Tab: {k} with labels "{labels_found}"')
                    v = func(v.loc[:, labels_found], *args, **kwargs)
                else:
                    if verbose:
                        print(f'Applying to Tab: {k}')
                    v = func(v, *args, **kwargs)
            # else:
            #     if axis == 1:
            #         print(v.columns)
            #         v.columns = func(v.columns, *args, **kwargs)
            #         print(v.columns)
            #     elif axis == 0:
            #         v.index = func(v.index, *args, **kwargs)
            self[k] = v

    def iter_decorator(self, func, verbose=False, *args, **kwargs):
        # TODO
        if verbose:
            print(f'Applied decorator on "{func.__name__}"')

        def inner(*args, **kwargs):
            if verbose:
                print(f'Decorated function iterate through:')
            for k, v in self.items():
                if verbose:
                    print(k)
                try:
                    self[k] = func(v, *args, **kwargs)
                except KeyError:
                    continue
        return inner

    def check_unique(self, column_filter, excluded_tabs=None):
        dictUniq = {k: df.filter(regex=column_filter, axis=1).rename(lambda x: 'Unique', axis=1)
                    for k, df in self.items() if k not in excluded_tabs}
        dfUnique = pd.concat(dictUniq, axis=0).droplevel(-1)
        dfNotUnique = dfUnique[dfUnique.duplicated(keep=False)]
        return dfNotUnique

    def check_columns_diff(self, main_col_filter, supp_col_filter, main_col_name='Main', supp_col_name='Supp',
                           main_excl_tabs=None, supp_excl_tabs=None):
        dictMain = {k: df.filter(regex=main_col_filter, axis=1).rename(lambda x: main_col_name, axis=1)
                    for k, df in self.items() if k not in main_excl_tabs}
        dictSupp = {k: df.filter(regex=supp_col_filter, axis=1).rename(lambda x: supp_col_name, axis=1)
                    for k, df in self.items() if k not in supp_excl_tabs}
        dfMain = pd.concat(dictMain, names=None, axis=0).droplevel(-1).drop_duplicates()
        dfSupp = pd.concat(dictSupp, names=None, axis=0).droplevel(-1).drop_duplicates()
        dfMerge = dfMain.merge(dfSupp, left_on=main_col_name, right_on=supp_col_name, how='left', indicator=True)
        return dfMerge[dfMerge['_merge'] != 'both']

    def export_excel(self, path: str, merge_cells=True, header=True, na_rep='N/A', **kwargs):
        temp_list = self.shape.index[self.shape[0] != 0]
        repr_list = self.shape.index[self.shape[0] == 0]
        # print(temp_list)
        temp = {k: v for k, v in self.items() if k in temp_list}
        if len(repr_list) > 0:
            print(f'{repr_list} are empty.')

        if not merge_cells:
            header = False
            for k, v in temp.items():
                v.columns.names = ['col_' + str(e) for e in v.columns.names]
                temp[k] = v.T.reset_index().T

        with pd.ExcelWriter(path) as writer:
            _ = [v.to_excel(writer, k, merge_cells=merge_cells, header=header, na_rep=na_rep, **kwargs) for k, v in temp.items()]
        # UP.verify_pathname(path, verify_exist=True)

    def print_heads(self, verbose=False, axis=1, melting=False):
        key_List = [*self.keys()]

        def zip_longest_head(head_generator):
            """
            For each DataFrame, generate heads (index or column) packaged in a list,
            Zip the list by zip_longest to get even length, packaged as list of lists,
            Convert to DataFrame and assign the corresponding key
            """
            return pd.DataFrame([*itt.zip_longest(*[*head_generator])], columns=key_List)

        if axis in [0, 'index']:
            temp = zip_longest_head((v._df_index.to_list() for v in self.values()))
        else:
            temp = zip_longest_head((v.columns.to_list() for v in self.values())).T
        if melting:
            temp = temp.stack().reset_index()
        if verbose:
            try:
                display(temp)
            except NameError:
                print(temp)
        return temp

    def idisplay(self):
        ([display(v) for k, v in self.items()])

    # TODO apply on mapper
    # a mapper of func including parameters, the assigned column name and the func name
    # use func name if col name is not provided
    # dictOp = {'Active': {'func': 'sum', 'axis': 1}, 'Dietary': {'func': 'sum', 'axis': 1},
    #           'Resting': {'func': 'quantile', 'axis': 1}}
    # for k, v in dictFiles.items():
    #     dictFiles[k] = v.apply(**dictOp[k])

    # TODO apply concate

    # TODO dict wise filter
    # filter out rows or columns or return a dict of dfMask
    # for k, v in dictInput.items():
    #     dictInput[k] = v[~(v == '').all(axis=1)]
    # for k, v in dictInput.items():
    #     dictInput[k] = v.mask(v == '', '-')
# ----------------------------------------------------------------------------------------------------------------------


def rename_MultiIndexDF(dictMapper, dfInput):
    dfInput.columns = dfInput.columns.to_flat_index()
    return dfInput.rename(columns=dictMapper)


def add_Suffix_DFcolumns(dfInput, strSuffix):
    dfInput = dfInput.add_suffix(strSuffix)
    dfInput.columns = dfInput.columns.str.replace(strSuffix + strSuffix, strSuffix)
    return dfInput


def assign_Default_Values(dfInput, dictValue):
    return dfInput.assign(**dictValue)


def set_dtype(dfInput, datatype):
    _ = [dfInput[strCol].astype(datatype) for strCol in dfInput.columns]
    return dfInput


def Get_Rolling_Duplicate(dfInput, strKey, strName):
    def Count_Rolling_Duplicate(srsGrouped):
        dictElem, lstCount = {}, []
        for Elem in srsGrouped.to_list():
            lstCount.append(dictElem.setdefault(Elem, 0))
            dictElem[Elem] += 1
        return pd.Series(lstCount, index=srsGrouped, name=strName)
    return dfInput.groupby(by=dfInput._df_index, sort=False).apply(lambda x: Count_Rolling_Duplicate(x[strKey]))

# ----------------------------------------------------------------------------------------------------------------------


def join_MultiRows(series, delimiter=', ', drop=True, dropna=False, na_repl='-'):
    if drop:
        series = series.drop_duplicates()
    if dropna:
        series = series.dropna()

    if not (drop and dropna):
        series = series.astype('str').replace('nan', na_repl)
    else:
        series = series.astype('str')

    if series.shape[0] > 1:
        return delimiter.join(series.to_list())
    elif series.shape[0] == 1:
        return series.values[0]
    else:
        return ''


def format_Number(numInput, intSig):
    return '{: .' + intSig + '}'.format(numInput)


def contains_Any_Listed_Values(srsInput, lstValue):
    dfContains = pd.DataFrame([srsInput.str.contains(val) for val in lstValue]).T
    # return dfContains
    return dfContains.any(axis=1)


# ----------------------------------------------------------------------------------------------------------------------

def get_ColRename_Mapper(dfInput, lstKeyCols, strValCol):
    return dfInput.set_index(lstKeyCols).to_dict()[strValCol]


def get_Reindex_Mapper(dfInput, strKeyCol, strValCol):
    return {key: df[strValCol].to_list() for key, df in dfInput.groupby(strKeyCol, sort=False)}


def get_MultiDF_ColRename_Mapper(dfInput, strDFCol, lstKeyCols, strValCol):
    return {key: df.set_index(lstKeyCols).to_dict()[strValCol] for key, df in dfInput.groupby(strDFCol, sort=False)}

# ----------------------------------------------------------------------------------------------------------------------

def rename_dictDataFrames(dictMapper, dictInput):
    return {k: rename_MultiIndexDF(dictMapper[k], v) for k, v in dictInput.items()}

def reindex_DataFramesDict(dictMapper, dictDFs):
    return {key: dictDFs[key].reindex(columns=val) for key, val in dictMapper.items()}


def export_dictDataFrame_AsExcel(strPath, dictDFs, **kwargs):
    with pd.ExcelWriter(strPath) as xwsr:
        for k, v in dictDFs.items():
            v.to_excel(xwsr, k, **kwargs)

def reindex_dictDataFrames(dictMapper, lstDFs):
    print('to be removed')
    # return {key: df.reindex(columns=dictMapper[key]) for key, df in zip(dictMapper.keys(), lstDFs)}
