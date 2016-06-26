from __future__ import with_statement
import re
from itertools import *
from collections import OrderedDict
import pandas as pd
from pandas import DataFrame, Series
import numpy as np
from sparse.core.utils import *
from sparse.core.spql_interpreter import SpQLInterpreter
# ------------------------------------------------------------------------------

'''The sparse_dataframe module contains the SparseDataFrame class

The SparseDataFrame class is an extension of the DataFrame class from the Pandas
library.  SparseDataFrames contain additional methods for converting sparse data,
such as esoteric databases, logs and custom made tables, into SparseDataFrames,
with the actual data residing within a Pandas DataFrame.

Date:
    01.18.2015

Platform:
    Unix

Author:
    Alex Braun <alexander.g.braun@gmail.com> <http://www.AlexBraunVFX.com>
'''

class SparseDataFrame(Base):
    '''
    Class for converting sparse data into well-formated, tabular data

    The SparseDataFrame class which is an extension of the DataFrame class from
    the Pandas library.  SparseDataFrames contain additional methods for converting
    sparse data, such as esoteric databases, logs and custom made tables, into
    SparseDataFrames, with the actual data residing within a Pandas DataFrame,
    acessible through the data attribute.

    Attributes:
        cls (str): Class descriptor
        name (str): Name descriptor
        data (DataFrame): Internal DataFrame where data is actually stored

    Example:
        >>> data = [[ 'joe',  12, 'mechanic'],
        >>> ['bill',  22,  'soldier'],
        >>> [ 'sue',  65,    'pilot'],
        >>> ['jane',  43,  'teacher']]

        >>> columns = ['name', 'age', 'profession']

        >>> sdf = SparseDataFrame(data=data, columns=columns)

        >>> print sdf
        <sparse.core.sparse_dataframe.SparseDataFrame object at 0x10accfed0>

        >>> print sdf.data
           name  age profession
        0   joe   12   mechanic
        1  bill   22    soldier
        2   sue   65      pilot
        3  jane   43    teacher
    '''
    def __init__(self, data=None, index=None, columns=None, dtype=None, copy=False):
        '''SparseDataFrame initializer

        Args:
            data (array-like or DataFrame, optional): Data to be ingested. Default: None
            index (Index or array-like, optional): Index of dataframe. Default: None
            columns (Index or array-like, optional): Column names. Default: None
            dtype (dtype, optional): Data type to force, otherwise infer. Default: None
            copy (bool, optional): Copy data from inputs. Default: None
            name (str, optional): Name of object. Default: None

        Returns:
            SparseDataFrame
        '''
        self._spql = SpQLInterpreter()

        if type(data) is DataFrame:
            self._data = data
        else:
            self._data = DataFrame(data=data, index=index, columns=columns, dtype=dtype, copy=copy)
    # --------------------------------------------------------------------------
    def applymap(self, func, columns=[], errors=False):
        data = self._data
        func_ = func
        if errors == False:
            func_ = lambda x: try_(x, func)

        if isinstance(columns, str):
            columns = [columns]
        if len(columns) == 0:
            data = data.applymap(func_)
        else:
            data[columns] = data[columns].applymap(func_)

        self._data = data
        return self
    # --------------------------------------------------------------------------

    # math
    def set_decimal_expansion(self, expansion, columns=[], errors=False):
        '''Truncates a float item at specified number of digits after the decimal'''
        func = lambda x: set_decimal_expansion(x, expansion)
        return self.applymap(func, columns, errors)

    def round_to(self, order, columns=[], errors=False):
        '''Rounds a given number to a given order of magnitudes after the decimal'''
        func = lambda x: round_to(x, order)
        return self.applymap(func, columns, errors)
    # --------------------------------------------------------------------------

    # info
    def is_iterable(self, columns=[], errors=False):
        '''Returns a boolean mask indicating which elements are iterable

        Args:

        Returns:
            DataFrame mask

        Example:
            >>> print sdf.data
               name  age profession
            0   joe   12   mechanic
            1  bill   22    soldier
            2   sue   65      pilot
            3  jane   43    teacher

            >>> mask = sdf.is_iterable()
            >>> print mask
               name    age profession
            0  True  False       True
            1  True  False       True
            2  True  False       True
            3  True  False       True

            >>> print sdf.data[mask]
               name  age profession
            0   joe  NaN   mechanic
            1  bill  NaN    soldier
            2   sue  NaN      pilot
            3  jane  NaN    teacher
        '''
        return self.applymap(is_iterable, columns, errors)

    def is_listlike(self, columns=[], errors=False):
        '''Determine if an item id listlike'''
        return self.applymap(is_listlike, columns, errors)

    def is_dictlike(self, columns=[], errors=False):
        '''Determine if an item id dictlike'''
        return self.applymap(is_dictlike, columns, errors)

    def is_dict_matrix(self, columns=[], errors=False):
        '''Determine if an item is an iterable of dicts'''
        return self.applymap(is_dict_matrix, columns, errors)
    # --------------------------------------------------------------------------

    # coercion
    def as_type(self, dtype, columns=[], errors=False):
        '''Converts data to specified type, leaving uncoercible types alone

        Args:
            dtype (type): Type to convert data into.


        Returns:
            DataFrame

        Example:
            >>> print sdf.data
               name  age profession
            0   joe   12   mechanic
            1  bill   22    soldier
            2   sue   65      pilot
            3  jane   43    teacher

            >>> print sdf.data.applymap(type)
                       name                   age    profession
            0  <type 'str'>  <type 'np.int64'>  <type 'str'>
            1  <type 'str'>  <type 'np.int64'>  <type 'str'>
            2  <type 'str'>  <type 'np.int64'>  <type 'str'>
            3  <type 'str'>  <type 'np.int64'>  <type 'str'>

            >>> sdf.as_type(str)
            >>> print sdf.data.applymap(type)
                       name           age    profession
            0  <type 'str'>  <type 'str'>  <type 'str'>
            1  <type 'str'>  <type 'str'>  <type 'str'>
            2  <type 'str'>  <type 'str'>  <type 'str'>
            3  <type 'str'>  <type 'str'>  <type 'str'>
        '''
        func = lambda x: as_type(x, dtype)
        return self.applymap(func, columns, errors)

    def as_iterable(self, columns=[], errors=False):
        '''Makes all the elements iterable

        Args:

        Returns:
            DataFrame of iterable elements

        Example:
            >>> print sdf.data
               name  age profession
            0   joe   12   mechanic
            1  bill   22    soldier
            2   sue   65      pilot
            3  jane   43    teacher

            >>> print sdf.as_iterable()
               name   age profession
            0   joe  [12]   mechanic
            1  bill  [22]    soldier
            2   sue  [65]      pilot
            3  jane  [43]    teacher
        '''
        func = lambda x: as_iterable(x)
        return self.applymap(func, columns, errors)

    def coerce_nulls(self, columns=[], errors=False):
        # TODO: make this method faster
        '''Coerce all null elements into np.nan

        Args:

        Returns:
            DataFrame of coerced elements

        Example:
            >>> print sdf.data
               name age profession
            0   joe  12   mechanic
            1  bill  ()         {}
            2   sue  65       [{}]
            3        43    teacher

            >>> print sdf.coerce_nulls()
                   name  age profession
                0   joe   12   mechanic
                1  bill  NaN        NaN
                2   sue   65        NaN
                3   NaN   43    teacher
        '''
        nulls = [   None,      '',      [],      {},      (),      set(),      OrderedDict(),
                   [None],    [''],    [[]],    [{}],    [()],    [set()],    [OrderedDict()],
                   (None),    (''),    ([]),    ({}),    (()),    (set()),    (OrderedDict()),
                           set(''), set([]), set({}), set(()), set(set()), set(OrderedDict())
        ]

        def _coerce_nulls(item):
            if item in nulls:
                return np.nan
            else:
                return item

        func = lambda x: _coerce_nulls(x)
        return self.applymap(func, columns, errors)

    def as_snakecase(self, columns=[], errors=False):
        if isinstance(columns, str):
            columns = [columns]
        return self.applymap(as_snakecase, columns, errors)

    def axis_as_snakecase(self, axis=1):
        self._data.rename_axis(as_snakecase, axis=axis, inplace=True)
        return self
    # --------------------------------------------------------------------------

    # regex
    def regex_match(self, pattern, group=0, ignore_case=False, columns=[], errors=False):
        # May be deprecated in favor of spql_search
        '''Apply regular expression matches to all DataFrame elements

        Args:
            pattern (str): Regular expression pattern to match
            group (int, optional): Regular expression group to return. Default: 0
            ignore_case (bool, optional): Ignore case. Default: False

        Returns:
            DataFrame of regex matches

        Example:
            >>> print sdf.data
               name  age         profession
            0   joe   12  Airplane Mechanic
            1  bill   22            soldier
            2   sue   65              pilot
            3  jane   43            teacher

            >>> print sdf.regex_match('airplane (mechanic)', group=1, ignore_case=True)
               name  age profession
            0   joe   12   Mechanic
            1  bill   22    soldier
            2   sue   65      pilot
            3  jane   43    teacher
        '''
        func = lambda x: regex_match(pattern, x, group=group, ignore_case=ignore_case)
        return self.applymap(func, columns, errors)

    def regex_search(self, pattern, group=0, ignore_case=False, columns=[], errors=False):
        # May be deprecated in favor of spql_search
        '''Apply regular expression searches to all DataFrame elements

        Args:
            pattern (str): Regular expression pattern to search
            group (int, optional): Regular expression group to return. Default: 0
            ignore_case (bool, optional): Ignore case. Default: False

        Returns:
            DataFrame of regex searches

        Example:
            >>> print sdf.data
               name  age                      profession
            0   joe   12  Experimental Airplane Mechanic
            1  bill   22                         soldier
            2   sue   65                           pilot
            3  jane   43                         teacher

            >>> print sdf.regex_search('airplane (mechanic)', group=1, ignore_case=True)
               name  age profession
            0   joe   12   Mechanic
            1  bill   22    soldier
            2   sue   65      pilot
            3  jane   43    teacher
        '''
        func = lambda x: regex_search(pattern, x, group=group, ignore_case=ignore_case)
        return self.applymap(func, columns, errors)

    def regex_sub(self, pattern, repl, count=0, ignore_case=False, columns=[], errors=False):
        '''Apply regular expression substitutions to all DataFrame elements

        Args:
            pattern (str): Regular expression pattern to substitute
            repl (str): String to replace pattern
            group (int, optional): Regular expression group to return. Default: 0
            count (int, optional): Maximum number of occurences to be replaced. Default: 0
            ignore_case (bool, optional): Ignore case. Default: False

        Returns:
            DataFrame of regex substitutions

        Example:
            >>> print sdf.data
               name  age         profession
            0   joe   12  Airplane Mechanic
            1  bill   22            soldier
            2   sue   65              pilot
            3  jane   43            teacher

            >>> print sdf.regex_sub('airplane', 'Helicopter', ignore_case=True)
               name  age           profession
            0   joe   12  Helicopter Mechanic
            1  bill   22              soldier
            2   sue   65                pilot
            3  jane   43              teacher
        '''
        func = lambda x: regex_sub(pattern, repl, x, count=count, ignore_case=ignore_case)
        return self.applymap(func, columns, errors)

    def regex_split(self, pattern, ignore_case=False, columns=[], errors=False):
        '''Splits elements into list of found regular expression groups

        Args:
            pattern (str): Regular expression pattern with groups, ie. "(foo)(bar)"
            ignore_case (bool, optional): Ignore case. Default: False

        Returns:
            DataFrame of with matched elements as lists of groups

        Example:
            >>> print sdf.data
               name  age           profession
            0   joe   12  Helicopter Mechanic
            1  bill   22              soldier
            2   sue   65     helicopter pilot
            3  jane   43              teacher

            >>> sdf.regex_split('(helicopter) (.*)', ignore_case=True)
            >>> print sdf.data
               name  age              profession
            0   joe   12  [Helicopter, Mechanic]
            1  bill   22                 soldier
            2   sue   65     [helicopter, pilot]
            3  jane   43                 teacher
        '''
        func = lambda x: regex_split(pattern, x, ignore_case=ignore_case)
        return self.applymap(func, columns, errors)
    # --------------------------------------------------------------------------

    # internal reshape
    def nan_to_bottom(self, columns=[], errors=False):
        '''Pushes all nan elements to the bottom rows of the DataFrame

        Args:

        Returns:
            DataFrame with nan elements at the bottom

        Example:
            >>> print sdf.data
               name  age profession
            0   joe   12   mechanic
            1  bill  NaN        NaN
            2   sue   65        NaN
            3   NaN   43    teacher

            >>> print sdf.nan_to_bottom()
               name  age profession
            0   joe   12   mechanic
            1  bill   65    teacher
            2   sue   43        NaN
            3   NaN  NaN        NaN
        '''
        func = lambda x: nan_to_bottom()
        return self.applymap(func, columns, errors)

    def unique(self):
        '''Returns a DataFrame of unique values, excluding np.nans

        Args:

        Returns:
            Unique DataFrame

        Example:
            >>> sdf.data
                make  model color  year
            0    gmc    suv  blue  2007
            1  honda    suv  blue  2007
            2   fiat    car  blue  2007
            3    gmc  truck  blue  1999

            >>> sdf.unique()
                make  model color  year
            0    gmc    suv  blue  2007
            1  honda    car   NaN  1999
            2   fiat  truck   NaN   NaN
        '''
        data = self._data
        mask = data.apply(lambda x: x.duplicated())
        data[mask] = np.nan
        data = data.apply(lambda x: nan_to_bottom(x))
        data = data.dropna(how='all')

        self._data = data
        return self

    def invert(self, columns=[], errors=False):
        '''Inverts a given iterable

        Example:
            >>> invert([1,2,3,1,2,3])
            [3,2,1,3,2,1]

            >>> invert(list('abc'))
            ['c', 'b', 'a']

            >>> invert(list('abxy'))
            ['y', 'x', 'b', 'a']
        '''
        return self.applymap(invert, columns, errors)

    def as_inverted_dict(self, key, prototype=True, columns=[], errors=False):
        '''Converts item into inverted index

        Example:
            >>> employee = {'employee': {'name': 'alex', 'id': 123}}
            >>> as_inverted_dict(employee, ['employee', 'name'])
            {'alex': {'employee': {'id': 123, 'name': 'alex'}}}

            >>> employees
            [ {'employee': {'name': 'alex', 'id': 123}},
              {'employee': {'name': 'janus', 'id': 456}},
              {'employee': {'name': 'janus', 'id': 456}}, # duplicate record
              {'employee': {'name': 'atticus', 'id': 789}} ]

            >>> as_inverted_dict(employees, ['employee', 'id'])
            {'123': {'employee': {'id': 123, 'name': 'alex'}},
             '456': {'employee': {'id': 456, 'name': 'janus'}},
             '789': {'employee': {'id': 789, 'name': 'atticus'}}}
        '''
        func = lambda x: as_inverted_inde(x, key, prototype)
        return self.applymap(func, columns, errors)

    def as_flat_list(self, columns=[], errors=False):
        return self.applymap(flatten_list, columns, errors)

    def as_prototype(self, columns=[], errors=False):
        '''Converts items to a prototypical dictionary

        Example:
            >>> people
            [{'first': 'tom',    'last': 'flately'},
             {'first': 'dick',   'last': 'schmidt'},
             {'first': 'harry',  'last': 'schmidt'}]

            # >>> as_prototype(people)
            # { last : ['flately', 'schmidt', 'schmidt']
            #   first : ['tom', 'dick', 'harry'] }

            >>> as_prototype(people)
            { last : [],
              first : [] }
        '''
        return self.applymap(as_prototype, columns, errors)
    # --------------------------------------------------------------------------

    # external reshape
    def flatten(self, columns=[], prefix=True, drop=True, dtype=dict, attach='inplace'):
        '''Split items of iterable elements into separate columns

        Args:
            dtype (type, optional): Columns types to be split. Default: dict
            prefix (bool, optional): Append original column name as a prefix to new columns

        Returns:
            Flattened DataFrame

        Example:
            >>> print sdf.data
                               foo             bar
            0  {u'a': 1, u'b': 10}     some string
            1  {u'a': 2, u'b': 20}  another string
            2  {u'a': 3, u'b': 30}            blah

            >>> sdf.flatten()
            >>> print sdf.data
                foo_a    foo_b             bar
            0       1       10     some string
            1       2       20  another string
            2       3       30            blah
        '''
        if isinstance(columns, str):
            columns = [columns]

        def _reorder_columns(columns, index):
            new_cols = []
            for col in columns:
                if col in index:
                    if not drop:
                        new_cols.append(col)
                    new_cols.extend( index[col] )
                else:
                    new_cols.append(col)
            return new_cols

        col_index = OrderedDict()
        def _flatten(data, columns):
            for col in columns:
                col_index[col] = []
            frames = []
            for col in columns:
                frame = DataFrame(data[col].tolist())
                if prefix:
                    columns = {}
                    for k in frame.columns:
                        columns[k] = str(col) + '_' + str(k)
                    frame.rename(columns=columns)
                frames.append(frame)
                col_index[col].extend( frame.columns.tolist() )
            data = pd.concat(frames, axis=1)
            return data

        data = self._data
        flatdata = data
        old_cols = data.columns.tolist()

        # determine flatenable columns via column mask
        if columns:
            flatdata = flatdata[columns]
        else:
            mask = data.applymap(lambda x: bool_test(type(x), '==', dtype))
            iterables = data[mask]
            iterables = iterables.dropna(how='all', axis=1)
            columns = iterables.columns.tolist()

        # Get right-hand flattened columns
        flatdata = _flatten(flatdata, columns)

        old_cols = data.columns.tolist()

        # drop original columns
        if drop:
            data = data.T.drop(columns).T

        # attach right-hand flattened columns to  original columns
        data = pd.concat([data, flatdata], axis=1)

        # reorganize columns
        if attach == 'inplace':
            cols = _reorder_columns(old_cols, col_index)
            data = data[cols]

        self._data = data
        return self

    def stack_by_column(self, column):
        '''Stacks data according to chunks demarcated by unique elements within
        a given column

        This method is usefull for generating tables that can be easily graphed

        Args:
            column (column name): Column by which to split data into chunks

        Returns:
            Stacked (striped) DataFrame

        Example:
            >>> print sdf.data
               name  age profession
            0   joe   12   Mechanic
            1  bill   22    soldier
            2  bill   25  policeman
            3   sue   65      pilot
            4   sue   14    student
            5   sue   44      nurse
            6  jane   22   engineer
            7  jane   43    teacher

            >>> print sdf.stack_by_column('name')
               joe        joe  bill       bill  sue        sue  jane       jane
               age profession   age profession  age profession   age profession
            0   12   Mechanic    22    soldier   65      pilot    22   engineer
            1  NaN        NaN    25  policeman   14    student    43    teacher
            2  NaN        NaN   NaN        NaN   44      nurse   NaN        NaN
        '''
        frames = []
        max_len = 0
        cols = list(self._data.columns.drop(column))
        grps = self._data.groupby(column)

        for item in self._data[column].unique():
            group = grps.get_group(item)
            group = group[cols]
            group.columns = [[item] * len(cols), cols]
            frames.append(group)

            if len(group) > max_len:
                max_len = len(group)

        for frame in frames:
            bufr = frame.head(1)
            bufr = bufr.apply(lambda x: np.nan)

            buf_len = max_len - len(frame)
            for i in range(buf_len):
                bufr.append(bufr)
            frame.append(bufr, ignore_index=True)
            frame.reset_index(drop=True)

        data = pd.concat(frames, axis=1)

        self._data = data
        return self

    def unstripe(self):
        '''Reduced striped DataFrame into DataFrame with unique columns

        Args:

        Returns:
            Unstriped DataFrame

        Example:
            >>> print sdf.data
              name profession  name profession  name profession  name profession
            0  joe   Mechanic  bill    soldier  bill  policeman  jane    teacher
            1  sue      pilot   NaN        NaN  jane   engineer   NaN        NaN

            >>> print sdf.unstripe()
               name profession
            0   joe   Mechanic
            1   sue      pilot
            2  bill    soldier
            3   NaN        NaN
            4  bill  policeman
            5  jane   engineer
            6  jane    teacher
            7   NaN        NaN
        '''
        data = self._data.reset_index(level=1, drop=True)

        new_cols = data.columns.unique().tolist()
        cols = Series(data.columns)
        mask = cols.duplicated()
        bad_cols = cols[mask].tolist()
        good_cols = set(cols.unique()).difference(bad_cols)

        items = []
        for col in new_cols:
            if col in bad_cols:
                item = data[col].unstack()
                item = Series(item.values)
                items.append(item)
            else:
                items.append(data[col])

        self.concat_irregular(items, axis=1, ignore_index=False)
        self._data.columns = new_cols
        return self

    def merge_columns(self, columns, func='default', new_column='default',
                    iterables=False, drop=False):
        '''Merge columns according to supplied or default function

        Args:
            columns (list): Columns to be merged
            func (func optional): Func used to merge columns. Default: default
            new_column (str optional): Name of merged column. Default: a_b...
            iterables (bool optional): True if any column contains an iterable. Default: False
            drop (bool optional): Drop columns to be merged. Default: False

        Example:
            >>> print sdf.data
              first_name last_name  age    job_1    job_2
            0       john   jenkins   23    pilot     None
            1       jane     smith   46  surgeon     None
            2      harry    harmon   27  teacher  chemist
            3        sue     marie   78    nurse    baker

            >>> print sdf.merge_columns(['first_name', 'last_name'])
              first_name last_name  age    job_1    job_2 first_name_last_name
            0       john   jenkins   23    pilot     None          johnjenkins
            1       jane     smith   46  surgeon     None          janesmith
            2      harry    harmon   27  teacher  chemist          harryharmon
            3        sue     marie   78    nurse    baker          suemarie

            >>> print sdf.merge_columns(['job_1', 'job_2'],
                func=lambda x: [ x[x.index[0]], x[x.index[1]] ],
                new_column='jobs')

              first_name last_name  age    job_1    job_2                jobs
            0       john   jenkins   23    pilot     None       [pilot, None]
            1       jane     smith   46  surgeon     None     [surgeon, None]
            2      harry    harmon   27  teacher  chemist  [teacher, chemist]
            3        sue     marie   78    nurse    baker      [nurse, baker]

            >>> print sdf.merge_columns(['job_1', 'job_2'],
                func=lambda x: {'1st': x[x.index[0]], '2nd': x[x.index[1]] },
                new_column='jobs', drop=True)
              first_name last_name  age                                      jobs
            0       john   jenkins   23          {u'2nd': None, u'1st': u'pilot'}
            1       jane     smith   46        {u'2nd': None, u'1st': u'surgeon'}
            2      harry    harmon   27  {u'2nd': u'chemist', u'1st': u'teacher'}
            3        sue     marie   78      {u'2nd': u'baker', u'1st': u'nurse'}

        Returns:
            DataFrame
        '''
        if isinstance(columns, str):
            columns = [columns]

        def _add(item):
            output = item[item.index[0]]
            for i, col in enumerate(columns[1:]):
                output += item[item.index[i + 1]]
            return output

        def _append(item):
            output = []
            for i, col in enumerate(columns):
                output.append(item[item.index[i]])
            return output

        if func == 'default':
            func = _add
            if iterables:
                func = _append

        data = self._data.copy()
        result = data[columns]
        result = result.T.apply(lambda x: [func(x)]).T
        result = result.apply(lambda x: x[0])

        col = '_'.join([str(x) for x in columns])
        if new_column != 'default':
            col = new_column
        data[col] = result

        if drop:
            for col in columns:
                del data[col]

        self._data = data
        return self

    def insert_level(self, item, level=0):
        '''Insert level into index

        Example:
            >>> data
                       0  1  2
            level_1 0  0  1  2
                    1  3  4  5
                    2  6  7  8

            >>> index = insert_level(data.index, 'new_level_1', 1)
            >>> data.index = index
            >>> data
                                   0  1  2
            level_1 new_level_1 0  0  1  2
                                1  3  4  5
                                2  6  7  8
        '''
        index = self._data.index
        index = index_to_matrix(index)
        item = [item] * len(index[0])
        index.insert(level, item)
        self._data.index = index
        return self
    # --------------------------------------------------------------------------

    # simulation
    def recurse(self,
                nondict_func=lambda store, key, val: val,
                dict_func=lambda store, key, val: val,
                key_func=lambda key: key,
                columns=[],
                errors=False):

        func = lambda x: recurse(x, nondict_func, dict_func, key_func, {})
        return self.applymap(func, columns, errors)

    def traverse(self, key_func, val_func, traverse='depth', replace=False):
        data = self._data.copy()

        spindex = data.columns.tolist()
        spindex = [x for x in spindex if re.search('sp_index', x)]

        index = data[spindex]
        _index = index.as_matrix().tolist()
        if traverse == 'breadth':
            index = data[spindex].transpose()
        index = index.as_matrix().tolist()

        for r, row in enumerate(index):
            for k, key in enumerate(row):
                if key_func(key):

                    ind = r
                    col = k
                    if traverse == 'breadth':
                        ind = k
                        col = r

                    try:
                        data.loc[ind, 'values'] = val_func(data.loc[ind, 'values'])
                    except:
                        continue

                    if replace:
                        column = spindex[col]
                        mask = data[column].apply(lambda x: x != key)
                        mask.ix[ind] = True
                        data = data[mask]

                        branch = _index[ind][0:col + 1]
                        while len(branch) < len(spindex):
                            branch.append('-->')
                        data.loc[ind, spindex] = branch
                        break

        self._data = data
        return self

    def get_revolutions(self, start=None, stop=None):
        def _get_revolutions(left, right):
            output = []
            for l in left:
                for r in right:
                    temp = [l]
                    if isinstance(l, list):
                        temp = copy(l)
                    temp.append(r)
                    output.append(temp)
            return output

        cols = self._data.columns.tolist()
        data = self._data[cols[0]].dropna().tolist()
        for col in cols[1:]:
            data = _get_revolutions(data, self._data[col].dropna().tolist())
        data = DataFrame(data, columns=cols)

        if start:
            start = data.T.apply(lambda x: ''.join(x.tolist()) == start).T
            start = data[start].index.values[0]
            data = data.ix[start:]

        if stop:
            stop = data.T.apply(lambda x: ''.join(x.tolist()) == stop).T
            stop = data[stop].index.values[0]
            data = data.ix[:stop]

        data.reset_index(drop=True)

        self._data = data
        return self
    # --------------------------------------------------------------------------

    # io
    def to_dataframe(self):
        return self._data

    def concat_irregular(self, items, axis=0, ignore_index=True):
        '''Concatenate DataFrames of different dimensions

        Example:
            >>> x = DataFrame(np.arange(0, 9).reshape(3, 3))
            >>> y = DataFrame(np.arange(100, 116).reshape(4, 4))
            >>> concat_irregular([x, y], axis=1)
                0   1   2    0    1    2    3
            0   0   1   2  100  101  102  103
            1   3   4   5  104  105  106  107
            2   6   7   8  108  109  110  111
            3 NaN NaN NaN  112  113  114  115
        '''
        max_len = 0
        for item in items:
            if len(item) > max_len:
                max_len = len(item)

        for item in items:
            bufr = item.head(1)
            bufr = bufr.apply(lambda x: np.nan)
            buf_len = max_len - len(item)
            for i in range(buf_len):
                if ignore_index:
                    item.append(bufr, ignore_index=ignore_index)
                else:
                    item.append(bufr)

        self._data = pd.concat(items, axis=axis)
        return self

    def concat_hierarchical(self, items):
        '''Concatenates multiple DataFrames with hierarchical indexes

        Args:
            items (list): List of DataFrames to be concat_hierarchicald.

        Returns:
            DataFrame

        Example:
            >>> joe
                                      values
            employee username -->      jabra
                     age      -->         67
                     name     last   abrante
                              first      joe
            id       -->      -->          1

            >>> jane
                                      values
            employee username -->    jjoplin
                     age      -->         53
                     name     last    joplin
                              first     jane
            id       -->      -->          3

            >>> concat_hierarchical([joe, jane])
                                      values
            employee username -->      jabra
                     age      -->         67
                     name     last   abrante
                              first      joe
            id       -->      -->          1
            employee username -->    jjoplin
                     age      -->         53
                     name     last    joplin
                              first     jane
            id       -->      -->          3
        '''
        items = [x.copy() for x in items]
        max_ = 0
        indexes = []
        for item in items:
            index = index_to_matrix(item.index)
            if max_ < len(index):
                max_ = len(index)
            indexes.append(index)

        for i, index in enumerate(indexes):
            top_level = index[0]
            for r in range(max_ - len(index)):
                index.insert(0, top_level)
            items[i].index = index

        output = items[0]
        for item in items[1:]:
            output = output.append(item)

        self._data = output
        return self

    def from_nested_dict(self, item, sp_index=False, justify='left'):
        '''Reads nested dictionary into a DataFrame

        Args:
            item (dict): Dictionary to be read
            name (str): Name of dictionary

        Returns:
            DataFrame
        '''
        values = flatten_nested_dict(item).values()
        data = None
        if sp_index:
            index = nested_dict_to_matrix(item, justify=justify)
            columns = []
            for i, item in enumerate(index[0]):
                columns.append("sp_index_" + str(i).zfill(2))
            data = DataFrame(index, columns=columns)
        else:
            index = nested_dict_to_index(item, justify=justify)
            data = DataFrame(index=index)
        data['values'] = values
        mask = data.apply(lambda x: x != 'null')
        mask = mask[mask].dropna()
        data = data.ix[mask.index]
        if sp_index:
            data.reset_index(drop=True)

        self._data = data
        return self

    def to_nested_dict(self):
        if 'sp_index_00' in self._data.columns:
            temp = self._data.values.tolist()
            matrix = []
            for row in temp:
                matrix.append([x for x in ifilter(lambda x: x != '-->', row)])
            return matrix_to_nested_dict(matrix)

    def to_inverted_dict(self, columns, key, prototype=True):
        '''Converts a list of columns containing dict or dict matrices to an inverted index

        Example:
            >>> sdf.data

        '''
        if isinstance(columns, str):
            columns = [columns]

        key = key.split('.')

        index = []
        for col in columns:
            data = self._data[col]
            index = data[data.apply(lambda x: is_dictlike(x))].tolist()
            lists = data[data.apply(lambda x: is_listlike(x))].tolist()
            for lst in lists:
                for item in lst:
                    index.append(item)
        return as_inverted_dict(index, key, prototype)

    def index_to_matrix(self):
        '''Convert a index to a matrix

        Example:
            >>> data.index
            MultiIndex(levels=[[u'A', u'B', u'C'],
                               [u'a', u'b', u'c']],
                       labels=[[0, 1, 2],
                               [0, 1, 2]])

            >>> index_to_matrix(data.index)
            [['A', 'B', 'C']
             ['a', 'b', 'c']]
        '''
        return index_to_matrix(self._data.index)
    # --------------------------------------------------------------------------

    # search
    def spql_search(self, string, field_operator='=='):
        '''Query data using the Sparse Query Language (SpQL)

        Args:
            string (str): SpQL search string
            field_operator (str): Advanced feature, do not use.  Default: '=='

        Returns:
            Queried (likely reduced) DataFrame

        Example:
            >>> print sdf.data
                name  age
            0    abe   15
            1  carla   22
            2   jack   57

            >>> sdf.spql_search('(name) is (abe) | (age) < (50)')
            >>> print sdf.data
                name  age
            0    abe   15
            1  carla   22

        Search:
                       query: (<field>, <field> ...) <operator> (<value>, <value> ...)
            multiple queries: <query> | <query> | <query> ...
                     example: (name) contains (jupiter)
                     example: (name) notcont (scratch, test)
                     example: (priority) < (2501)
                     example: (name) contains (jupiter) | (name) notcont (scratch, test) | (priority) < (2501)

        Field:
            A field is a known key of a database item.

            For instance, if a database exists as a table, then the column headers
            would count as fields.  A database of people might have the keys:
            name, height, weight and age.

            All fields (even single ones), must be surrounded by parenthesis
            use double quotes ("") to capture spaces and other special characters

        Value:
            A value is the, often unknown, datum associated with the key of a
            database item.

            For instance, if a database exists as a table, then the data
            contained in the body would count as values.  A database of people
            might have an item with the keys: name, height, weight, age, with
            values of say Steve, 180", 210lbs, 25.

            All values (even single ones), must be surrounded by parenthesis
            use double quotes ("") to capture spaces and other special characters

        Operator:
            An operator is a symbol which denotes an operation to be performed
            on a given database with the left and right operands (field and values)
            as its arguments.

            For instance, in arithematic, the addition operator (+) combines
            its two operands into a single value (2 + 2 = 4).  In SpQL, an
            operator peforms a test on the values of the fields denoted as
            the left operands, using the values denoted in the right operands
            as criteria.  So, "(name) contains (jupiter)" searches all the
            items in a database whose names contain the string "jupiter".

            Operators include:
                                          is,   =  :  exact match
                           is not,     isnot,  !=  :  does not match
                     greater than,        gt,   >  :  greater than
            greater than equal to,       gte,  >=  :  greater than or equal to
                        less than,        lt,   <  :  less than
               less than equal to,       lte,  <=  :  less than or equal to
                         contains,      cont,   ~  :  contains
                 does not contain,   notcont,  !~  :  does not contain
                       cscontains,    cscont,  ~~  :  contains (case sensitive)
               does not cscontain, csnotcont, !~~  :  does not contain (case sensitive)

        And:
            The AND operator (&) is a means of chaining queries together.  It pipes the
            results of the left query into the right query as its new, smaller
            database.

        Or:
            The OR operator (|) is a means of concatenating multiple queries into a
            single result.  Both operands are executed as independent queries and
            their results are then merged together with duplicate rows removed.
        '''
        self._spql.search(string)
        data = self._spql.dataframe_query(self._data, field_operator=field_operator)

        self._data = data
        return self
# ------------------------------------------------------------------------------

def main():
    '''
    Run help if called directly
    '''

    import __main__
    help(__main__)

__all__ = ['SparseDataFrame']

if __name__ == '__main__':
    main()
