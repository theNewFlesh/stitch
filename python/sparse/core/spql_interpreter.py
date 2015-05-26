#! /usr/bin/env python
# Alex Braun 01.18.2015

# ------------------------------------------------------------------------------
# The MIT License (MIT)

# Copyright (c) 2014 Alex Braun

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
# ------------------------------------------------------------------------------

'''
.. module:: spql_interpreter
	:date: 01.18.2015
	:platform: Unix
	:synopsis: Sparse Query Langauge interpreter

.. moduleauthor:: Alex Braun <ABraunCCS@gmail.com>
'''
# ------------------------------------------------------------------------------

import numpy
import pandas
from sparse.core.spql_parser import SpQLParser
from sparse.utilities.utils import *
# ------------------------------------------------------------------------------

class SpQLInterpreter(SpQLParser):
	'''
	Subclass of SpQLParser used for performing SpQL queries on supplied DataFrames

	Attributes:
		cls (str): Class descriptor.
		name (str): Name descriptor.
		last_search (str): Last SpQL query generated. Default: None.
		search_stats(str): Print statistics about the last query made.
	'''
	def __init__(self, name=None):
		'''
		SparseInterpreter initializer
		Args:
			name (str, optional): Name of object. Default: None
		'''
		super(SpQLInterpreter, self).__init__(name=name)
		self._cls = 'SpQLInterpreter'

	# deprecated
	def _gen_mongo_query(self, fields, operator, values):
		ops = {'==': '$in', '!=': '$ne', '>': '$gt', '>=': '$gte', '<': '$lt', 
		'<=': '$lte', 're': '$regex', 'nre': '$regex', 're.IGNORECASE': '$regex', 'nre.IGNORECASE': '$regex'}

		op = ops[operator]
		subquiries = []
		for field in fields:
			for value in values:
				subquery = {}
				subquery[field] = {op: value}
				if 'IGNORECASE' in operator:
					subquery[field]['$options'] = 'i'
				subquiries.append(subquery)

		mod_op = '$or'
		if 'nre' in operator:
			mod_op = '$not'

		return {'$match': {mod_op: subquiries}}

	# deprecated
	@property
	def mongo_query(self):
		return [self._gen_mongo_query(q['fields'], q['operator'], q['values']) for q in self._last_query]
	# --------------------------------------------------------------------------

	def _gen_dataframe_query(self, dataframe, fields=['all'], operator='==', values=[''], field_operator='=='):
		'''
		Semi-private method for processing invidual SpQL queries.

		Args:
			dataframe (DataFrame): DataFrame to query.
			fields (list, optional): Fields to query. Default: ['all'].
			operator (str, optional): SpQL operator to use in the query. Default '=='.
			values (list, optional): Values to look for. Default [''].

		Returns:
			Results DataFrame
		'''
		columns = dataframe.columns.to_series()
		if fields != ['all']:
			mask = columns.apply(lambda x: bool_test(x, field_operator, fields))
			columns = columns[mask]
		columns = columns.tolist()

		mask = dataframe[columns].applymap(lambda x: bool_test(x, operator, values))
		# This method avoids including prexisting nan values in the mask
		mask[mask == False] = numpy.nan
		mask.dropna(how='all', subset=columns, inplace=True)
		return mask.index
		
	def dataframe_query(self, dataframe, field_operator='=='):
		'''
		Query supplied DataFrame using last search.

		Args:
			dataframe (DataFrame): DataFrame to query.
			field_operator (str, optional): Operator used for determining matching fields. Default '=='.

		Returns:
			Results DataFrame
		'''
		if dataframe.index.has_duplicates:
			raise IndexError('DataFrame has non-unique values in its index')

		mask = pandas.Index([])
		for queries in self._last_search:
			and_mask = dataframe.index
			for q in queries:
				and_mask = self._gen_dataframe_query(dataframe.ix[and_mask], q['fields'], q['operator'], q['values'], field_operator=field_operator)
			mask = mask.union(and_mask)
		return dataframe.ix[mask]
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['SpQLInterpreter']

if __name__ == '__main__':
	main()