import numpy
import pandas
from sparse.core.spql_parser import SpQLParser
from sparse.core.utils import *
# ------------------------------------------------------------------------------

'''
.. module:: spql_interpreter
	:date: 01.18.2015
	:platform: Unix
	:synopsis: Sparse Query Langauge interpreter

.. moduleauthor:: Alex Braun <alexander.g.braun@gmail.com>
'''

class SpQLInterpreter(SpQLParser):
	'''
	Subclass of SpQLParser used for performing SpQL queries on supplied DataFrames

	Attributes:
		cls (str): Class descriptor.
		name (str): Name descriptor.
		last_search (str): Last SpQL query generated. Default: None.
		search_stats(str): Print statistics about the last query made.
	'''
	def __init__(self):
		'''
		SparseInterpreter initializer
		Args:
			name (str, optional): Name of object. Default: None
		'''
		super(SpQLInterpreter, self).__init__()
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
