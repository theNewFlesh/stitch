import json
from pandas import Series
from stitch.core.stitch_dataframe import StitchFrame
from stitch.core.utils import *
from stitch.core.errors import *
# ------------------------------------------------------------------------------

'''
.. module:: backingstore
	:platform: Unix
	:synopsis: Backingstore base for interfacing with Probe API

.. moduleauthor:: Alex Braun <alexander.g.braun@gmail.com>
'''

class BackingStore(Base):
	def __init__(self, name=None):
		super(BackingStore, self).__init__(name=name)
		self._cls = 'BackingStore'
		self._data = None
		self._results = None

	@property
	def source_data(self):
		# method for retrieving source data
		raise EmptyFunction('Please define this function in your subclass')

	@property
	def data(self):
		return self._data

	@property
	def results(self):
		return self._results
	# --------------------------------------------------------------------------

	def get_database(self):
		self.update()
		database = {}
		database['metadata'] = {}
		database['metadata']['data_type'] = 'json orient=records, DataFrame'
		database['data'] = self._data.to_json(orient='records')
		return database

	def update(self):
		data = []
		for datum in self.source_data:
			data.append(Series(datum))
		sdata = StitchFrame(data)
		sdata.flatten()
		sdata.coerce_nulls()
		data = sdata._data
		# data.dropna(how='all', axis=1, inplace=True)
		data['probe_id'] = data.index
		sdata._data = data
		self._data = sdata
	# --------------------------------------------------------------------------

	def _execute_instruction(self, instruction):
		func = instruction['func']
		args = instruction['args']
		kwargs = instruction['kwargs']
		getattr(self, func)(*args, **kwargs)

	def process_order(self, order):
		order = json.loads(order)
		dtype = order['metadata']['data_type']
		if dtype == u'json orient=records, DataFrame':
			results = StitchFrame()
			results.read_json(order['data'], orient='records')
			self._results = results
		else:
			raise TypeError('Unrecognized data type: ' + dtype)

		for instruction in order['instructions']:
			self._execute_instruction(instruction)
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['BackingStore']

if __name__ == '__main__':
	main()
