from __future__ import with_statement
from collections import OrderedDict
import json
from stitch.core.errors import *
from stitch.core.utils import *
from stitch.core.stitch_interpreter import StitchInterpreter
from stitch.core.stitch_dataframe import StitchFrame
# ------------------------------------------------------------------------------

'''
.. module:: probe_api
	:platform: Unix
	:synopsis: API for Probe BackingStore

.. moduleauthor:: Alex Braun <alexander.g.braun@gmail.com>
'''

class ProbeAPI(Base):
	def __init__(self, backingstore, updates='automatic', name=None):
		super(ProbeAPI, self).__init__(name=name)
		self._cls = 'ProbeAPI'

		if updates not in ['manual', 'automatic']:
			raise NameError('Improper update mode supplied. Should be manual or automatic. Value provided: ' + updates)

		self._backingstore = backingstore
		self._updates = updates
		self._database = None
		self._results = None
		self._interpreter = StitchInterpreter()
		self._mongodb = None
		self._elasticsearch = None
		if self._updates == 'manual':
			self.update()

	@property
	def database(self):
		if self._updates is 'automatic':
			self.update()
		return self._database

	@property
	def data(self):
		return self.database['data']

	@property
	def metadata(self):
		return self._database['metadata']

	@property
	def data_type(self):
		return self._database['metadata']['data_type']

	@property
	def results(self):
		return self._results

	def update(self):
		self._database = self._backingstore.get_database()
	# --------------------------------------------------------------------------

	@property
	def updates(self):
		return self._updates

	def manual_updates(self):
		self._updates = 'manual'

	def automatic_updates(self):
		self._updates = 'automatic'
	# --------------------------------------------------------------------------

	def stitch_search(self, string, field_operator='==', display_fields=[]):
		results = StitchFrame()
		results.read_json(self.data)
		results.stitch_search(string, field_operator=field_operator)

		if len(results.data) == 0:
			raise NotFound('No search results found')

		if display_fields:
			results._data = results._data[display_fields]

		results = results._data.to_json(orient='records')
		self._results = results
	# --------------------------------------------------------------------------

	def send_order(self, instructions):
		order = {}
		order['data'] = self._results
		order['metadata'] = self.metadata
		order['instructions'] = instructions
		order = json.dumps(order)

		self._backingstore.process_order(order)
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['ProbeAPI']

if __name__ == '__main__':
	main()
