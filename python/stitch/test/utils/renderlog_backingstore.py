import os
import warnings
import re
import numpy as np
import pandas as pd
from pandas import DataFrame
from stitch.core.stitch_frame import StitchFrame
from stitch.frameworks.probe.backingstore import BackingStore
from stitch.test.utils.renderlog_utils import *
from stitch.core.utils import *
from stitch.core.errors import *
# ------------------------------------------------------------------------------

'''
.. module:: renderlog_backingstore
	:platform: Unix
	:synopsis: Error Log Backing Store for interfacing with Probe API

.. moduleauthor:: Alex Braun <alexander.g.braun@gmail.com>
'''

class RenderLogBackingStore(BackingStore):
	def __init__(self, path=None, text=None, expand=False):
		super(RenderLogBackingStore, self).__init__()

		self._data = None
		self._results = None
		self._path = path
		self._text = text
		self._expand = expand
	# --------------------------------------------------------------------------

	@property
	def path(self):
		return self._path

	def _get_dir_data(self, path):
		output = []
		for root, dirs, files in os.walk(path):
			for f in files:
				file_ = os.path.join(root, f)
				with open(file_, 'r') as log:
					readlines = log.readlines()
					if len(readlines) > 0:
						data = [[x, file_] for x in readlines]
						output.append(data)
		return output

	@property
	def source_data(self):
		if self._text:
			output = self._text.split('\n')
			output = [[x, ''] for x in output]
			return [output]
		elif self._path:
			if os.path.isdir(self._path):
				return self._get_dir_data(self._path)
			else:
				with open(self._path, 'r') as log:
					output = [[x, self._path] for x in log.readlines()]
					# output = DataFrame(output)
					return [output]
		else:
			raise NotFound('Please provide a file path, directory path or readlines data')

	def _log_data(self, data):
		data = DataFrame(data, columns=['raw_data', 'filepath'])
		data = data.applymap(lambda x: x.strip('\n'))
		data['line'] = data.index
		data['line'] += 1
		data = data[data['raw_data'] != '']

		data['filename'] = data['filepath'].apply(lambda x: os.path.basename(x))

		data['warning'] = data['raw_data'].apply(lambda x: get_warnings(x, logtype='mental ray'))

		data['progress'] = np.nan
		prog = data['raw_data'].apply(lambda x: get_progress(x, logtype='alfred'))
		if len(prog) > 0:
			mask = prog.tail(1)
			data.loc[mask.index, 'progress'] = mask.tolist()[0]

		data2 = data.dropna(subset=['warning'])
		data2.reset_index(drop=True, inplace=True)

		err = data['raw_data'].apply(lambda x: get_errors(x))
		data['error'] = err

		if self._expand:
			tbs = data['raw_data'].apply(lambda x: get_tracebacks(x)).dropna()

			# get traceback text chunks (ie traceback to error, traceback to error)
			tbs = tbs.index.tolist()
			errs = err.dropna().index.tolist()
			data['chunk_id'] = np.nan
			chunks = []
			chunk_ids = []
			if tbs and errs:
				c = 0
				for t, e in zip(tbs, errs):
					if e - t < 100:
						new_chunk = range(t, e + 1)
						chunks.extend(new_chunk)
						mask = [str(c).zfill(3) for x in new_chunk]
						chunk_ids.extend(mask)
						c += 1

				# drop data not in chunks
				data = data.ix[chunks]

				# assign ids to each chunk
				data['chunk_id'] = chunk_ids
				dups = err.drop_duplicates().index.tolist()
				drop = data['chunk_id'].ix[dups].unique().tolist()
				mask = data['chunk_id'].apply(lambda x: x in drop)

				# remove duplicate chunks by error
				data = data[mask]

				err_name = data['error'].bfill().apply(lambda x: x[0:3])
				data['chunk_id'] = err_name + data['chunk_id']
				data['chunk_id'] = data['chunk_id'].apply(lambda x: x.lower())

			data['chunk'] = data['error'].bfill()

			data['traceback_line'] = data['raw_data'].apply(lambda x: get_traceback_line(x))
			data['traceback_file'] = data['raw_data'].apply(lambda x: get_traceback_file(x))

		# merge traceback data and warning data
		data = pd.concat([data, data2])

		data = data.applymap(lambda x: np.nan if x == '' else x)

		if not self._expand:
			data = data[['line', 'warning', 'error', 'progress', 'filepath', 'filename']]
			data.drop_duplicates(['warning', 'error', 'progress'], inplace=True)
			data.dropna(subset=['warning', 'error', 'progress'], how='all', inplace=True)

		data = data.sort_values(['filename', 'line'])
		return data

	def update(self):
		data = []
		for datum in self.source_data:
			data.append(self._log_data(datum))
		if len(data) > 1:
			data = pd.concat(data)
		else:
			data = data[0]

		data.reset_index(drop=True, inplace=True)
		data.fillna('', inplace=True)

		data['probe_id'] = data.index

		sdata = StitchFrame(data)
		self._data = sdata
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['RenderLogBackingStore']

if __name__ == '__main__':
	main()
