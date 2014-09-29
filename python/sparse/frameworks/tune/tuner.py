#! /usr/bin/env python
# Alex Braun 04.13.2014

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
.. module:: tuner
	:date: 08.22.2014
	:platform: Unix
	:synopsis: Configuration framework

.. moduleauthor:: Alex Braun <ABraunCCS@gmail.com>
'''
# ------------------------------------------------------------------------------

from __future__ import with_statement
from collections import OrderedDict
import warnings
import os
import json
import importlib
import pandas
from sparse.utilities.utils import Base
from sparse.utilities.utils import interpret_nested_dict
from sparse.frameworks.tune import tune_imports
from sparse.core.sparse_lut import SparseLUT
# ------------------------------------------------------------------------------

class Tuner(Base):
	def __init__(self, name=None):
		super(Tuner, self).__init__(name=name)
		self._cls = 'Tuner'
		self._imports = {}
		self._config = {}
		self._config_path = None
		self._lut = None
		self.update()

	def update(self):
		self._config = {}
		reload(tune_imports)
		root = tune_imports.CONFIG_PATH
		self._config_path = root

		all_files = os.listdir(root)
		all_files = [os.path.join(root, x) for x in all_files]
		configs = [x for x in all_files if os.path.splitext(x)[1] == '.config']
		for conf in configs:
			with open(os.path.join(root, conf)) as config:
				config = json.loads(config.read())
				for key, value in config.iteritems():
					if key in self._config.keys():
						if type(value) is dict and type(self._config[key]) is dict:
							value = dict(self._config[key].items() + value.items())
						else:
							warnings.warn('Non-unique primary keys detected: ' + value, Warning)
					self._config[key] = value

		luts = [x for x in all_files if os.path.splitext(x)[1] == '.lut']
		master_luts = []
		for lut in luts:
			data = pandas.read_table(lut, delim_whitespace=True, index_col=False)
			master_luts.append(data)
		
		master_lut = None
		if len(master_luts) > 0:
			if len(master_luts) > 1:
				master_lut = pandas.concat(master_luts, axis=1)
			else:
				master_lut = master_luts[0]
			self._lut = SparseLUT(master_lut)

	def resolve_config(self):
		self._imports = {}
		from sparse.frameworks.tune import tune_imports
		IMPORTS = tune_imports.get_imports()
		self._imports = IMPORTS
		self._config = interpret_nested_dict(self._config,
						lambda x: IMPORTS[x] if x in IMPORTS.keys() else x)

	def resolve(self, config):
		path = config['path']
		module = os.path.basename(path).split('.')[0]

		if 'class' in config.keys():
			key = getattr(importlib.import_module(module, path), config['class']) 
		else:
			key = importlib.import_module(module, path)

	def tune(self, items, lut_index):
		input_lut = self._config[lut_index]['input_lut']
		output_lut = self._config[lut_index]['output_lut']
		return self._lut.transform_items(items, input_lut, output_lut)
			
	@property
	def config(self):
		return self._config

	@property
	def config_path(self):
		return self._config_path

	@property
	def imports(self):
		return self._imports
# ------------------------------------------------------------------------------
def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['Tuner']

if __name__ == '__main__':
	main()
