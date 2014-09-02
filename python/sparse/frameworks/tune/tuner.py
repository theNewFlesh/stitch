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
from sparse.utilities.utils import Base
from sparse.utilities.utils import interpret_nested_dict
from sparse.utilities.utils import double_lut_transform
from sparse.utilities.utils import list_to_lut
from sparse.frameworks.tune import tune_imports
# ------------------------------------------------------------------------------

class Tuner(Base):
	def __init__(self, name=None):
		super(Tuner, self).__init__(name=name)
		self._cls = 'Tuner'
		self._imports = {}
		self._config = {}
		self._config_path = None
		self.update_config()

	def _remove_non_configs(self, files):
		non_configs = ['.DS_Store']
		output = []
		for f in files:
			if f in non_configs:
				pass
			else:
				output.append(f)
		return output

	def update_config(self):
		self._config = {}
		reload(tune_imports)
		root = tune_imports.CONFIG_PATH
		self._config_path = root

		ls = self._remove_non_configs(os.listdir(root))
		for conf in ls:
			with open(os.path.join(root, conf)) as config:
				config = json.loads(config.read())
				for key, value in config.iteritems():
					if key in self._config.keys():
						if type(value) is dict and type(self._config[key]) is dict:
							value = dict(self._config[key].items() + value.items())
						else:
							warnings.warn('Non-unique primary keys detected: ' + value, Warning)
					self._config[key] = value

	def resolve_config(self):
		self._imports = {}
		from sparse.frameworks.tune import tune_imports
		IMPORTS = tune_imports.get_imports()
		self._imports = IMPORTS
		self._config = interpret_nested_dict(self._config,
						lambda x: IMPORTS[x] if x in IMPORTS.keys() else x)

	def tune(self, items, lut_index):
		conf = self._config
		input_lut = conf[lut_index]['input_lut']
		input_lut = conf['luts'][input_lut]
		output_lut = conf[lut_index]['output_lut']
		output_lut = conf['luts'][output_lut]
		return double_lut_transform(items, input_lut, output_lut)

	def create_lut(self, items, name, filename):
		temp = self._config['luts']['interchange_lut']
		ilut = OrderedDict()
		for key in sorted(temp.keys()):
			ilut[key] = temp[key]
		temp = list_to_lut(items, ilut)
		lut = OrderedDict()
		for key in sorted(temp.keys()):
			lut[key] = temp[key]

		with open(os.path.join(self._config_path, filename), 'w+') as lut_file:
			lut_file.write('{ "luts" : {\n')
			lut_file.write('\t"' + name + '" : {\n')
			lut = zip(lut.keys(), lut.values())
			for key, value in lut[:-1]:
				key = '"' + key + '"'
				value = '"' + value + '",'
				line = '\t\t\t{:<20}: {:<10}\n'.format(key, value)
				lut_file.write(line)
			key = '"' + lut[-1][0] + '"'
			value = '"' + lut[-1][1] + '"'
			line = '\t\t\t{:<20}: {:<10}\n'.format(key, value)
			lut_file.write(line)
			lut_file.write('\t\t}\n')
			lut_file.write('\t}\n')
			lut_file.write('}\n')
			
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
