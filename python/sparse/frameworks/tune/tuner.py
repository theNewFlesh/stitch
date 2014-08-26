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

import os
import json
from sparse.utilities.utils import Base, interpret_nested_dict
from sparse.frameworks.tune import imports
# ------------------------------------------------------------------------------

class Tuner(Base):
	def __init__(self, name=None):
		super(Tuner, self).__init__(name=name)
		self._cls = 'Tuner'
		self._imports = {}
		self._config = {}
		self.update()

	def update(self):
		self._imports = {}
		self._config = {}
		reload(imports)

		self._imports = imports.IMPORTS
		for conf in os.listdir(imports.CONFIG):
			with open(os.path.join(imports.CONFIG, conf)) as config:
				config = json.loads(config.read())
				imp = self._imports
				config = interpret_nested_dict(config, lambda x: imp[x] if x 
											   in imp.keys() else x)
				for key, value in confg.iteritems():
					self._config[key] = value

	@property
	def config(self):
		return self._config
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
