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

'''The sparse_string module contains the SparseString class.

The SparseString class is used for easily composing a grammar out of SparsePhrases
and SparseWords from a csv document.

Date:
	01.18.2015

Platform:
	Unix

Author:
	Alex Braun <ABraunCCS@gmail.com> <http://www.AlexBraunVFX.com>
'''
# ------------------------------------------------------------------------------

import re
from collections import OrderedDict
import numpy
import pandas
from pandas import DataFrame, Series
from sparse.utilities.utils import Base
from sparse.core.sparse_dataframe import SparseDataFrame
from sparse.core.sparse_phrase import SparsePhrase, SparseWord
from sparse.utilities.utils import flatten_nested_dict, combine, insert_level
# ------------------------------------------------------------------------------

class SparseString(Base):
	def __init__(self, data=None, name=None):
		super(SparseString, self).__init__(name=name)
		self._cls = 'SparseString'
		
		self.data = data
		self._charset = {}
		self._elements = {}
		self._master_phrase = None
		if data != None:
			self.generate_grammar
	
	def _process_data(self, data):
		data.fillna('', inplace=True)
		
		# remove whitespace characters
		data['type'] = data['type'].apply(lambda x: x.strip(' '))
		data['descriptor'] = data['descriptor'].apply(lambda x: x.strip(' '))
		data['capture'] = data['capture'].apply(lambda x: x.strip(' '))
		
		# determine capture patterns
		mask = data['type'] == 'capture'
		data['items'][mask] = data['items'][mask].apply(lambda x: x.split(','))
		data['items'][mask] = data['items'][mask].apply(lambda x: [int(y) for y in x])
		caps = {}
		for i, row in data[mask].iterrows():
			caps[row['descriptor']] = row['items']
		self._captures = caps
		mask = data['capture'].apply(lambda x: x in caps)
		data['capture'][mask] = data['capture'][mask].apply(lambda x: caps[x])

		# determine characater set
		mask = data['type'] == 'charset'
		data['items'][mask] = data['items'][mask].apply(lambda x: eval('[' + x + ']'))
		charset = {}
		for i, row in data[mask].iterrows():
			charset[row['descriptor']] = row['items']
		self._charset = charset

		# transfer word items to respective component columns and swap with charset equivalents
		mask = data['type'].apply(lambda x: x in ['word', 'phrase', 'compound_phrase', 'master'])
		data['items'][mask] = data['items'][mask].apply(lambda x: re.split(' *', x))
		data['determiners'] = None
		data['tokens'] = None
		data['terminators'] = None
		
		mask = data['type'] == 'word'
		data['determiners'][mask] = data['items'][mask].apply(lambda x: charset[x[0]])
		data['tokens'][mask]      = data['items'][mask].apply(lambda x: charset[x[1]])
		data['terminators'][mask] = data['items'][mask].apply(lambda x: charset[x[2]])
		
		data['flags']    = data['flags'].apply(lambda x: 0 if x == '' else int(x))
		data['elements'] = data['items']
		mask = data['type'].apply(lambda x: x in ['charset', 'capture'])
		data['elements'][mask] = None
		
		return data
	
	def read_csv(self, csv):
		data = pandas.read_csv(csv, sep='\t', quotechar="'")
		self.data = self._process_data(data)
		self.generate_grammar()
	
	def generate_grammar(self):
		def add_word(row):
			desc = row['descriptor']
			det = row['determiners']
			tok = row['tokens']
			term = row['terminators']
			flg = row['flags']
			cap = row['capture']
			rest = row['restricted']
			self._elements[desc] = SparseWord(descriptor=desc, determiners=det, tokens=tok, terminators=term,
									 flags=flg, capture=cap, restricted=rest)
		
		def add_phrase(row):
			desc = row['descriptor']
			link = row['linking']
			elem = row['elements']
			elem = [self._elements[x] for x in elem]
			self._elements[desc] = SparsePhrase(desc, elem, linking=link)
		
		data = self.data
		word_data = data[data['type'] == 'word']
		for i, row in word_data.iterrows():
			add_word(row)
		
		phrase_data = data[data['type'] == 'phrase']
		for i, row in phrase_data.iterrows():
			add_phrase(row)
		
		c_phrase_data = data[data['type'] == 'compound_phrase']
		for i, row in c_phrase_data.iterrows():
			add_phrase(row)
			
		master_data = data[data['type'] == 'master']
		for i, row in master_data.iterrows():
			add_phrase(row)
			self._master_phrase = self._elements[row['descriptor']]
	
	def smart_parse(self, string):
		self._master_phrase.smart_parse(string)
	
	def parse(self, string):
		self._master_phrase.parse(string)
		   
	def diagnose(self, string, output='DataFrame'):
		def conform(dict_):
			for k, v in dict_.iteritems():
				if k in ['mutation', 'element_order']:
					dict_[k] = str(v)
			return dict_
	
		def convert(items):
			if type(items) == list:
				output = {}
				for i, item in enumerate(items):
					if type(item) == dict:
						temp = conform(item)
						output[i] = interpret_nested_dict(temp, lambda x: convert(x))
					else:
						output[i] = item
				return output
			return items
		
		diagnosis = self._master_phrase.diagnose(string)
		if output == 'DataFrame':
			data = conform(diagnosis)
			data = interpret_nested_dict(data, lambda x: convert(x))
			sdf = SparseDataFrame()
			sdf.read_nested_dict(data, inplace=True)
			return sdf.data
		else:
			return diagnosis
	
	@property
	def regex(self):
		return self._master_phrase.regex
	
	@property
	def grok(self):
		return self._master_phrase.grok
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['SparseString']

if __name__ == '__main__':
	main()