import re
from collections import OrderedDict
import yaml
import numpy
import pandas
from pandas import DataFrame, Series
from stitch.core.utils import *
from stitch.core.stitch_frame import StitchFrame
from stitch.core.stitch_phrase import StitchPhrase, StitchWord
# ------------------------------------------------------------------------------

'''The stitch_string module contains the StitchString class.

The StitchString class is used for easily composing a grammar out of StitchPhrases
and StitchWords from a csv document.

Platform:
	Unix

Author:
	Alex Braun <alexander.g.braun@gmail.com> <http://www.AlexBraunVFX.com>
'''

class StitchString(Base):
	def __init__(self, source=None, data=None):
		self._data = data
		self._elements = {}
		self._master_phrase = None
		if data != None:
			self.generate_grammar

		if source:
			self.from_yaml(source)

	def from_yaml(self, source):
		lex = None
		with open(source, 'r') as f:
			lex = yaml.load(f)
		lut = lex['vars']

		data = []
		for word, val in lex['words'].iteritems():
			val['items'] = [ lut[x] for x in val['items'] ]
			val['capture'] = lut[val['capture']]
			val['descriptor'] = word
			val['type'] = 'word'
			data.append(val)

		for phrase, val in lex['phrases'].iteritems():
			val['descriptor'] = phrase
			val['type'] = 'phrase'
			data.append(val)

		for phrase, val in lex['master_phrase'].iteritems():
			val['descriptor'] = phrase
			val['type'] = 'master'
			data.append(val)

		data = DataFrame(data)

		data['determiners'] = None
		data['tokens'] = None
		data['terminators'] = None
		mask = data[data['type'] == 'word']
		data.loc[mask.index, 'determiners'] = mask['items'].apply(lambda x: x[0])
		data.loc[mask.index,      'tokens'] = mask['items'].apply(lambda x: x[1])
		data.loc[mask.index, 'terminators'] = mask['items'].apply(lambda x: x[2])
		data['elements'] = data['items']

		cols = [
			'type',
			'descriptor',
			'items',
			'flags',
			'capture',
			'restricted',
			'linking',
			'determiners',
			'tokens',
			'terminators',
			'elements'
		]
		data = data[cols]

		self._data = data
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
			self._elements[desc] = StitchWord(descriptor=desc, determiners=det, tokens=tok, terminators=term,
									 flags=flg, capture=cap, restricted=rest)

		def add_phrase(row):
			desc = row['descriptor']
			link = row['linking']
			elem = row['elements']
			elem = [self._elements[x] for x in elem]
			self._elements[desc] = StitchPhrase(desc, elem, linking=link)

		data = self._data
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

	def diagnose(self, string, as_dataframe=True):
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
		if as_dataframe:
			data = conform(diagnosis)
			data = interpret_nested_dict(data, lambda x: convert(x))
			sdf = StitchFrame()
			sdf.from_nested_dict(data)
			return sdf._data
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

__all__ = ['StitchString']

if __name__ == '__main__':
	main()
