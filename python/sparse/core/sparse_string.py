#! /usr/bin/env python
# Alex Braun 04.23.2014

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

'''The sparse_string module contains the SparsePhrase class.

The SparsePhrase class is used for parsing strings and generating regular
expressions according to the DTT (determiner, token, terminator) paradigm. 

Date:
	09.06.2014

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
from sparse.utilities.utils import flatten_nested_dict, combine, insert_level
# ------------------------------------------------------------------------------

class SparseWord(Base):
	def __init__(self, descriptor='token', 
				 determiners=[''], tokens=['.*'], terminators=[''],
				 flags=0, capture=[0, 1, 0], data=None, name=None):
		super(SparseWord, self).__init__(name=name)
		self._cls = 'SparseWord'
		
		def reduce(raw):
			raw = re.sub('(?<!\\\\)\{.*(?<!\\\\)\}', '', raw)
			raw = re.sub('(?<!\\\\)\(.*(?<!\\\\)\)', '', raw)
			raw = re.sub('(?<!\\\\)[.?+*^$\[\]]', '', raw)
			return raw

		self.data = data
		if not data:
			markers = determiners + terminators
			markers = [reduce(x) for x in markers]
			markers = DataFrame(markers, columns=['raw'])
			markers['length'] = markers['raw'].apply(lambda x: len(x))
			markers.sort(columns=['length'], inplace=True)
			markers = markers['raw'].unique().tolist()
			markers_ = '[^' + ''.join(markers) + ']+'

			end = ['.+?', '.*?', '']
			determiners.append('^')
			determiners.extend(end)
			tokens.append(markers_)
			tokens.extend(end)
			terminators.append('$')
			terminators.extend(end)


			capture = [bool(x) for x in capture]
			d_desc = descriptor + '_determiner'
			t_desc = descriptor + '_terminator'
			cols =            [ 'component',  'descriptor', 'raw',           'mutations', 'capture'  ]
			data = DataFrame([[ 'determiner',  d_desc,       determiners[0],  determiners, capture[0]],
							  [ 'token',       descriptor,   tokens[0],       tokens,      capture[1]],
							  [ 'terminator',  t_desc,       terminators[0],  terminators, capture[2]]],
							 columns=cols, index=[[descriptor] * 3, [0, 1, 2]])

			data['class'] = self._cls
			data['phrase'] = descriptor
			data['word'] = descriptor
			data['flags'] = flags
			data['mutation'] = 0
			data['total_mutations'] = data['mutations'].apply(lambda x: len(x))
			# data['markers'] = None
			# data['markers'] = data['markers'].apply(lambda x: markers)
			data['conflict'] = False	
			data['regex'] = data['raw']

			cols = ['class', 'phrase', 'word', 'component', 'descriptor', 
					'flags', 'mutation', 'mutations', 'total_mutations',
					'capture', 'conflict', 'raw', 'regex']
			data = data[cols]
			self.data = data
			self.mutate([0,0,0])
	
		self._descriptor = self.data[self.data['component'] == 'determiner']['phrase'].item()


	def mutate(self, mutation):
		def _mutate(component, integer):
			data = self.data
			mask = data[data['component'] == component]
			mutation = mask['mutations'].item()[integer]
			data.loc[mask.index, 'raw'] = mutation
			if mask['capture'].item():
				mutation = '(?P<' + mask['descriptor'].item() + '>' + mutation + ')'
			data.loc[mask.index, 'regex'] = mutation
			data.loc[mask.index, 'mutation'] = integer
		
		mutation = _mutation_handler(mutation)
		for key, val in mutation.iteritems():
			_mutate(key, val)
		return self.data

	@property
	def regex(self):
		data = self.data	
		regex = data['regex'].tolist()
		regex = ''.join(regex)
		regex = re.compile(regex, flags=data.ix[1]['flags'])
		return regex

	def parse(self, string):
		found = self.regex.search(string)
		if found:
			return found.groupdict()
		else:
			return None

	def diagnose(self, string):
		def test(mutation):
			self.mutate(mutation)
			found = self.parse(string)
			if found:
				return True
			return False

		def mutate_test(component, mutate):		
			data = self.data
			data = data[data['component'] == component]
			total = data['total_mutations']  - 4
			for i in range(0, total):
				found_token = False
				if component == 'determiner':
					found_token = test([i, mutate[1], mutate[2]])
				elif component == 'token':
					found_token = test([mutate[0], i, mutate[2]])
				else:	
					found_token = test([mutate[0], mutate[1], i])
				if found_token:
					return i
			return None

		o = OrderedDict()
		o['descriptor'] = self._descriptor
		o['class'] = 'SparseWord'

		found = self.parse(string)
		if found:
			o['error'] = False
			return o

		o['error'] = True
		o['token_mutation'] = False
		o['suggested_token_mutation'] = 0
		o['determiner_mutation'] = False
		o['suggested_determiner_mutation'] = 0
		o['terminator_mutation'] = False
		o['suggested_terminator_mutation'] = 0
		o['suggested_mutation'] = [0, 0, 0]

		o['token_mutation'] = not test([-1, 0, -1])
		if o['token_mutation']:
			o['suggested_token_mutation'] = mutate_test('token', [-1, 0, -1])

		if o['suggested_token_mutation'] != None:
			t = o['suggested_token_mutation']

			o['determiner_mutation'] = not test([0, t, -1])
			if o['determiner_mutation']:
				o['suggested_determiner_mutation'] = mutate_test('determiner', [0, t, -1])
			
			o['terminator_mutation'] = not test([-1, t, 0])
			if o['terminator_mutation']:
				o['suggested_terminator_mutation'] = mutate_test('terminator', [-1, t, 0])

		sdm = o['suggested_determiner_mutation']
		skm = o['suggested_token_mutation']
		stm = o['suggested_terminator_mutation']
		sm = [sdm, skm, stm]
		o['perform_mutation'] = False
		if None not in sm:
			o['perform_mutation'] = True
			o['suggested_mutation'] = sm

		self.mutate([0, 0, 0])
		return o
# ------------------------------------------------------------------------------
		
class SparsePhrase(Base):
	def __init__(self, descriptor, elements, linking=True, data=None, name=None):
		super(SparsePhrase, self).__init__(name=name)
		self._cls = 'SparsePhrase'

		self._elements = elements
		self._phrases = []
		self._words = []
		self._markers = []
		self._descriptor = descriptor
		self._linking = linking

		self.data = data
		if not data:
			self.construct_data(elements)

	def construct_data(self, elements):
		# cols = ['class', 'phrase', 'word', 'component', 'descriptor', 
		# 		'flags', 'mutation', 'mutations', 'total_mutations',
		# 		'capture', 'conflict', 'raw', 'regex']
		# data = DataFrame(columns=cols)
		# for element in elements:
		# 	temp = element.data.copy()
		# 	if element._cls == 'SparsePhrase':
		# 		self._phrases.append(element)
		# 		temp['class']  = 'SparsePhrase'
		# 		temp['phrase'] = element._descriptor
		# 	else:
		# 		self._words.append(element)
		# 	data = pandas.concat([data, temp])
	
		# data.reset_index(drop=True, inplace=True)
		data = combine([x.data for x in elements])
		data.index = insert_level(data.index, self._descriptor)
		self.data = data
		self.determine_conflicts()
		self.markers

	def determine_conflicts(self):
		data = self.data
		items = data[data['component'] != 'token']
		items = items['regex'].tolist()
		items = items[1:-1]
	
		temp = []
		new_item = []
		for item in items:
			new_item.append(item)
			if len(new_item) == 2:
				temp.append(new_item)
				new_item = []

		conflicts = [False, False]
		for item in temp:
			if item[0] == item[1]:
				conflicts.append(False)
				conflicts.append(False)
				conflicts.append(False)
			else:
				conflicts.append(True)
				conflicts.append(True)
				conflicts.append(False)
		conflicts.append(False)
						
		data['conflict'] = conflicts
		return self.data

	@property
	def markers(self):
		data = self.data
		markers = data[data['component'] != 'token']
		# markers = markers[markers['class'] == 'SparseWord']
		markers = markers[['raw']]
		markers['length'] = markers['raw'].apply(lambda x: len(x))
		markers.sort(columns=['length'], ascending=False, inplace=True)
		markers = markers['raw'].unique().tolist()
		self._markers = markers
		return '|'.join(self._markers)

	def _get_substrings(self, string):
		items = re.split(self.markers, string)
		items = [x for x in items if x != '']

		slots = string
		for item in items:
			slots = re.sub(item, '<item>', slots)
		markers = slots.split('<item>')
		markers = [x for x in markers if x != '']
		
		slots = re.sub(self.markers, '<marker>', slots)
		slots = re.sub('<', '', slots)
		slots = slots.split('>')
		slots = [x for x in slots if x != '']

		items.reverse()
		markers.reverse()

		data = []
		for i, slot in enumerate(slots):
			if slot == 'marker':
				if markers:
					data.append(['marker', markers.pop()])
			else:
				if items:
					data.append(['item', items.pop()])
		data = DataFrame(data, columns=['type', 'raw'])
		
		substrings = []
		substring = data['raw'].ix[0]
		for i, row in data.ix[1:].iterrows():
			if row['type'] == 'marker':
				substring += row['raw']
				substrings.append(substring)
				substring = row['raw']
			else:
				substring += row['raw']
		substrings.append(substring)
		return substrings

	def mutate(self, mutation, mode='all', index=None):
		def _mutate(data, component, integer):
			mask = data[data['component'] == component]
			data = self.data
		
			data.loc[mask.index, 'mutation'] = integer

			mutations = mask['mutations'].apply(lambda x: x[integer])
			data.loc[mask.index, 'raw'] = mutations
			data.loc[mask.index, 'regex'] = mutations
			
			mask = mask[mask['capture'] == True]
			data.loc[mask.index, 'regex'] = '(?P<'
			data.loc[mask.index, 'regex'] += data.loc[mask.index, 'descriptor']
			data.loc[mask.index, 'regex'] += '>'
			data.loc[mask.index, 'regex'] += data.loc[mask.index, 'raw']
			data.loc[mask.index, 'regex'] += ')'

		mutation = _mutation_handler(mutation)
			
		data = self.data
		if index.__class__.__name__ != 'NoneType':
			data = data.ix[index]
		if mode == 'internal':
			data = data[data['conflict']]
		elif mode == 'external':
			head = data.head(1).index.item()
			tail = data.tail(1).index.item() 
			data = data.ix[[head, tail]]
		elif mode == 'both':
			both = data['conflict'].tolist()
			both[0] = True
			both[-1] = True
			data = data[both]
		elif mode == 'all':
			pass
		else:
			raise TypeError('Improper mode specified: ' + mode)

		if len(data) > 0:
			for key, val in mutation.iteritems():
				_mutate(data, key, val)
		return self.data

	@property
	def regex(self):
		data = self.data
		if self._linking:
			mask = _mask_pairs(data['raw'].tolist())
			data = data[mask]
		regex = data['regex'].tolist()
		regex = ''.join(regex)
		return re.compile(regex)
	
	def parse(self, string):
		found = self.regex.search(string)
		if found:
			return found.groupdict()
		else:
			return None

	def diagnose(self, string):
		def _test(mutation, string, index):
			self.mutate(mutation, index=index)
			found = self.parse(string)
			if found:
				return True
			return False

		def test(string, element):
			# o = OrderedDict()   
			# found = self.parse(string)
			# if found:
			# 	o['error'] = False
			# 	return o
			
			# o['error'] = True
			# o['perform_mutation'] = False
			# o['broken_phrase_structure'] = False

			data = self.data
			self.construct_data(self._elements)        
			found = self.parse(string)
			if found:
				return o
			
			linking = self._linking
			self._linking = False
			found = self.parse(string)
			if found:
				o['linking'] = False
				return o
			self._linking = linking
					
			index = data.ix[self._descriptor]
			index = index.ix[element._descriptor]
			det = index[index['component'] == 'determiner'].index
			o['drop_determiners'] = _test([-1, 0, 0], string, det)
			if o['drop_determiners']:
				o['suggested_mutation'] = [-1, 0, 0]
				o['suggested_index'] = det
				o['perform_mutation'] = True
				return o
			
			self.construct_data(self._elements)
			term = index[index['component'] == 'terminator'].index
			o['drop_terminators'] = _test([0, 0, -1], string, det)
			if o['drop_terminators']:
				o['suggested_mutation'] = [0, 0, -1]
				o['suggested_index'] = term
				o['perform_mutation'] = True
				return o
			
			o['broken_phrase_structure'] = True
			return o
		
		o = OrderedDict()   
		found = self.parse(string)
		if found:
			o['error'] = False
			return o
		
		o['error'] = True
		o['perform_mutation'] = False
		o['broken_phrase_structure'] = False
		
		data = self.data
		bad_elements = []
		for element in self._elements:
			diagnosis = element.diagnose(string)
			o[element._descriptor] = diagnosis
			if diagnosis['error']:
				if diagnosis['perform_mutation']:
					if diagnosis['class'] == 'SparsePhrase':
						element.mutate(diagnosis['suggested_mutation'], index=diagnosis['suggested_index'])
					else:
						element.mutate(diagnosis['suggested_mutation'])
				else:
					bad_elements.append(element)
		
		for phrase in self._phrases:
			o[phrase._descriptor] = test(string, phrase)

		return o
		

	# def diagnose(self, string):
	# 	def test(element, mutation, mode=None):
	# 		if mode:
	# 			element.mutate(mutation, mode=mode)
	# 		else:
	# 			element.mutate(mutation)
	# 		found = element.parse(string)
	# 		if found:
	# 			return True
	# 		return False

	# 	o = OrderedDict()

	# 	found = self.parse(string)
	# 	if found:
	# 		o['error'] = False
	# 		return o

	# 	o['error'] = True
	# 	o['descriptor'] = self._descriptor
	# 	o['class'] = 'SparsePhrase'

	# 	for element in self._elements:
	# 		diagnosis = element.diagnose(string)
	# 		if diagnosis['error']:
	# 			if diagnosis['class'] == 'SparseWord':
	# 				if diagnosis['perform_mutation']:
	# 					mask = data['word'] == diagnosis['descriptor']
	# 					self.mutate(diagnosis['suggested_mutation'], mask=mask)
	# 				else:
	# 					print 'broken_phrase_structure'
	# 			else:
	# 				pass

	# 	linking = self._linking
	# 	for state in [False, True]:
	# 		self._linking = state
	# 		found = self.parse(string)
	# 		if found:
	# 			print state
	# 	self._linking = linking

	# 	for drop in ['determiner', 'terminator']:
	# 		data = self.data
	# 		data = data[data['component'] != drop]
	# 		regex = data['regex'].tolist()
	# 		regex = ''.join(regex)
	# 		found = re.search(regex, string)
	# 		if found:
	# 			print drop


		# 	found = element.parse(string)
		# 	if not found:
		# 		bad_elements.append(element)
		# if not bad_elements:
		# 	o['broken_phrase_structure'] = True
		# 	return o
		
		# for element in bad_elements:
		# 	if element._cls == 'SparseWord': 
		# 		diagnosis = element.diagnose(string)
		# 		o[element._descriptor] = diagnosis
		# 		if diagnosis['error']:
		# 			if diagnosis['perform_mutation']:
		# 				element.mutate(diagnosis['suggested_mutation'])
		# 	if element._cls == 'SparsePhrase':
		# 		diagnosis = element.diagnose(string)
		# 		o[element._descriptor] = diagnosis
		# return o



		# o['word_mutation'] = test([0, -3, 0])
		# if o['word_mutation']:
		# 	o['missing_word'] = not test([0, -2, 0])

		# o['scaffold_mutation'] = test([-3, 0, -3])
		# if o['scaffold_mutation']:
		# 	o['determiner_mutation'] = not test([0, 0, -3])
		# 	o['terminator_mutation'] = not test([-3, 0, 0])	
			
		# 	o['missing_scaffold_element'] = not test([-2, 0, -2])
		# 	if o['missing_scaffold_element']:
		# 		if not o['determiner_mutation']:
		# 			o['missing_terminator'] = not test([0, 0, -2])
		# 		if not o['terminator_mutation']:
		# 			o['missing_determiner'] = not test([-2, 0, 0])

		# o['broken_phrase_structure'] = False
		# if o['scaffold_mutation'] and o['word_mutation']:
		# 	o['broken_phrase_structure'] = True

		# self.mutate([0,0,0], mode='all')

		# def compound_parse(string):
		# 	output = OrderedDict()
		# 	substrings = self._get_substrings(string)
		# 	for sub in substrings:
		# 		output[sub] = []
		# 		for phrase in self._phrases:
		# 			found = phrase.parse(sub)
		# 			if found:
		# 				output[sub].append(phrase)
		# 		for word in self._words:
		# 			found = word.parse(sub)
		# 			if found:
		# 				output[sub].append(word)
		# 	return output
		# o['compound_parse'] = compound_parse(string)

		# return o

	# def _compound_parse(substrings, elements, parse_type='simple'):
	# 	new_elements = []
	# 	for sub in substrings:
	# 		for element in elements:
	# 			found = None
	# 			if parse_type = 'smart':
	# 				found = element.smart_parse(sub)
	# 			else:
	# 				found = element.parse(sub) 
	# 			if found:
	# 				new_elements.append(element)
	# 				elements.remove(element)
	# 				substrings.remove(sub)
	# 				break
	# 	return 

	def smart_parse(self, string):
		found = self.parse(string)
		if found:
			return found
		else:
			diagnosis = self.diagnose(string)
			if diagnosis == 'broken_phrase_structure':
				pass
			elif diagnosis == 'scaffold_mutation':
				pass

			substrings = self._get_substrings(string)
			new_elements = []

			phrases = self._phrases
			for sub in substrings:
				for phrase in phrases:
					found = phrase.parse(sub)
					if found:
						new_elements.append(phrase)
						phrases.remove(phrase)
						substrings.remove(sub)
						break
			
			words = self._words
			for sub in substrings:
				for word in words:
					found = word.parse(sub)
					if found:
						new_elements.append(word)
						words.remove(word)
						substrings.remove(sub)
						break
			
			if len(substrings) > 0:
				for sub in substrings:
					for phrase in phrases:
						phrase.smart_parse(sub)
					for phrase in phrases:
						phrase.smart_parse(sub)
					

	def compound_parse(self, string, mutate=' |0| '):
		mutations = [' |0| ', 'd|0| ', ' |0|t']#, 'd| |t']
		found = None
		for mutate in mutations:
			found = self.parse(string, mutate=mutate)
			if found:
				print 'parse', mutate, string, found
				return found
			else:
				return self._compound_parse(string)

	def _compound_parse(self, string):
		output = {}
		substrings = self._get_substrings(string)
		
		mutations = [' |0| ', 'd|0| ', ' |0|t']#, 'd| |t']
		for mutate in mutations:
			new_substrings = substrings
			elements = self._elements
			 
			for s, sub in enumerate(substrings):
				for e, elem in enumerate(elements):
					found = elem.parse(sub, mutate=mutate)
					if found:
						print 'parse', mutate, sub, elem._descriptor, found
						for k, v in found.iteritems():
							output[k] = v
						new_substrings.pop(s)
						elements.pop(e)
						break

			for sub in new_substrings:
				for element in new_elements:
					found = element.compound_parse(sub, mutate=mutate)
					if found:
						print 'compound parse', mutate, sub, element._descriptor, found
						for k, v in found.iteritems():
							output[k] = v
						break
		return output

		# for sub in new_substrings:
		# 	for element in self._phrase_elements:
		# 		found = element.compound_parse(sub)
		# 		if found:
		# 			print 'compound parse', sub, element._descriptor, found
		# 			for k, v in found.iteritems():
		# 				output[k] = v
		# 				break
		
		# data['class'] = data['raw'].apply(lambda x: 'SparsePhrase' if re.search(self.submarkers, x) else 'SparseWord')
		# mask = data[data['type'] == 'item']
		# data.loc[mask.index, 'class'] == 'SparseWord'
	
		# substring = data[data['class'] == 'SparsePhrase']
		# substring = substring['raw'].tolist()
		# substring = ''.join(substring)

		# tokstring = data[data['class'] == 'SparseWord']
		# tokstring = tokstring['raw'].tolist()
		# tokstring = ''.join(tokstring)

		# output = {}
		# found = None
		# for token in self._token_elements:
		# 	found = token.parse(tokstring, drop=drop)
		# 	if found:
		# 		for k, v in found.iteritems():
		# 			output[k] = v

		# for item in self._phrase_elements:
		# 	found = item.parse(substring, drop=drop)
		# 	if found:
		# 		for k, v in found.iteritems():
		# 			output[k] = v
		# 	found = item._compound_parse(substring, drop=drop, recursive=recursive)
		# 	output[item._descriptor] = found

		# return output

	# def compound_parse(self, string, drop='d|k|t', recursive=True):
	# 	item = self._compound_parse(string, drop=drop, recursive=recursive)
	# 	data = SparseDataFrame()
	# 	data = data.read_nested_dict(item)
	# 	data.columns = ['result']
		
	# 	mask = _mask_to_list(mask)
	# 	data['determiner'] = mask[0]
	# 	data['token']      = mask[1]
	# 	data['terminator'] = mask[2]
		
	# 	data['recursive'] = recursive
	# 	data['type'] = 'compound'
		
	# 	descriptors = []
	# 	for row in data.index.tolist():
	# 		desc = row
	# 		if recursive:
	# 			new_row = [x for x in row if x != '-->']
	# 			desc = new_row[-1]
	# 		descriptors.append(desc)
	# 	data['descriptor'] = descriptors

	# 	data['length'] = data['result'].apply(lambda x: len(x) if type(x) == str else 0)
	# 	return data
		
	def master_parse(self, string):
		recurse = [True, False]
		drops = ['d|k|t', ' |k|t', 'd|k| ', ' |k| ']
				# 'd| |t', ' | |t', 'd| | ', ' | | ']

		data = {}
		for drop in drops:
			key = ', '.join([d, 'False', 'simple'])
			data[key] = self.parse(string, drop=drop) 

		for r in recurse:
			for drop in drops:
				key = ', '.join([d, str(r), 'compound'])
				data[key] = self._compound_parse(string, drop=drop, recursive=r)
		
		output = SparseDataFrame()
		output = output.read_nested_dict(data)

		output.columns = ['result']
		output = output[output['result'] != {}]

		index = output.index.get_level_values(0)
		index = [x.split(', ') for x in index]

		mask = [_mask_to_list(x[0]) for x in index]
		output['determiner'] = [x[0] for x in mask]
		output['token']    = [x[1] for x in mask]
		output['terminator'] = [x[2] for x in mask]

		output['recursive']  = [x[1] for x in index]
		output['type']       = [x[2] for x in index]
		output['recursive']  = output['recursive'].apply(lambda x: eval(x))

		descriptors = []
		for row in output.index.tolist():
			new_row = [x for x in row if x != '-->']
			descriptors.append(new_row[-1])
		output['descriptor'] = descriptors

		output['length'] = output['result'].apply(lambda x: len(x) if type(x) == str else 0)
		
		# output.reset_index(level=0, drop=True, inplace=True)

		return output
# ------------------------------------------------------------------------------

def _mutation_handler(mutation):
	output = {}
	if mutation[0] not in ['', None]:
		output['determiner'] = mutation[0]
	if mutation[1] not in ['', None]:
		output['token'] = mutation[1]
	if mutation[2] not in ['', None]:
		output['terminator'] = mutation[2]
	return output

def _mask_pairs(items):
	prev = items[0]
	output = [True]
	for item in items[1:]:
		if item != prev:
			output.append(True)
		else:
			output.append(False)
		prev = item
	return output

def _mask_to_list(mask):
	output = []
	mask_re = re.compile('\S')
	for item in mask.split('|'):
		found = mask_re.search(item)
		if found:
			output.append(True)
		else:
			output.append(False)
	return output
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['SparseWord', 'SparsePhrase']

if __name__ == '__main__':
	main()
