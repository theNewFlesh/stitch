from __future__ import with_statement, print_function, absolute_import
from itertools import *
from functools import *
import re
from collections import OrderedDict
import numpy
import pandas
from pandas import DataFrame, Series
from stitch.core.utils import Base
# ------------------------------------------------------------------------------

'''The stitch_phrase module contains the StitchPhrase class.

The StitchPhrase class is used for parsing strings and generating regular
expressions according to the DTT (determiner, token, terminator) paradigm.

Platform:
	Unix

Author:
	Alex Braun <alexander.g.braun@gmail.com> <http://www.AlexBraunVFX.com>
'''

SEP = '\xff'

class StitchWord(Base):
	'''
	Class for representing a word within the Stitch grammatical paradigm

	A StitchWord functions as a token within a larger combinatorial grammar.
	They are defined by three components rather a single one.  The first is
	called the "determiner", which is a list of regular expressions used to
	demarcate the beginning of the word.  The second is called the "token",
	which is a list of regular expressions used to capture the substring you are
	actually interested in.	The last is called the "terminator", which is a
	list of regular expressions used to	demarcate the end of the word.  This
	three-part paradigm is called the DKT (Determiner, Token, Terminator)
	paradigm.

	With this paradigm, StitchWords can not only parse strings by internally
	defined	regular expressions, but they can diagnose what is wrong with them
	when they fail and repair them. Both diagnostics and repair function by
	means of component mutations made possible by the DKT paradigm.

	StitchWords are fit into StitchPhrases which utilize these components in
	order to chain words together into new phrase structures and perform
	mutations.
	'''
	def __init__(self, descriptor='token',
				 determiners=[''], tokens=['.*'], terminators=[''],
				 flags=0, capture=[0, 1, 0], restricted=True,
				 data=None):
		self._restricted = restricted

		def reduce(raw):
			raw = re.sub('(?<!\\\\)\{.*(?<!\\\\)\}', '', raw)
			raw = re.sub('(?<!\\\\)\(.*(?<!\\\\)\)', '', raw)
			raw = re.sub('(?<!\\\\)[.?+*^$\[\]]', '', raw)
			return raw

		self._data = data
		if not data:
			markers = determiners + terminators
			markers = [reduce(x) for x in markers]
			markers = DataFrame(markers, columns=['raw'])
			markers['length'] = markers['raw'].apply(lambda x: len(x))
			markers.sort_values('length', inplace=True)
			markers = markers['raw'].unique().tolist()
			markers_ = '[^' + ''.join(markers) + ']+'

			end = ['[^' + SEP + ']+?',
				   '[^' + SEP + ']*?',
				    '']
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
							 columns=cols)
			data['class'] = self.__class__.__name__
			data['phrase'] = descriptor
			data['word'] = descriptor
			data['flags'] = flags
			data['mutation'] = 0
			data['total_mutations'] = data['mutations'].apply(lambda x: len(x))
			data['restricted'] = self._restricted
			data['conflict'] = False
			data['regex'] = data['raw']

			cols = ['class', 'phrase', 'word', 'component', 'descriptor',
					'flags', 'mutation', 'mutations', 'total_mutations',
					'restricted', 'capture', 'conflict', 'raw', 'regex']
			data = data[cols]
			self._data = data
			self.mutate([0,0,0])
			self._backup = data.copy()

		self._descriptor = self._data[self._data['component'] == 'determiner']['phrase'].item()


	def reset(self):
		self._data = self._backup.copy()

	def mutate(self, mutation):
		def _mutate(component, integer):
			data = self._data
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
		return self._data

	def nullify(self):
		self.mutate([-1, -2, -1])
		self._data['capture'] = False

	@property
	def regex(self):
		data = self._data
		regex = data['regex'].tolist()
		regex = ''.join(regex)
		regex = re.compile(regex, flags=data.ix[1]['flags'])
		return regex

	@property
	def grok(self):
		# Logstash grok support
		word = self._descriptor.upper() + ' (' + self._data['raw'].ix[1] + ')'

		if self._data['capture'].ix[1]:
			word = '_' + word
			repl = '%{_' + self._descriptor.upper()
			repl += ':' + self._descriptor.lower()
			repl += '}'
			phrase = self.regex.pattern
			repl_re = re.compile('\(\?P<' + self._descriptor + '>.*?\)')
			phrase = repl_re.sub(repl, phrase)
			phrase = self._descriptor.upper() + ' (?:' + phrase + ')'
			return '\n'.join([word, phrase])
		else:
			return word

	def parse(self, string):
		found = self.regex.search(string)
		if found:
			return found.groupdict()
		else:
			return None

	def smart_parse(self, string):
		found = self.parse(string)
		if not found:
			diagnosis = self.diagnose(string)
			if diagnosis['error']:
				if diagnosis['fix']:
					self.repair(diagnosis['fix'])
		return self.parse(string)

	def repair(self, fix):
		self.mutate(fix[0]['mutation'])

	def diagnose(self, string):
		def test(mutation):
			self.mutate(mutation)
			found = self.parse(string)
			if found:
				return True
			return False

		def mutate_test(component, mutate):
			data = self._data
			data = data[data['component'] == component]
			total = data['total_mutations']
			if self._restricted:
				total -= 4
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
		o['class'] = 'StitchWord'

		found = self.parse(string)
		if found:
			o['error'] = False
			return o

		o['error'] = True
		o['fix'] = []

		mutation = [0, 0, 0]
		mutation[1] = mutate_test('token', [-1, 0, -1])
		t = mutation[1]
		if t != None:
			if not test([0, t, -1]):
				mutation[0] = mutate_test('determiner', [0, t, -1])
			if not test([-1, t, 0]):
				mutation[2] = mutate_test('terminator', [-1, t, 0])

		if None not in mutation:
			o['fix'].append({'element': self._descriptor,
							 'type': 'mutation',
							 'mutation': mutation})

		self.mutate([0, 0, 0])
		return o
# ------------------------------------------------------------------------------

class StitchPhrase(Base):
	'''
	Class for representing a phrase within the Stitch grammatical paradigm

	A StitchPhrase functions as a complete combinatorial (and mutative) grammar
	comprised of other, simpler StitchPhrases and (ultimately) StitchWords.
	Based on diagnoses performed after failed parse operatons; StitchPhrases are
	able to repair themselves such that they succeed upon reparsing the same
	string.

	Grammars are represented as tables within StitchPhrases.  From the "regex"
	column of this table a regular expression is derived and used to parse
	supllied strings.  When a parse fails, the phrase performs a diagnosis and
	repair process which reconfigures this table in such a way that the next
	regular expression derived from succeeds.
	'''
	def __init__(self, descriptor, elements, linking=True, data=None):
		elem = OrderedDict()
		for e in elements:
			elem[e._descriptor] = e
		self._elements = elem

		self._phrases = []
		self._words = []
		self._markers = []
		self._descriptor = descriptor
		self._linking = linking

		self._data = data
		if not data:
			self.construct_data()

	def construct_data(self):
		cols = ['class', 'phrase', 'word', 'component', 'descriptor',
				'flags', 'mutation', 'mutations', 'total_mutations',
				'restricted', 'capture', 'conflict', 'raw', 'regex']

		data = DataFrame(columns=cols)
		for element in self._elements.values():
			temp = element._data.copy()
			data = pandas.concat([data, temp])
		data.reset_index(drop=True, inplace=True)
		self._data = data
		self.determine_conflicts()
		self.markers

	def determine_conflicts(self):
		data = self._data
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
		return self._data

	# may be removed
	@property
	def markers(self):
		data = self._data
		markers = data[data['component'] != 'token']
		# markers = markers[markers['class'] == 'StitchWord']
		markers = markers[['raw']]
		markers['length'] = markers['raw'].apply(lambda x: len(x))
		markers.sort_values('length', ascending=False, inplace=True)
		markers = markers['raw'].unique().tolist()
		self._markers = markers
		return '|'.join(self._markers)

	# may be removed
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

	def nullify(self):
		self._data['capture'] = False
		self.mutate([-1, -1, -1])
		self.mutate([-2, -1, -1], index=[0])

	def mutate(self, mutation, mode='all', index=None):
		def _mutate(data, component, integer):
			mask = data[data['component'] == component]
			data = self._data

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

		data = self._data
		if index.__class__.__name__ != 'NoneType':
			data = data.ix[index]
		if mode == 'conflict':
			data = data[data['conflict']]
		elif mode == 'ends':
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
		return self._data

	@property
	def regex(self):
		def sub_with_wildcard(regex):
			wild_re = re.compile('\[\^' + SEP + ']')
			found = wild_re.search(regex)
			if found:
				return wild_re.sub('.', regex)
			return regex

		data = self._data
		if self._linking:
			# mask restricted
			rmask = data['restricted'].tolist()
			# unmask all tokens
			cmask = data['component'].apply(lambda x: x =='token').tolist()
			for i, item in enumerate(cmask):
				if item == True:
					rmask[i] = True
			data = data[rmask]
			mask = _mask_pairs(data['raw'].tolist())
			data = data[mask]

		# substitute non-separators with wildcard .
		regex = data['regex'].apply(lambda x: sub_with_wildcard(x))
		regex = regex.tolist()
		regex = ''.join(regex)
		return re.compile(regex)

	@property
	def grok(self):
		output = [x.grok for x in self._elements.values()]
		regex = self.regex.pattern
		grok_re = re.compile('\(\?P<(.*?)>.*?\)')
		found = grok_re.finditer(regex)
		for item in found:
			name = item.group(1)
			repl = '%{_' + name.upper() + ':' + name.lower() + '}'
			pattern = '\(\?P<' + name + '>.*?\)'
			regex = re.sub(pattern, repl, regex)

		temp = self._descriptor.upper() + ' (?:' + regex + ')'
		output.append(temp)
		return '\n'.join(output)

	def parse(self, string):
		found = self.regex.search(string)
		if found:
			return found.groupdict()
		else:
			return None

	def smart_parse(self, string):
		found = self.parse(string)
		if not found:
			diagnosis = self.diagnose(string)
			if diagnosis['error']:
				if diagnosis['fix']:
					self.repair(diagnosis['fix'])
		return self.parse(string)

	def reset(self):
		for element in self._elements.values():
			element.reset()
		self.construct_data()

	def repair(self, fix):
		if fix == []:
			raise IndexError('fix is empty')

		for instr in fix:
			if instr['type'] == 'linking':
				self._linking = instr['linking']
			elif instr['type'] == 'scaffold':
				self.mutate(instr['mutation'], mode=instr['mode'])
			elif instr['type'] == 'phrase_structure':
				new_order = OrderedDict()
				for item in instr['element_order']:
					new_order[item] = self._elements[item]
				self._elements = new_order
				self.construct_data()
			elif instr['type'] == 'element':
				element = self._elements[instr['element']]
				element.repair(instr['fix'])
				self.construct_data()
			else:
				pass

	def _scaffold_test(self, string):
		def mutation_test(string, mutation, mode='all'):
			self.mutate([0, 0, 0], mode='all')
			self.mutate(mutation, mode=mode)
			found = self.parse(string)
			if found:
				return True
			return False

		def test():
			o = OrderedDict()
			found = self.parse(string)
			if found:
				o['error'] = False
				return o

			o['error'] = True
			o['fix'] = []

			# test for incorrect linking state
			linking = self._linking
			self._linking = not linking
			found = self.parse(string)
			self._linking = linking
			if found:
				o['fix'].append({'element': self._descriptor,
								 'type': 'linking',
								 'linking': not linking})
				return o

			# test for broken scaffold only
			o['broken_scaffold'] = mutation_test(string, [-2, 0, -2], mode='all')
			if o['broken_scaffold']:
				o['conflicting_determiners'] = mutation_test(string, [-1, 0, 0], mode='both')
				if o['conflicting_determiners']:
					o['fix'].append({'element': self._descriptor,
									 'type': 'scaffold',
									 'mutation': [-1, 0, 0],
									 'mode': 'both'})

					if mutation_test(string, [-1, 0, 0], mode='conflict'):
						o['fix'][-1]['mode'] = 'conflict'
						return o
					if mutation_test(string, [-1, 0, 0], mode='ends'):
						o['fix'][-1]['mode'] = 'ends'
						return o
					return o

				o['conflicting_terminators'] = mutation_test(string, [0, 0, -1], mode='both')
				if o['conflicting_terminators']:
					o['fix'].append({'element': self._descriptor,
									 'type': 'scaffold',
									 'mutation': [0, 0, -1],
									 'mode': 'both'})
					if mutation_test(string, [0, 0, -1], mode='conflict'):
						o['fix'][-1]['mode'] = 'conflict'
						return o
					if mutation_test(string, [0, 0, -1], mode='ends'):
						o['fix'][-1]['mode'] = 'ends'
						return o
				return o
			# no fix
			return o

		output = test()
		self.reset()
		return output

	def _element_test(self, string):
		def test():
			desc = self._descriptor
			o = OrderedDict()
			o['fix'] = []
			o['error'] = False
			o['unfound_elements'] = []
			help_str = string
			for name, element in self._elements.iteritems():
				diagnosis = element.diagnose(string)
				if diagnosis['error']:
					if diagnosis['fix']:
						o['fix'].append({'element': name,
										'type': 'element',
										'fix': diagnosis['fix']})
						element.repair(diagnosis['fix'])
					else:
						o['unfound_elements'].append(name)
						continue
					o['error'] = True

				# replace element result with marker
				tail = element._data.tail(1)['regex']
				t = element._data.tail(1)
				element._data.loc[t.index, 'regex'] = ''
				padded_re = SEP + element.regex.pattern
				padded_re = re.compile(padded_re)
				found = padded_re.search(help_str)
				if found:
					help_str = padded_re.sub(SEP + name + SEP, help_str)
				else:
					help_str = element.regex.sub(SEP + name + SEP, help_str)
				e = element._data.tail(1)
				element._data.loc[e.index, 'regex'] = tail

			# test phrase with mutated elements in original order
			self.construct_data()
			p = self._scaffold_test(string)
			if not p['error']:
				return o
			if p['fix']:
				o['error'] = True
				for key, val in p.iteritems():
					if key not in o:
						if key != 'fix':
							o[key] = val
						else:
							o['fix'].extend(val)
				return o

			for key in o['unfound_elements']:
				element = self._elements[key]
				element.nullify()
			unfound = o['unfound_elements']

			# determine new element order from helper string
			temp = help_str.split(SEP)
			temp = [x for x in temp if x != '']
			new_elements = OrderedDict()
			for item in temp:
				if item in self._elements:
					new_elements[item] = self._elements[item]
				else:
					if unfound:
						null = unfound.pop(0)
						new_elements[null] = self._elements[null]

			if new_elements == self._elements:
				o['broken_phrase_structure'] = False
			else:
				o['broken_phrase_structure'] = True
				o['element_order'] = new_elements.keys()
				o['fix'].append({'element': self._descriptor,
								 'type': 'phrase_structure',
								 'element_order': new_elements.keys()})
			return o

		elements = self._elements
		output = test()
		self._elements = elements
		self.reset()
		return output

	def diagnose(self, string):
		o = self._scaffold_test(string)
		if o['error']:
			if o['fix']:
				self.repair(o['fix'])
		else:
			return o

		o = self._element_test(string)
		if ['error']:
			if o['fix']:
				self.repair(o['fix'])
			else:
				self.reset()
				o = OrderedDict({'fix': [], 'error': o['error']})
		else:
			return o

		p = self._scaffold_test(string)
		if p['error']:
			o['error'] = True
			for key, val in p.iteritems():
				if key not in o:
					if key != 'fix':
						o[key] = val
					else:
						o['fix'].extend(val)
		self.reset()
		return o
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

__all__ = ['StitchWord', 'StitchPhrase']

if __name__ == '__main__':
	main()
