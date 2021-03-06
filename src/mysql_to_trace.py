#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import with_statement
import sys
import re
import MySQLdb
import getopt
import gzip
import cPickle as pickle
from array import array
from itertools import imap

_verbose = 0
_DEFAULT_COMPRESS = 3

class GzipFile(gzip.GzipFile):
	def __enter__(self):
		if self.fileobj is None:
			raise ValueError("I/O operation on closed GzipFile object")
		return self
	def __exit__(self, *args):
		self.close()

class Field(object):
	"""Represents a database table field."""
	def __init__(self, name, ftype, rtype=None, table=None, is_pkey=False, nullable=True):
		self.table = table
		self.name = name
		if table:
			self._fullname = table + '.' + name
			self._fullname_escaped = self._fullname.replace(' ', '_')
			self._fullname_quoted  = '`' + table + '`.`' + name + '`'
		else:
			self._fullname = name
			self._fullname_escaped = name.replace(' ', '_')
			self._fullname_quoted  = '`' + name + '`'
		self.ftype = ftype
		self.nullable = nullable
		self.is_pkey = is_pkey
		self.rtype, self.to_val, self.cmp = ftype_to_rep_val_comp(ftype)
		if rtype is not None: 
			self.rtype = rtype
		if is_pkey:
			if self.rtype == 'int':
				self.rtype = 'hashcode'
			if nullable:
				self.nullable = False
				if _verbose:
					print >>sys.stderr, "Dropping nullable for PK:", self.fullname()
		if self.nullable:
			self.__nullable_name = self._fullname_escaped + '__IS_NULL__'
			
				
	def fullname(self, quoted=False, escaped=False):
		"""Returns the field name including the table, if known."""
		if escaped:
			if quoted: raise NotImplementedError
			return self._fullname_escaped
		if quoted:
			return self._fullname_quoted
		return self._fullname
	def to_decl(self):
		return self.to_old_decl()
	def to_old_decl(self):
		"""Returns Daikon variable declaration in the old format."""
		return '\n'.join((self.fullname(escaped=True), self.ftype, self.rtype, str(self.cmp)))
	def to_decl_v2(self):
		"""Returns Daikon variable declaration in the new (2.0) format."""
		fullname = self.fullname(escaped=True)
		is_array = '[' in self.rtype
		flags = 'non_null' if self.is_pkey else None
		return var_decl_v2(fullname, self.rtype, dec_type=self.ftype.replace(' ', '_'), array=is_array, flags=flags, comp=self.cmp)
	def _nullable_name(self, v1=False):
		return self.__nullable_name
	def null_decl_v1(self):
		if not self.nullable: raise RuntimeWarning, 'field is not nullable'
		nname = self._nullable_name(v1=True)
		return '\n'.join((nname, nname, 'hashcode', '8'))
	def null_trace_v1(self, val):
		if not self.nullable: raise RuntimeWarning, 'field is not nullable'
		nname = self._nullable_name(v1=True)
		return '\n'.join((nname, 'null' if val is None else str(id('')), '1')) # FIXME id('')???
	def null_decl_v2(self):
		if not self.nullable: raise RuntimeWarning, 'field is not nullable'
		nname = self._nullable_name(v1=False)
		return var_decl_v2(nname, 'hashcode', dec_type=nname, comp='8')
	def __repr__(self):
		return "Field(name=%(name)r, ftype=%(ftype)r, rtype=%(rtype)r, table=%(table)r, is_pkey=%(is_pkey)r, nullable=%(nullable)r)" % self.__dict__

def var_decl_v2(variable, rep_type, dec_type='unspecified', var_kind='variable', flags=None, array=None, comp='1'):
	data = ['  variable ' + variable, 'var-kind ' + var_kind, 'dec-type ' + dec_type, 'rep-type ' + rep_type]
	if array: data.append('array 1')
	if flags: data.append('flags ' + flags)
	data.append('comparability ' + comp)
	return '\n    '.join(data) + '\n'

def to_val(val):
	if val is None:
		return 'nonsensical' # FIXME is nonsensical the same as null?
	return val

try:
	from valconv import to_str_val
	print "Using valconv.to_str_val..."
except:
	print "Using Python version of to_str_val..."
	RE_STR_ESCAPE = re.compile(r'[\r\n\t\b"]')
	_escapes = {'\r': '\\r', '\n': '\\n', '\t': '\\t', '\b': '\\b', '"': '\\"'}
	def __str_escape(matcher, escapes=_escapes):
		val = matcher.group(0)
		return escapes.get(val, val)

	def to_str_val(val, pattern=RE_STR_ESCAPE):
		if val is None:
			return 'null'
		return '"' + pattern.sub(__str_escape, str(val)) + '"'

def to_bit_val(val):
	if val is None:
		return 'nonsensical'
	return ord(val)

if array('b').itemsize == 1:
	def __to_bin_str(val):
		return ' '.join(imap(str, array('b', val)))
else:
	def __sbyte(val):
		if val & 0x80:
			return val - 256
		return val
	def __to_bin_str(val):
		return ' '.join(imap(str, imap(__sbyte, imap(ord, val))))

def to_bin_val(val):
	if val is None:
		return 'nonsensical' # FIXME, represent null?
	return "[%s]" % __to_bin_str(val)

def to_set_val(val):
	if not val:
		return 'nonsensical' # FIXME, represent null?
	return '["%s"]' % '" "'.join( x.replace('"', '\\"') for x in val.split(',') )


def convert(basename, decls_version=2, decls=True, dtrace=True, use_gzip=True, compress=_DEFAULT_COMPRESS, append=False, tables=None, **conn_args):
	decls_path  = basename + '.decls'
	fields_path = basename + '.fields'
	dtrace_path = basename + '.dtrace'
	conn = None
	try:
		conn = MySQLdb.connect(**conn_args)
		fields = None
		
		if decls:
			# read metadata, write serialized fields and Daikon .decls file
			if decls_version == 1:
				write_decls = write_old_decls
			elif decls_version == 2:
				write_decls = write_decls_v2
			else:
				raise ValueError, "decls_version must be 1 or 2"
			fields = get_table_fields(conn, save_to=fields_path)
			write_decls(fields, decls_path)
		
		if dtrace:
			# write dtrace with fields either from above or previously serialized
			if fields is None:
				with open(fields_path, 'rb') as fieldsfile:
					fields = pickle.load(fieldsfile)
			write_old_trace(conn, fields, dtrace_path, use_gzip=use_gzip, compress=compress, append=append, tables=tables)
	finally:
		if conn: conn.close()

def get_table_names(conn):
	"""Retrieves the tables from given MySQL connection."""
	cur = conn.cursor()
	try:
		cur.execute('SHOW TABLES')
		return [ row[0] for row in cur ]
	finally:
		cur.close()

_RE_STR = re.compile(r'enum|(var)?char|(big|small|medium)?text', re.IGNORECASE)
_RE_INT = re.compile(r'(big|small|medium|tiny)?int(eger)?', re.IGNORECASE)
_RE_BIT = re.compile(r'bit', re.IGNORECASE)
_RE_DBL = re.compile(r'float|decimal|double', re.IGNORECASE)
_RE_BIN = re.compile(r'(big|medium|small)?blob', re.IGNORECASE)
_RE_SET = re.compile(r'set', re.IGNORECASE)
_RE_TIME = re.compile(r'datetime|timestamp', re.IGNORECASE)
_RE_DATE = re.compile(r'date', re.IGNORECASE)
def ftype_to_rep_val_comp(ftype):
	pindex = ftype.find('(')
	base_type = ftype if pindex == -1 else ftype[:pindex]
	
	if _RE_STR.match(base_type):
		return ('java.lang.String', to_str_val, '1')
	elif _RE_INT.match(base_type):
		return ('int', to_val, '2')
	elif _RE_BIT.match(base_type):
		return ('int', to_bit_val, '2')
	elif _RE_DBL.match(base_type):
		return ('double', to_val, '3')
	elif _RE_BIN.match(base_type):
		return ('int[]', to_bin_val, '4[2]')
	elif _RE_SET.match(base_type):
		return ('java.lang.String[]', to_set_val, '5[1]')
	elif _RE_TIME.match(base_type):
		return ('java.lang.String', to_str_val, '6')
	elif _RE_DATE.match(base_type):
		return ('java.lang.String', to_str_val, '7')
	else:
		print >>sys.stderr, "Warn: Unhandled base type:", base_type
		return ('java.lang.String', to_str_val, '1')
	

def get_table_fields(conn, save_to=None):
	tables = get_table_names(conn)

	fields = {} # mapped by table name
	cur = conn.cursor()
	try:
		for table in tables:
			cur.execute('DESCRIBE `%s`' % table)
			tfields = []
			for row in cur:
				fname, ftype, nullable, keytype = row[:4]
				nullable = nullable in ('YES', 'yes')
				f = Field(fname, ftype, table=table, is_pkey=keytype=='PRI', nullable=nullable)
				if _verbose: print repr(f)
				tfields.append(f)
			fields[table] = tfields
	finally:
		cur.close()

	if save_to is not None:
		with open(save_to, mode='wb') as outfile:
			pickle.dump(fields, outfile, protocol=pickle.HIGHEST_PROTOCOL)
	return fields

def write_old_decls(all_fields, outpath):
	"""Writes declarations out in the old Daikon format."""
	with open(outpath, 'w') as out:
		for table, fields in all_fields.iteritems():
			out.write('DECLARE\n%s:::POINT\n' % table)
			for field in fields:
				if field.nullable:
					out.write('%s\n' % field.null_decl_v1())
				out.write('%s\n' % field.to_old_decl())
			out.write('\n')

def write_old_trace(conn, all_fields, outpath, use_gzip=True, compress=_DEFAULT_COMPRESS, append=False, tables=None):
	"""Writes a data trace of the current database state"""
	if not use_gzip:
		out = open(outpath, 'a' if append else 'w')
	else:
		if not outpath.endswith('.gz'): 
			outpath += '.gz'
		out = GzipFile(outpath, 'ab' if append else 'wb', compress)
		
	if tables is not None and not isinstance(tables, set):
		tables = set(tables)
	
	# write the trace file
	with out:
		# build string pieces in a buffer to write once per db row
		# saves a lot of time, especially with gzip on
		buf = []
		write = buf.append
		def mwrite(*args): buf.extend(args)
		
		cur = conn.cursor()
		try:
			for table, fields in all_fields.iteritems():
				if tables and table not in tables: 
					continue

				tbl_point = '\n%s:::POINT\n' % table
				q = 'SELECT ' + ', '.join( f.fullname(quoted=True) for f in fields ) + \
					' FROM `' + table + '`'
				try:
					cur.execute(q)
					for row in cur:
						write(tbl_point)
						for i, field in enumerate(fields):
							val = row[i]
							if field.nullable:
								write(field.null_trace_v1(val))
								write('\n')
							fval = str(field.to_val(val))
							fmod = '1' if fval != 'nonsensical' else '2' 
							mwrite(field.fullname(escaped=True), '\n', fval, '\n', fmod, '\n')
						out.write(''.join(buf))
						del buf[:]
				except MySQLdb.Error, e:
					print >>sys.stderr, "Error %d: %s\nQuery: %s" % (e.args[0], e.args[1], q)
					raise
		finally:
			cur.close()

def write_decls_v2(all_fields, outpath):
	"""Writes declarations out in the version 2 Daikon format."""
	with open(outpath, 'w') as out:
		out.write('decl-version 2.0\n' \
			'input-language MySQL\n\n')
		for table, fields in all_fields.iteritems():
			out.write('ppt ' + table + ':::POINT\n')
			out.write('ppt-type point\n')
			for field in fields:
				if field.nullable:
					out.write(field.null_decl_v2())
				out.write(field.to_decl_v2())
			out.write('\n')

def main(args=None):
	if args is None: args = sys.argv[1:]
	try:
		opts, args = getopt.gnu_getopt(args, "hH:u:p:P:d:o:V:vc:f:O:at:",
			("help", "host=", "user=", "password=", "port=", "database=", 
			 "output=", "version=", "verbose", "no-gzip", "compress-level=", 
			 "fields-file=", "operation=", "append", "tables="))
	except getopt.GetoptError, err:
		print >>sys.stderr, str(err)
		return 1

	# defaults
	output = args[0] if args else None
	version = '2'
	verbose = 0
	use_gzip = True
	append = False
	tables = None
	compress_level = _DEFAULT_COMPRESS
	operation = set(('decls', 'dtrace'))
	
	# read options
	cargs = {}
	for o, a in opts:
		o = o.lstrip('-')
		if o in ('H', 'host'):
			cargs['host'] = a
		elif o in ('u', 'user'):
			cargs['user'] = a
		elif o in ('p', 'password'):
			cargs['passwd'] = a
		elif o in ('P', 'port'):
			cargs['port'] = a
		elif o in ('d', 'database'):
			cargs['db'] = a
		elif o in ('o', 'output'):
			output = a
		elif o in ('V', 'version'):
			version = a
		elif o in ('v', 'verbose'):
			verbose += 1
		elif o in ('no-gzip'):
			use_gzip = False
		elif o in ('c', 'compress-level'):
			compress_level = a
		elif o in ('O', 'operation'):
			if ',' in a:
				operation = set(a.split(','))
			else:
				operation = set((a,))
		elif o in ('a', 'append'):
			append = True
		elif o in ('t', 'tables'):
			tables = set(a.split(','))
			
	# check options
	if not output:
		print >>sys.stderr, "No output specified."
		return 1
	try:
		compress_level = int(compress_level)
		if compress_level == -1:
			use_gzip = False
		elif compress_level > 9:
			print >>sys.stderr, "Clamping compression level (" + str(compress_level) + ") to 9"
			compress_level = 9
		elif compress_level < -1:
			raise ValueError
	except ValueError:
		print >>sys.stderr, "Invalid compression level:", compress_level
		return 1
	if 'user' not in cargs:
		cargs['user'] = output
	if 'db' not in cargs:
		cargs['db'] = output
	if version not in ('1', '1.0', '2', '2.0'):
		print >>sys.stderr, "Unrecognized version:", version
		return 1
	
	global _verbose
	_verbose = verbose
	if verbose:
		print "Tracing '" + output + "' with version", version, "and args:\n" + repr(cargs)
	convert(output, decls_version=int(version), decls='decls' in operation, dtrace='dtrace' in operation, \
			use_gzip=use_gzip, compress=compress_level, append=append, tables=tables, **cargs)
	return 0

if __name__ == '__main__':
	sys.exit(main())
