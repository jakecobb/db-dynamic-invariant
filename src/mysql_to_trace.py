#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import with_statement
import sys
import re
import MySQLdb
import getopt
from array import array
from itertools import imap

_verbose = 0

_DEFAULT_ARGS = {'host': 'localhost', 'db': 'world', 'user': 'world'}
def get_conn_args(**args):
	"""Returns the given connection arguments with defaults for any not given."""
	return dict(_DEFAULT_ARGS).update(args)

class Field(object):
	"""Represents a database table field."""
	def __init__(self, name, ftype, rtype=None, table=None, is_pkey=False, nullable=True):
		self.table = table
		self.name = name
		self.ftype = ftype
		self.nullable = nullable
		self.is_pkey = is_pkey
		if is_pkey and nullable:
			self.nullable = False
			if _verbose:
				print >>sys.stderr, "Dropping nullable for PK:", self.fullname()
		self.rtype, self.to_val, self.cmp = ftype_to_rep_val_comp(ftype)
		if rtype is not None: 
			self.rtype = rtype
		if is_pkey and self.rtype == 'int':
			self.rtype = 'hashcode'
	def fullname(self, quoted=False, escaped=False):
		"""Returns the field name including the table, if known."""
		p = (self.table, self.name) if self.table else (self.name,)
		if quoted:
			p = map(lambda x: "`%s`" % x, p)
		if escaped:
			p = map(lambda x: x.replace(' ', '_'), p)
		return '.'.join(p)
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
#		if v1:
			return self.fullname(escaped=True) + '__IS_NULL__'
#		return 'NULL(' + self.fullname(escaped=True) + ')' 
	def null_decl_v1(self):
		if not self.nullable:
			return ''
		nname = self._nullable_name(v1=True)
		return '\n'.join((nname, nname, 'hashcode', '8'))
	def null_trace_v1(self, val):
		if not self.nullable:
			return ''
		nname = self._nullable_name(v1=True)
		return '\n'.join((nname, 'null' if val is None else str(id('')), '1')) # FIXME id('')???
	def null_decl_v2(self):
		if not self.nullable: return ''
		nname = self._nullable_name(v1=False)
		return var_decl_v2(nname, 'hashcode', dec_type=nname, comp='8')

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

def to_str_val(val):
	if val is None:
		return 'null'
	return '"%s"' % str(val).replace('"', '\\"')


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


def convert_simple(outfile, **conn_args):
	conn = None
	try:
		conn = MySQLdb.connect(**conn_args)
		all_fields = get_table_fields(conn)
		write_old_decls(all_fields, outfile + '.decls')
		write_old_trace(conn, all_fields, outfile + '.dtrace')
	finally:
		if conn: conn.close()

def convert(outfile, **conn_args):
	conn = None
	try:
		conn = MySQLdb.connect(**conn_args)
		all_fields = get_table_fields(conn)
		write_decls_v2(all_fields, outfile + '.decls')
		write_old_trace(conn, all_fields, outfile + '.dtrace') # FIXME v2 trace
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
_RE_DBL = re.compile(r'float|decimal|double', re.IGNORECASE)
_RE_BIN = re.compile(r'blob', re.IGNORECASE)
_RE_SET = re.compile(r'set', re.IGNORECASE)
_RE_TIME = re.compile(r'datetime|timestamp', re.IGNORECASE)
def ftype_to_rep(ftype):
	raise DeprecationWarning
	pindex = ftype.find('(')
	base_type = ftype if pindex == -1 else ftype[:pindex]
	
	if _RE_STR.match(base_type):
		return 'java.lang.String'
	elif _RE_INT.match(base_type):
		return 'int'
	elif _RE_DBL.match(base_type):
		return 'double'
	elif _RE_BIN.match(base_type):
		return 'int[]'
	else:
		print >>sys.stderr, "Warn: Unhandled base type:", base_type
		return 'java.lang.String'
	
def ftype_to_rep_val_comp(ftype):
	pindex = ftype.find('(')
	base_type = ftype if pindex == -1 else ftype[:pindex]
	
	if _RE_STR.match(base_type):
		return ('java.lang.String', to_str_val, '1')
	elif _RE_INT.match(base_type):
		return ('int', to_val, '2')
	elif _RE_DBL.match(base_type):
		return ('double', to_val, '3')
	elif _RE_BIN.match(base_type):
		return ('int[]', to_bin_val, '4[2]')
	elif _RE_SET.match(base_type):
		return ('java.lang.String[]', to_set_val, '5[1]')
	elif _RE_TIME.match(base_type):
		return ('java.lang.String', to_str_val, '6')
	else:
		print >>sys.stderr, "Warn: Unhandled base type:", base_type
		return ('java.lang.String', to_str_val, '1')
	

def get_table_fields(conn):
	tables = get_table_names(conn)

	fields = {} # mapped by table name
	cur = conn.cursor()
	try:
		for table in tables:
			cur.execute('DESCRIBE `%s`' % table)
			tfields = []
			for row in cur:
				fname, ftype, nullable, keytype = row[:4]
				tfields.append(Field(fname, ftype, table=table, is_pkey=keytype=='PRI', nullable=nullable))
			fields[table] = tfields
	finally:
		cur.close()

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

def nullable_decl_v1(field):
	raise DeprecationWarning
	fname = field.fullname(escaped=True)
	return '\n'.join('NULL(' + fname + ')', fname + ' IS NULL', 'hashcode', '8')

def write_old_trace(conn, all_fields, outpath):
	"""Writes a data trace of the current database state"""
	with open(outpath, 'w') as out:
		# header needed?
		cur = conn.cursor()
		try:
			for table, fields in all_fields.iteritems():
				tbl_point = '\n%s:::POINT\n' % table
				q = 'SELECT ' + ', '.join( f.fullname(True) for f in fields ) + \
					' FROM `' + table + '`'
				try:
					cur.execute(q)
					for row in cur:
						out.write(tbl_point)
						for i, field in enumerate(fields):
							val = row[i]
							if field.nullable:
								out.write(field.null_trace_v1(val))
								out.write('\n')
							fval = field.to_val(val)
							fmod = 1 if fval != 'nonsensical' else 2 
							out.write( '%s\n%s\n%d\n' % (field.fullname(escaped=True), fval, fmod) )
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

def process_world(declpath='world.decls', dtracepath='world.dtrace'):
	convert_simple('world', **_DEFAULT_ARGS)

def main(args=None):
	if args is None: args = sys.argv[1:]
	try:
		opts, args = getopt.gnu_getopt(args, "hH:u:p:P:d:o:V:v",
			("help", "host=", "user=", "port=", "password=", "database=", "output=", "version=", "verbose"))
	except getopt.GetoptError, err:
		print >>sys.stderr, str(err)
		return 1

	# defaults
	output = args[0] if args else None
	version = '2'
	verbose = 0
	
	# read options
	cargs = {}
	for o, a in opts:
		o = o.lstrip('-')
		if o in ('H', 'host'):
			cargs['host'] = a
		elif o in ('u', 'user'):
			cargs['user'] = a
		elif o in ('p', 'port'):
			cargs['port'] = a
		elif o in ('d', 'database'):
			cargs['db'] = a
		elif o in ('o', 'output'):
			output = a
		elif o in ('V', 'version'):
			version = a
		elif o in ('v', 'verbose'):
			verbose += 1
			
	# check options
	if not output:
		print >>sys.stderr, "No output specified."
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
	if int(version) == 1:
		convert_simple(output, **cargs)
	else:
		convert(output, **cargs)
	return 0

if __name__ == '__main__':
	sys.exit(main())
