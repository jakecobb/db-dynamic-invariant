#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import with_statement
import sys
import re
import MySQLdb

_DEFAULT_ARGS = {'host': 'localhost', 'db': 'world', 'user': 'world'}

class Field(object):
	"""Represents a database table field."""
	def __init__(self, name, ftype, rtype=None, table=None):
		self.table = table
		self.name = name
		self.ftype = 'dbfield/' + ftype
		self.rtype = ftype_to_rep(ftype) if rtype is None else rtype
		if self.rtype == 'java.lang.String':
			self.to_val = to_str_val
		else:
			self.to_val = to_val
	def fullname(self):
		"""Returns the field name including the table, if known."""
		if self.table:
			return self.table + '.' + self.name
		return self.name
	def to_decl(self):
		return self.to_old_decl()
	def to_old_decl(self):
		"""Returns Daikon variable declaration in the old format."""
		return '\n'.join((self.fullname(), self.ftype, self.rtype, str(id(self))))

def to_val(val):
	if val is None:
		return 'nonsensical'
	return val
def to_str_val(val):
	if val is None:
		return 'null'
	return '"%s"' % str(val).replace('"', '\\"')

def convert(outfile, conn_args):
	if not conn_args:
		conn_args = _DEFAULT_ARGS
	conn = None
	try:
		conn = MySQLdb.connect(**conn_args)
		decls = table_decls(conn)
		
		with open(outfile, 'w') as out:
			out.write("DECLARE\n")
			for k, v in decls:
				point_name = "%s:::POINT" % (k, v[0])
			
			
	except MySQLdb.Error, e:
		print >>sys.stderr, "DB Error %d: %s" % (e.args[0], e.args[1])
		raise e
	finally:
		if conn:
			conn.close()

def convert_simple(outfile, decls):
	with open(outfile, 'wb') as out:
		out.write("DECLARE\n")
		buf = []
		for table, fields in decls.iteritems():
			out.write("%s:::POINT\n" % table)
			for fname, ftype, frep in fields:
				out.write('\n'.join((fname, ftype, frep, '1', '\n')))
			out.write("\n")


def get_table_names(conn):
	tables = None

	cur = conn.cursor()
	try:
		cur.execute('SHOW TABLES')
		tables = [ row[0] for row in cur ]
	finally:
		cur.close()

	return tables

def table_decls(conn):
	tables = get_table_names(conn)
	
	decls = {}
	cur = conn.cursor()
	try:
		for table in tables:
			cur.execute('DESCRIBE `%s`' % table)
			fields = []
			for row in cur:
				fname, ftype = row[:2]
				frep = ftype_to_rep(ftype)
				finfo = (fname, ftype, frep)
				fields.append(finfo)
			decls[table] = fields
	finally:
		cur.close()

	return decls

_RE_STR = re.compile(r'(var)?char|enum')
_RE_INT = re.compile(r'(big|small|medium)?int')
_RE_DBL = re.compile(r'float|decimal|double')
def ftype_to_rep(ftype):
	pindex = ftype.find('(')
	base_type = ftype if pindex == -1 else ftype[:pindex]
	
	if _RE_STR.match(base_type):
		return 'java.lang.String'
	elif _RE_INT.match(base_type):
		return 'int'
	elif _RE_DBL.match(base_type):
		return 'double'
	else:
		print >>sys.stderr, "Warn: Unhandled base type:", base_type
		return 'java.lang.String'

def get_table_fields(conn):
	tables = get_table_names(conn)

	fields = {} # mapped by table name
	cur = conn.cursor()
	try:
		for table in tables:
			cur.execute('DESCRIBE `%s`' % table)
			tfields = []
			for row in cur:
				fname, ftype = row[:2]
				tfields.append(Field(fname, ftype, table=table))
			
			fields[table] = tfields
	finally:
		cur.close()

	return fields

def write_old_decls(all_fields, outpath):
	"""Writes declarations out in the old Daikon format."""
	with open(outpath, 'w') as out:
		#out.write('DECLARE\n')
		for table, fields in all_fields.iteritems():
			out.write('DECLARE\n%s:::POINT\n' % table)
			for field in fields:
				out.write('%s\n' % field.to_old_decl())
			out.write('\n')

def write_old_trace(conn, all_fields, outpath):
	"""Writes a data trace of the current database state"""
	with open(outpath, 'w') as out:
		# header needed?
		cur = conn.cursor()
		try:
			templ = '%s\n%s\n1\n'
			for table, fields in all_fields.iteritems():
				tbl_point = '\n%s:::POINT\n' % table
				q = 'SELECT ' + ', '.join( f.fullname() for f in fields ) + \
					' FROM `' + table + '`'
				cur.execute(q)
				for row in cur:
					out.write(tbl_point)
					for i, field in enumerate(fields):
						out.write( templ % (field.fullname(), field.to_val(row[i])) )
		finally:
			cur.close()

def process_world(declpath='world.decls', dtracepath='world.dtrace'):
	try:
		conn = MySQLdb.connect(**_DEFAULT_ARGS)
		all_fields = get_table_fields(conn)
		write_old_decls(all_fields, declpath)
		write_old_trace(conn, all_fields, dtracepath)
	finally:
		if conn: conn.close()

def main(argv=None):
	if argv is None:
		argv = sys.argv[:]



if __name__ == '__main__':
	main()
