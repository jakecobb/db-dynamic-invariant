#!/usr/bin/env python
'''
Created on Nov 21, 2010

@author: jake
'''
from __future__ import with_statement
import cPickle as pickle
import mysql_to_trace as mtrace
import os
import sqlalchemy
import sqlalchemy.engine.base
import sqlalchemy.ext.serializer as sqlserializer
import sys

_engine = None

class Tracer(object):
	def __init__(self, dbname, datadir='invariant-data', engine=None, url=None, conn_args=None):
		self.dbname = dbname
		self.datadir = datadir
		self.url = url
		self.conn_args = conn_args
		self.engine = engine
		self.meta = None
		self.fields = None
		self.use_gzip = True
		self.compress_level = 3
		self.append_trace = True
		
	def _check_datadir(self):
		if not os.path.isdir(self.datadir):
			os.makedirs(self.datadir)

	def load_tables(self, force_fresh=False, skip_save=False):
		"""Loads the table metadata.
		
		@param force_fresh: to always reflect even if saved data is available
		@param skip_save: to skip saving data for later reuse
		"""
		ser_path = os.path.join(self.datadir, self.dbname + '_meta.ser')
		# maybe load existing
		if not force_fresh:
			self.meta = _readobj(ser_path, alchemy=True)
			if self.meta: return
		
		# reflect table tale
		if self.engine is None:
			self.engine = sqlalchemy.create_engine(self.url, connect_args=self.conn_args)
		self.meta = reflected_tables(self.engine)
		
		# maybe save for reuse
		try:
			if not skip_save: _writeobj(self.meta, ser_path, alchemy=True)
		except IOError, e:
			print >>sys.stderr, "Failed to save metadata, path=%s, err=%s" % (ser_path, e.strerror)
	
	def load_fields(self, force_fresh=False, skip_save=False):
		"""Loads the field data.  
		
		If the table data is not yet loaded, it will be loaded first
		using the same load/save parameters given to this method.
		
		@param force_fresh: to always reflect even if saved data is available
		@param skip_save: to skip saving data for later reuse
		"""
		if self.meta is None: 
			self.load_tables(force_fresh=force_fresh, skip_save=skip_save)
		
		ser_path = os.path.join(self.datadir, self.dbname + '_fields.ser')
		if not force_fresh:
			self.fields = _readobj(ser_path)
			if self.fields: return
		
		self.fields = get_trace_fields(self.meta)
		
		try:
			if not skip_save: _writeobj(self.fields, ser_path)
		except IOError, e:
			print >>sys.stderr, "Failed to save field data, path=%s, err=%s" % (ser_path, e.strerror)

	def write_decls(self, overwrite=True, v1=False):
		"""Writes field data as a Daikon declarations file in the 2.0 format.
		
		@param overwrite: if an existing file should be overwritten
		@param v1: if the old (1.0) format should be used instead
		"""
		if not self.fields: 
			self.load_fields()
		self._check_datadir()
			
		decls_path = os.path.join(self.datadir, self.dbname + '.decls')
		if overwrite or not os.path.isfile(decls_path):
			write = mtrace.write_decls_v2 if not v1 else mtrace.write_old_decls
			write(self.fields, decls_path)

	def write_trace(self, tables=None):
		"""Writes the current DB state as a Daikon trace file.
		
		@param tables: a sequence of table names to trace instead of all tables
		"""
		if not self.fields:
			self.load_fields()
		self._check_datadir()
		
		if not isinstance(tables, (set, type(None))):
			tables = set(tables)
		
		mode = 'a' if self.append_trace else 'w'
		trace_path = os.path.join(self.datadir, self.dbname + '.dtrace')
		if self.use_gzip:
			mode += 'b'
			trace_path += '.gz'
			out = mtrace.GzipFile(trace_path, mode, self.compress_level)
		else:
			out = open(trace_path, mode)
			
		with out:
			# build string pieces in a buffer to write once per db row
			# saves a lot of time, especially with gzip on
			buf = []
			write = buf.append
			def mwrite(*args): buf.extend(args)
			
			conn = self.engine.connect()
			try:
				for table, fields in self.fields.iteritems():
					if tables and table not in tables: 
						continue
	
					tbl_point = '\n%s:::POINT\n' % table
					dbtable = self.meta.tables[table]
					result = conn.execute(dbtable.select())
					try:
						for row in result:
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
					finally:
						result.close()
			finally:
				conn.close()

		
def reflected_tables(engine):
	"""Reflects a set of database tables
	
	@param engine: an sqlalchemy database engine
	@rtype: list
	@return: sqlalchemy.MetaData with all the table data 
	"""
	conn = engine.connect()
	try:
		meta = sqlalchemy.MetaData()
		for tname in engine.table_names():
#			table = sqlalchemy.Table(tname, meta)
			# work-around for PK redefinition bug using the same metadata
			table = sqlalchemy.Table(tname, sqlalchemy.MetaData()) 
			conn.reflecttable(table)
			table.tometadata(meta)
		return meta
	finally:
		conn.close()

def get_trace_fields(engine_or_meta=None, save_to=None):
	"""Gets the trace fields by reflection from the given engine.
	
	@param engine_or_meta: an sqlalchemy engine for reflection or 
		an sqlalchemy.MetaData with the tables already reflected
	@param save_to: optional path to save the fields to
	@rtype: dict
	@return: a dict of table name -> Field
	"""
	if isinstance(engine_or_meta, sqlalchemy.engine.base.Engine):
		meta = reflected_tables(engine_or_meta)
	elif isinstance(engine_or_meta, sqlalchemy.MetaData):
		meta = engine_or_meta
	else:
		raise TypeError("Need an 'Engine' or 'MetaData', not '%s'" % str(type(engine_or_meta)))
	
	fields = {} # mapped by table name
	for table in meta.tables.itervalues():
		# various str(...) calls are to avoid unicode strings
		fields[str(table.name)] = [
			mtrace.Field(str(col.name), col.type.get_col_spec(), table=str(table.name), is_pkey=col.primary_key, nullable=col.nullable) 
			for col in table.columns
		]

	if save_to is not None:
		with open(save_to, mode='wb') as outfile:
			pickle.dump(fields, outfile, protocol=pickle.HIGHEST_PROTOCOL)
	return fields

def _readobj(path, alchemy=False):
	if not os.path.isfile(path):
		return None
	with open(path, 'rb') as handle:
		if alchemy:
			return sqlserializer.loads(handle.read())
		return pickle.load(handle)
def _writeobj(obj, path, alchemy=False):
	dirname = os.path.dirname(path)
	if dirname and not os.path.isdir(dirname):
		os.makedirs(dirname)
	with open(path, 'wb') as handle:
		if alchemy:
			handle.write(sqlserializer.dumps(obj))
		else:
			pickle.dump(obj, handle, protocol=pickle.HIGHEST_PROTOCOL)

def main(args=None):
	args = args or sys.argv[1:]
	base = args[1] if len(args) >= 2 else 'world'
	url = args[0] if args else 'mysql://' + base + '@localhost/' + base
		
	engine = sqlalchemy.create_engine(url)
	tracer = Tracer(base, engine=engine)
	tracer.load_fields(force_fresh=True)
	tracer.write_decls()
	tracer.write_trace()

if __name__ == '__main__':
	main()
