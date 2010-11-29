#!/usr/bin/env python
from __future__ import with_statement
import os, sys, re
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
import alchemy_trace
import sqlalchemy.exc
import BeautifulSoup as bsoup
import functools

def _get_meta(db):
	if not isinstance(db, dict): db = {'db': db}
	try:
		engine = sqlalchemy.create_engine('mysql://%(db)s@localhost/%(db)s' % db)
		return (engine, alchemy_trace.reflected_tables(engine))
	except sqlalchemy.exc.SQLAlchemyError, ex1:
		try:
			engine = sqlalchemy.create_engine('mysql://%(db)s:%(db)s@localhost/%(db)s' % db)
			return (engine, alchemy_trace.reflected_tables(engine))
		except sqlalchemy.exc.SQLAlchemyError, ex2:
			print >>sys.stderr, 'Failed to process %(db)r in two attempts:' % db
			print >>sys.stderr, ex1
			print >>sys.stderr, ex2
			return None

def print_db_stats(dbs):
	count_star = sqlalchemy.func.count('*')
	row_template = r'  \texttt{%(db)s} & %(tables)d & %(columns)d & %(rows)d \\ \hline'
	for db in dbs:
		args = {'db': db}
		meta = _get_meta(args)
		if meta is None:
			continue
		engine, meta = meta
		conn = engine.connect()
		try:
			args['tables'] = len(meta.tables)
			rows = cols = 0
			for table in meta.tables.itervalues():
				cols += len(table.columns)
				row_count = sqlalchemy.select(columns=[count_star], from_obj=[table], bind=conn)
				rows += row_count.scalar()

			args['columns'] = cols
			args['rows'] = rows

			print row_template % args
		finally:
			conn.close()
	
def count_test_cases(report_dir, print_names=False):
	"""Counts test cases from either XML or text files in a given directory.
	If both types are present, the XML file will be used.

	@param report_dir: the directory containing the report files
	@param print_names: to print debug info
	@return: the number of test cases or None if there are no files to check
	"""
	if not os.path.isdir(report_dir):
		print >>sys.stderr, 'Not a directory:', report_dir
		return

	#tests = 0
	xmlpat = re.compile(r'^TEST-.*\.xml$')
	txtpat = re.compile(r'^TEST-.*\.txt$')
	make_abs = functools.partial(os.path.join, report_dir)

	all_files = os.listdir(report_dir)
	xmlfiles = map(make_abs, filter(re.compile(r'^TEST-.*\.xml$').match, all_files))
	if xmlfiles:
		return count_xml_tests(xmlfiles, print_names=print_names)
	txtfiles = map(make_abs, filter(re.compile(r'^TEST-.*\.txt$').match, all_files))
	if txtfiles:
		return count_txt_tests(txtfiles, print_names=print_names)

	print >>sys.stderr, "No test case files in dir:", report_dir

def count_xml_tests(files, print_names=False):
	"""Counts JUnit test cases from a collection of XML files.

	@param files: the xml file paths
	@param print_names: to print debug info
	@return the number of test cases found
	"""
	tests  = 0
	for xmlfile in files:
		with open(xmlfile, 'r') as handle:
			soup = bsoup.BeautifulStoneSoup(handle)
			testcases = soup.findAll('testcase')
			tests += len(testcases)
			if print_names:
				print 'Tests in %r:' % xmlfile
				for testcase in testcases:
					print '\t' + testcase.get('classname', '???') + '.' + testcase['name']
	return tests

def count_txt_tests(files, print_names=False):
	"""Counts JUnit test cases from a collection of text files.

	@param files: the text file paths
	@param print_names: to print debug info
	@return the number of test cases found
	"""
	tests = 0
	suitepat = re.compile(r'^Testsuite:\s+([\w.$]+).*')
	casepat  = re.compile(r'^Testcase:\s+([\w$]+).*')
	for txtfile in files:
		cases = []
		suite = None
		with open(txtfile, 'r') as handle:
			for line in handle:
				m = suitepat.match(line)
				if m:
					suite = m.group(1)
					continue
				m = casepat.match(line)
				if m:
					cases.append(m.group(1))
		tests += len(cases)
		if print_names:
			print 'Tests in %r:' % txtfile
			prefix = '\t' + (suite or '???') + '.'
			print '\n'.join( prefix + case for case in cases )
	return tests

def main(dbs):
	print_db_stats(dbs)

if __name__ == '__main__':
	dbs = ('world', 'sakila', 'menagerie', 'employees', 'itrust', 'jwhois', 'jtrac')
	main(dbs)

