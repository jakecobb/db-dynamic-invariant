cimport libc.stdlib

cdef inline short should_escape(char c):
	if c == '\r' or c == '\n' or c == '\b' or c == '\t' or c == '"' or c == '\\':
		return 1
	return 0

cdef inline char escape_char(char c):
	if c == '\r': return 'r'
	if c == '\n': return 'n'
	if c == '\t': return 't'
	if c == '\b': return 'b'
	# should be '"' or '\\'
	return c


def to_str_val(val):
	if val is None:
		return 'null'

	cdef bytes strval = str(val)
	cdef Py_ssize_t vallen = len(strval)
	if vallen == 0:
		return '""'

	# count how many characters are needed
	cdef Py_ssize_t newlen = vallen + 2
	cdef char c
	for c in strval:
		if should_escape(c):
			newlen += 1
	
	# now copy while escaping and surrounding in quotes
	cdef char* c_newval = <char*>libc.stdlib.malloc((newlen+1) * sizeof(char))
	c_newval[0] = '"'
	c_newval[newlen] = 0
	c_newval[newlen-1] = '"'
	cdef unsigned int i = 1
	for c in strval:
		if should_escape(c):
			c_newval[i] = '\\'
			i += 1
			c_newval[i] = escape_char(c)
		else:
			c_newval[i] = c
		i += 1

	try:
		return <bytes>c_newval
	finally:
		libc.stdlib.free(c_newval)
