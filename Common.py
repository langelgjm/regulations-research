from datetime import datetime
from urllib import urlencode
import sqlite3

class Logfile(object):
	'''
	Simple Logfile class.
	'''
	def __init__(self, filename, disp=False):
		self.filename = filename
		self.f = open(filename, "a")
		self.disp = disp 
		self.write("----- START -----")
	def write(self, msg):
		self.f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S "))
		self.f.write(msg)
		self.f.write("\n")
		if self.disp:
			print msg
	def flush(self):
		self.f.flush()
	def name(self):
		return self.f.name
	def close(self):
		self.write("----- END -----")
		self.f.close()

def make_config_dict(cp):
    '''
    Return a nested dict of sections/options by iterating through a ConfigParser instance.
    '''
    d = {}
    for s in cp.sections():
        e = {}
        for o in cp.options(s):
            e[o] = cp.get(s,o)
        d[s] = e
    return d

def yield_sql_results(cursor, fetch_num=1000):
	'''
	Yield batches of SQL results.
	'''
	while True:
		results = cursor.fetchmany(fetch_num)
		if not results:
			break
		for result in results:
			yield result

def get_sqlite_conn(filename):
	return sqlite3.connect(filename)

def get_sql_row_as_dict(cursor, table, primary_key, primary_key_value):
	'''
	Return a single row as a dictionary; on error, return error.
	Be sure to pass in a cursor object whose connection has row_factory=sqlite3.Row
	You may pass in a ROWID instead of a primary key, but if you do so, be sure to set primary_key = None
	'''
	try:
		if primary_key is not None:       
			r = dict(cursor.execute('SELECT * FROM ' + table + ' WHERE ' + primary_key + '=?', (primary_key_value,)).fetchone())
		else:
			r = dict(cursor.execute('SELECT * FROM ' + table + ' WHERE ROWID=?', (primary_key_value,)).fetchone())
	except sqlite3.InterfaceError as e:
		return e
	except sqlite3.OperationalError as e:
		return e
	return r

def get_sql_rows(cursor, table, primary_key, primary_key_value):
	'''
	Return a cursor ready to fetch selected rows; on error, return error.
	Be sure to pass in a cursor object whose connection has row_factory=sqlite3.Row
	You may pass in a ROWID instead of a primary key, but if you do so, be sure to set primary_key = None
	'''
	try:
		if primary_key is not None:       
			cursor = cursor.execute('SELECT * FROM ' + table + ' WHERE ' + primary_key + '=?', (primary_key_value,))
		else:
			cursor = cursor.execute('SELECT * FROM ' + table + ' WHERE ROWID=?', (primary_key_value,))
	except sqlite3.InterfaceError as e:
		return e
	except sqlite3.OperationalError as e:
		return e
	return cursor

def sample_sql_rows(cursor, table, primary_key, sample_size=1000):
	'''
	Return a cursor ready to fetch sampled rows; on error, return error.
	'''
	try:       
		#cursor = cursor.execute('SELECT * FROM ' + table + ' ORDER BY RANDOM() LIMIT ' + str(sample_size))
		#Check to make sure this is really random. The hard-coded constant comes from select count(*) from table
		#cursor = cursor.execute('SELECT * FROM ' + table + ' WHERE ' + primary_key + ' IN (SELECT ' + primary_key + ' from ' + table + ' WHERE RANDOM() % 3229781 LIMIT ' + str(sample_size) + ')')
		cursor = cursor.execute('SELECT * FROM ' + table + ' WHERE ' + primary_key + ' IN (SELECT ' + primary_key + ' from ' + table + ' ORDER BY RANDOM() LIMIT ' + str(sample_size) + ')')
	except sqlite3.InterfaceError as e:
		return e
	except sqlite3.OperationalError as e:
		return e
	return cursor

def get_sqlite_sample(cursor, table, sample_size=1000):
	'''
	Return a random sample of ROWIDs of size sample_size.
	'''
	try:
		r = cursor.execute('SELECT ROWID FROM ' + table + ' ORDER BY RANDOM() LIMIT ' + sample_size).fetchall()
	except sqlite3.InterfaceError as e:
		return e
	except sqlite3.OperationalError as e:
		return e
	return r

def update_sql_value(cursor, table, primary_key, primary_key_value, column, value, commit=True):
	'''
	Update a single column in existing records with a given value.
	Optionally commit the update after performing it.
	Return None on success, otherwise return the error message.
	'''
	try:
		cursor.execute('UPDATE ' + table + ' SET ' + column + '=? WHERE ' + primary_key + '=?', (value, primary_key_value))
	except sqlite3.InterfaceError as e:
		return e
	except sqlite3.OperationalError as e:
		return e
	if commit:
		cursor.commit()
	return None

def len_GET(url, params):
	'''
	Return the approximate length (in bytes) of a formatted GET request.
	'''
	r = 'GET {}?{} HTTP/1.1'.format(url, urlencode(params))
	return len(r)

html_entity_translation_table = {"&acute;":    " ",
    "&cedil;":    " ",
    "&circ;":    " ",
    "&macr;":    " ",
    "&middot;":    " ",
    "&tilde;":    " ",
    "&uml;":    " ",
    "&Aacute;":    "a",
    "&aacute;":    "a",
    "&Acirc;":    "a",
    "&acirc;":    "a",
    "&AElig;":    "ae",
    "&aelig;":    "ae",
    "&Agrave;":    "a",
    "&agrave;":    "a",
    "&Aring;":    "a",
    "&aring;":    "a",
    "&Atilde;":    "a",
    "&atilde;":    "a",
    "&Auml;":    "a",
    "&auml;":    "a",
    "&Ccedil;":    "c",
    "&ccedil;":    "c",
    "&Eacute;":    "e",
    "&eacute;":    "e",
    "&Ecirc;":    "e",
    "&ecirc;":    "e",
    "&Egrave;":    "e",
    "&egrave;":    "e",
    "&ETH;":    "th",
    "&eth;":    "th",
    "&Euml;":    "e",
    "&euml;":    "e",
    "&Iacute;":    "I",
    "&iacute;":    "I",
    "&Icirc;":    "I",
    "&icirc;":    "I",
    "&Igrave;":    "I",
    "&igrave;":    "I",
    "&Iuml;":    "I",
    "&iuml;":    "I",
    "&Ntilde;":    "n",
    "&ntilde;":    "n",
    "&Oacute;":    "o",
    "&oacute;":    "o",
    "&Ocirc;":    "o",
    "&ocirc;":    "o",
    "&OElig;":    "oe",
    "&oelig;":    "oe",
    "&Ograve;":    "o",
    "&ograve;":    "o",
    "&Oslash;":    "o",
    "&oslash;":    "o",
    "&Otilde;":    "o",
    "&otilde;":    "o",
    "&Ouml;":    "o",
    "&ouml;":    "o",
    "&Scaron;":    "s",
    "&scaron;":    "s",
    "&szlig;":    "ss",
    "&THORN;":    "th",
    "&thorn;":    "th",
    "&Uacute;":    "u",
    "&uacute;":    "u",
    "&Ucirc;":    "u",
    "&ucirc;":    "u",
    "&Ugrave;":    "u",
    "&ugrave;":    "u",
    "&Uuml;":    "u",
    "&uuml;":    "u",
    "&Yacute;":    "y",
    "&yacute;":    "y",
    "&yuml;":    "y",
    "&Yuml;":    "y",
    "&amp;":    " ",
    "&gt;":    " ",
    "&lt;":    " ",
    "&quot;":    " ",
    "&cent;":    " ",
    "&curren;":    " ",
    "&euro;":    " ",
    "&pound;":    " ",
    "&yen;":    " ",
    "&brvbar;":    " ",
    "&bull;":    " ",
    "&copy;":    " ",
    "&dagger;":    " ",
    "&Dagger;":    " ",
    "&frasl;":    " ",
    "&hellip;":    " ",
    "&iexcl;":    " ",
    "&image;":    " ",
    "&iquest;":    " ",
    "&lrm;":    " ",
    "&mdash;":    " ",
    "&ndash;":    " ",
    "&not;":    " ",
    "&oline;":    " ",
    "&ordf;":    " ",
    "&ordm;":    " ",
    "&para;":    " ",
    "&permil;":    " ",
    "&prime;":    " ",
    "&Prime;":    " ",
    "&real;":    " ",
    "&reg;":    " ",
    "&rlm;":    " ",
    "&sect;":    " ",
    "&shy;":    " ",
    "&sup1;":    " ",
    "&trade;":    " ",
    "&weierp;":    " ",
    "&bdquo;":    " ",
    "&laquo;":    " ",
    "&ldquo;":    " ",
    "&lsaquo;":    " ",
    "&lsquo;":    " ",
    "&raquo;":    " ",
    "&rdquo;":    " ",
    "&rsaquo;":    " ",
    "&rsquo;":    " ",
    "&sbquo;":    " ",
    "&emsp;":    " ",
    "&ensp;":    " ",
    "&nbsp;":    " ",
    "&thinsp;":    " ",
    "&zwj;":    " ",
    "&zwnj;":    " ",
    "&deg;":    " ",
    "&divide;":    " ",
    "&frac12;":    " ",
    "&frac14;":    " ",
    "&frac34;":    " ",
    "&ge;":    " ",
    "&le;":    " ",
    "&minus;":    " ",
    "&sup2;":    " ",
    "&sup3;":    " ",
    "&times;":    " ",
    "&alefsym;":    " ",
    "&and;":    " ",
    "&ang;":    " ",
    "&asymp;":    " ",
    "&cap;":    " ",
    "&cong;":    " ",
    "&cup;":    " ",
    "&empty;":    " ",
    "&equiv;":    " ",
    "&exist;":    " ",
    "&fnof;":    " ",
    "&forall;":    " ",
    "&infin;":    " ",
    "&int;":    " ",
    "&isin;":    " ",
    "&lang;":    " ",
    "&lceil;":    " ",
    "&lfloor;":    " ",
    "&lowast;":    " ",
    "&micro;":    " ",
    "&nabla;":    " ",
    "&ne;":    " ",
    "&ni;":    " ",
    "&notin;":    " ",
    "&nsub;":    " ",
    "&oplus;":    " ",
    "&or;":    " ",
    "&otimes;":    " ",
    "&part;":    " ",
    "&perp;":    " ",
    "&plusmn;":    " ",
    "&prod;":    " ",
    "&prop;":    " ",
    "&radic;":    " ",
    "&rang;":    " ",
    "&rceil;":    " ",
    "&rfloor;":    " ",
    "&sdot;":    " ",
    "&sim;":    " ",
    "&sub;":    " ",
    "&sube;":    " ",
    "&sum;":    " ",
    "&sup;":    " ",
    "&supe;":    " ",
    "&there4;":    " ",
    "&Alpha;":    " ",
    "&alpha;":    " ",
    "&Beta;":    " ",
    "&beta;":    " ",
    "&Chi;":    " ",
    "&chi;":    " ",
    "&Delta;":    " ",
    "&delta;":    " ",
    "&Epsilon;":    " ",
    "&epsilon;":    " ",
    "&Eta;":    " ",
    "&eta;":    " ",
    "&Gamma;":    " ",
    "&gamma;":    " ",
    "&Iota;":    " ",
    "&iota;":    " ",
    "&Kappa;":    " ",
    "&kappa;":    " ",
    "&Lambda;":    " ",
    "&lambda;":    " ",
    "&Mu;":    " ",
    "&mu;":    " ",
    "&Nu;":    " ",
    "&nu;":    " ",
    "&Omega;":    " ",
    "&omega;":    " ",
    "&Omicron;":    " ",
    "&omicron;":    " ",
    "&Phi;":    " ",
    "&phi;":    " ",
    "&Pi;":    " ",
    "&pi;":    " ",
    "&piv;":    " ",
    "&Psi;":    " ",
    "&psi;":    " ",
    "&Rho;":    " ",
    "&rho;":    " ",
    "&Sigma;":    " ",
    "&sigma;":    " ",
    "&sigmaf;":    " ",
    "&Tau;":    " ",
    "&tau;":    " ",
    "&Theta;":    " ",
    "&theta;":    " ",
    "&thetasym;":    " ",
    "&upsih;":    " ",
    "&Upsilon;":    " ",
    "&upsilon;":    " ",
    "&Xi;":    " ",
    "&xi;":    " ",
    "&Zeta;":    " ",
    "&zeta;":    " ",
    "&crarr;":    " ",
    "&darr;":    " ",
    "&dArr;":    " ",
    "&harr;":    " ",
    "&hArr;":    " ",
    "&larr;":    " ",
    "&lArr;":    " ",
    "&rarr;":    " ",
    "&rArr;":    " ",
    "&uarr;":    " ",
    "&uArr;":    " ",
    "&clubs;":    " ",
    "&diams;":    " ",
    "&hearts;":    " ",
    "&spades;":    " ",
    "&loz;":    " ",
    "&#180;":    " ",
    "&#184;":    " ",
    "&#710;":    " ",
    "&#175;":    " ",
    "&#183;":    " ",
    "&#732;":    " ",
    "&#168;":    " ",
    "&#193;":    "a",
    "&#225;":    "a",
    "&#194;":    "a",
    "&#226;":    "a",
    "&#198;":    "ae",
    "&#230;":    "ae",
    "&#192;":    "a",
    "&#224;":    "a",
    "&#197;":    "a",
    "&#229;":    "a",
    "&#195;":    "a",
    "&#227;":    "a",
    "&#196;":    "a",
    "&#228;":    "a",
    "&#199;":    "c",
    "&#231;":    "c",
    "&#201;":    "e",
    "&#233;":    "e",
    "&#202;":    "e",
    "&#234;":    "e",
    "&#200;":    "e",
    "&#232;":    "e",
    "&#208;":    "th",
    "&#240;":    "th",
    "&#203;":    "e",
    "&#235;":    "e",
    "&#205;":    "I",
    "&#237;":    "I",
    "&#206;":    "I",
    "&#238;":    "I",
    "&#204;":    "I",
    "&#236;":    "I",
    "&#207;":    "I",
    "&#239;":    "I",
    "&#209;":    "n",
    "&#241;":    "n",
    "&#211;":    "o",
    "&#243;":    "o",
    "&#212;":    "o",
    "&#244;":    "o",
    "&#338;":    "oe",
    "&#339;":    "oe",
    "&#210;":    "o",
    "&#242;":    "o",
    "&#216;":    "o",
    "&#248;":    "o",
    "&#213;":    "o",
    "&#245;":    "o",
    "&#214;":    "o",
    "&#246;":    "o",
    "&#352;":    "s",
    "&#353;":    "s",
    "&#223;":    "ss",
    "&#222;":    "th",
    "&#254;":    "th",
    "&#218;":    "u",
    "&#250;":    "u",
    "&#219;":    "u",
    "&#251;":    "u",
    "&#217;":    "u",
    "&#249;":    "u",
    "&#220;":    "u",
    "&#252;":    "u",
    "&#221;":    "y",
    "&#253;":    "y",
    "&#255;":    "y",
    "&#376;":    "y",
    "&#38;":    " ",
    "&#62;":    " ",
    "&#60;":    " ",
    "&#162;":    " ",
    "&#164;":    " ",
    "&#8364;":    " ",
    "&#163;":    " ",
    "&#165;":    " ",
    "&#166;":    " ",
    "&#8226;":    " ",
    "&#169;":    " ",
    "&#8224;":    " ",
    "&#8225;":    " ",
    "&#8260;":    " ",
    "&#8230;":    " ",
    "&#161;":    " ",
    "&#8465;":    " ",
    "&#191;":    " ",
    "&#8206;":    " ",
    "&#8212;":    " ",
    "&#8211;":    " ",
    "&#172;":    " ",
    "&#8254;":    " ",
    "&#170;":    " ",
    "&#186;":    " ",
    "&#182;":    " ",
    "&#8240;":    " ",
    "&#8242;":    " ",
    "&#8243;":    " ",
    "&#8476;":    " ",
    "&#174;":    " ",
    "&#8207;":    " ",
    "&#167;":    " ",
    "&#173;":    " ",
    "&#185;":    " ",
    "&#8482;":    " ",
    "&#8472;":    " ",
    "&#8222;":    " ",
    "&#171;":    " ",
    "&#8220;":    " ",
    "&#8249;":    " ",
    "&#8216;":    " ",
    "&#187;":    " ",
    "&#8221;":    " ",
    "&#8250;":    " ",
    "&#8217;":    " ",
    "&#8218;":    " ",
    "&#8195;":    " ",
    "&#8194;":    " ",
    "&#8201;":    " ",
    "&#8205;":    " ",
    "&#8204;":    " ",
    "&#176;":    " ",
    "&#247;":    " ",
    "&#189;":    " ",
    "&#188;":    " ",
    "&#190;":    " ",
    "&#8805;":    " ",
    "&#8804;":    " ",
    "&#8722;":    " ",
    "&#178;":    " ",
    "&#179;":    " ",
    "&#215;":    " ",
    "&#8501;":    " ",
    "&#8743;":    " ",
    "&#8736;":    " ",
    "&#8776;":    " ",
    "&#8745;":    " ",
    "&#8773;":    " ",
    "&#8746;":    " ",
    "&#8709;":    " ",
    "&#8801;":    " ",
    "&#8707;":    " ",
    "&#402;":    " ",
    "&#8704;":    " ",
    "&#8734;":    " ",
    "&#8747;":    " ",
    "&#8712;":    " ",
    "&#9001;":    " ",
    "&#8968;":    " ",
    "&#8970;":    " ",
    "&#8727;":    " ",
    "&#181;":    " ",
    "&#8711;":    " ",
    "&#8800;":    " ",
    "&#8715;":    " ",
    "&#8713;":    " ",
    "&#8836;":    " ",
    "&#8853;":    " ",
    "&#8744;":    " ",
    "&#8855;":    " ",
    "&#8706;":    " ",
    "&#8869;":    " ",
    "&#177;":    " ",
    "&#8719;":    " ",
    "&#8733;":    " ",
    "&#8730;":    " ",
    "&#9002;":    " ",
    "&#8969;":    " ",
    "&#8971;":    " ",
    "&#8901;":    " ",
    "&#8764;":    " ",
    "&#8834;":    " ",
    "&#8838;":    " ",
    "&#8721;":    " ",
    "&#8835;":    " ",
    "&#8839;":    " ",
    "&#8756;":    " ",
    "&#913;":    " ",
    "&#945;":    " ",
    "&#914;":    " ",
    "&#946;":    " ",
    "&#935;":    " ",
    "&#967;":    " ",
    "&#916;":    " ",
    "&#948;":    " ",
    "&#917;":    " ",
    "&#949;":    " ",
    "&#919;":    " ",
    "&#951;":    " ",
    "&#915;":    " ",
    "&#947;":    " ",
    "&#921;":    " ",
    "&#953;":    " ",
    "&#922;":    " ",
    "&#954;":    " ",
    "&#923;":    " ",
    "&#955;":    " ",
    "&#924;":    " ",
    "&#956;":    " ",
    "&#925;":    " ",
    "&#957;":    " ",
    "&#937;":    " ",
    "&#969;":    " ",
    "&#927;":    " ",
    "&#959;":    " ",
    "&#934;":    " ",
    "&#966;":    " ",
    "&#928;":    " ",
    "&#960;":    " ",
    "&#982;":    " ",
    "&#936;":    " ",
    "&#968;":    " ",
    "&#929;":    " ",
    "&#961;":    " ",
    "&#931;":    " ",
    "&#963;":    " ",
    "&#962;":    " ",
    "&#932;":    " ",
    "&#964;":    " ",
    "&#920;":    " ",
    "&#952;":    " ",
    "&#977;":    " ",
    "&#978;":    " ",
    "&#933;":    " ",
    "&#965;":    " ",
    "&#926;":    " ",
    "&#958;":    " ",
    "&#918;":    " ",
    "&#950;":    " ",
    "&#8629;":    " ",
    "&#8595;":    " ",
    "&#8659;":    " ",
    "&#8596;":    " ",
    "&#8660;":    " ",
    "&#8592;":    " ",
    "&#8656;":    " ",
    "&#8594;":    " ",
    "&#8658;":    " ",
    "&#8593;":    " ",
    "&#8657;":    " ",
    "&#9827;":    " ",
    "&#9830;":    " ",
    "&#9829;":    " ",
    "&#9824;":    " ",
    "&#9674;":    " "}

def main():
	pass

if __name__ == "__main__":
	main()
