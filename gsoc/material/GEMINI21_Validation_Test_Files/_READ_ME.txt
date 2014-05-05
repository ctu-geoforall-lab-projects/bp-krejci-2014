UK GEMINI2.1 VALIDATION TEST FILES
----------------------------------
Version 1 October 2012
Author: Peter Vodden, NERC Centre for Ecology & Hydrology

These files are provided to UK Location for their use, to assist with the development and testing of applications thaty are aware of the ISO 19139, ISO19115 and UK GEMINI 2.1 standards for recording metadata.

INTRODUCTION
------------

The ISO19139 XML test files contained in this package are designed to trigger error reports in validation software, as follows:

01 - an ISO19115 Dataset record expected to fail XSD schema validation with a specific schema error.
02 - an ISO19115 Dataset record expected to pass schema validation, but fail a specified 19139 schematron constraint.
03 - an ISO19115 Dataset record expected to pass schema validation, but fail a specified GEMINI2.1 schematron constraint.
04 - an ISO19115 Dataset record expected to pass XSD, 19139 and GEMINI2.1 Schematron validation.
05 - an ISO19115 Series record expected to fail XSD schema validation with a specific schema error.
06 - an ISO19115 Series record expected to pass schema validation, but fail a specified 19139 schematron constraint.
07 - an ISO19115 Series record expected to pass schema validation, but fail a specified GEMINI2.1 schematron constraint.
08 - an ISO19115 Series record expected to pass XSD, 19139 and GEMINI2.1 Schematron validation.
09 - an ISO19115 Service record expected to fail XSD schema validation with a specific schema error.
10 - an ISO19115 Service record expected to pass schema validation, but fail a specified 19139 schematron constraint.
11 - an ISO19115 Service record expected to pass schema validation, but fail a specified GEMINI2.1 schematron constraint.
12 - an ISO1911 5 Service record expected to pass XSD, 19139 and GEMINI2.1 Schematron validation.

In tabular form, expected results are:

Record          XSD     19139       GEMINI2.1
                Schema  Schmatron   Schematron
------          ------  ---------   ----------

01 Dataset      fail    n/a         n/a
02 Dataset      pass    fail        pass
03 Dataset      pass    pass        fail
04 Dataset      pass    pass        pass
05 Series       fail    n/a         n/a
06 Series       pass    fail        pass
07 Series       pass    pass        fail
08 Series       pass    pass        pass
09 Service      fail    n/a         n/a
10 Service      pass    fail        pass
11 Service      pass    pass        fail
12 Service      pass    pass        pass

The files are numbered and named accordingly, and the embedded File Identifier and Title elements inside the XML are filled out to make identification as simple as possible.

EXPECTED RESULTS
----------------
Two typical Schematron-basde validation reports are included with the packege:

19139_Schematron_Report.htm
GEMINI_Schematron_Report.htm

These demonstrate the type of error messages that should be generated if schematron based validation software is working correctly against these files.

CONTEXT
-------

The files were created using:


Altova XML Spy 2010
Altova XML engine 2011

And with reference to:

UK GEMINI standards, guidance and Schematron available from http://location.defra.gov.uk/resources/discovery-metadata-service/

Eden XSD schemas available from http://eden.ign.fr/xsd/isotc211/isofull/20090316

ISO19139 Schematron available from MEDIN at http://www.oceannet.org/marine_data_standards/medin_disc_stnd.html

DISCLAIMER
----------

The author and the Centre for Ecology & Hydrology make no claim concerning the completeness, accuracy or general fitness for purpose of these files, and will not be responsible for any consequence arising from their use.

These XML records are only designed to deliberately trigger error messages in validation software.

The element values in these records (whilst based originally on "real" records), are an amalgam that does not necessarily make sense as actual metadata describing a real data object.

Whilst the files may be used to shed light on particular XML encodings, this document is not part of any published standard and the files described should not be treated as exemplars of good practice in either creating or encoding UK GEMINI2.1 metadata. Use of a WMS linked within thse files is governed by the CEH Information Gateway terms & conditions https://gateway.ceh.ac.uk/disclaimer. 

END OF DOCUMENT






 
