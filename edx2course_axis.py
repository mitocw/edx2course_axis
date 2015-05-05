#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
<nbformat>3.0</nbformat>

File:   edx2course_axis.py

From an edX xml file set, generate:

  course_id, index, url_name,  category, gformat, start, due, name, path, module_id, data

course_id = edX standard {org}/{course_num}/{semester}
index     = integer giving temporal order of course material
url_name  = unique key for item
category  = (known as a "tag" in some edX docs) chapter, sequential, vertical,
            problem, video, html, ...
gformat   = "grading format", ie assignment name
start     = start date
due       = end date
name      = full (display) name of item
path      = path with url_name's to this item from course root, ie
            chapter/sequential/position
module_id = edX standard {org}/{course_num}/{category}/{url_name} id for an
            x-module
data      = extra data for element, eg you-tube id's for videos
chapter_mid = module_id of the chapter within which this x-module exists
              (empty if not within a chapter)


usage:   python edx2course_axis.py COURSE_DIR

or:      python edx2course_axis.py course_tar_file.xml.tar.gz

requires BeautifulSoup and path.py to be installed
"""

import os
import sys
import re
import csv
import logging
import codecs
import json
import glob
import datetime
import xbundle
import tempfile
from collections import namedtuple, defaultdict
from lxml import etree
from path import path
from fix_unicode import fix_bad_unicode

DO_SAVE_TO_MONGO = False
DO_SAVE_TO_BIGQUERY = False
DATADIR = "DATA"

VERBOSE_WARNINGS = True
FORCE_NO_HIDE = False

log = logging.getLogger()  # pylint: disable=invalid-name
logging.basicConfig()
log.setLevel(logging.DEBUG)

# storage class for each axis element
Axel = namedtuple(
    'Axel',
    'course_id index url_name category gformat start due name path module_id data chapter_mid')


class Policy(object):

    """
    Handle policy.json for edX course.  Also deals with grading_policy.json
    if present in same directory.
    """
    policy = None
    grading_policy = None
    InheritedSettings = ['format', 'hide_from_toc', 'start', 'due']

    def __init__(self, pfn):
        """
        pfn = policy file name
        """
        self.pfn = path(pfn)
        print "loading policy file %s" % pfn
        self.policy = json.loads(open(pfn).read())

        gfn = self.pfn.dirname() / 'grading_policy.json'
        if os.path.exists(gfn):
            self.gfn = gfn
            self.grading_policy = json.loads(open(gfn).read())

    @property
    def semester(self):
        """
        Find "semester" string inside JSON object.
        """
        keys = [x for x in self.policy.keys() if x.startswith("course/")]
        assert len(keys) == 1
        return keys[0].split("/", 1)[1]

    def get_metadata(self, xml, setting, default=None, parent=False):
        """
        Retrieve policy for xml element, given the policy JSON and, for a specific setting.
        Handles inheritance of certain settings (like format and hide_from_toc)

        xml = etree
        setting = string
        """
        if parent:
            val = xml.get(setting, None)
            if val is not None and not val == 'null' and (
                    (setting in self.InheritedSettings) and not val == ""):
                return val

        url_name = xml.get(
            'url_name', xml.get('url_name_orig', '<no_url_name>'))
        pkey = '%s/%s' % (xml.tag, url_name)

        if pkey in self.policy and setting in self.policy[pkey]:
            return self.policy[pkey][setting]

        if not setting in self.InheritedSettings:
            return default

        parent = xml.getparent()  # inherited metadata: try parent
        if parent is not None:
            return self.get_metadata(parent, setting, default, parent=True)


def get_from_parent(xml, attr, default):
    """
    get attribute from parent, recursing until end or found
    """
    parent = xml.getparent()
    if parent is not None:
        val = parent.get(attr, None)
        if val is not None:
            return val
        return get_from_parent(parent, attr, default)
    return default


def date_parse(datestr, retbad=False):
    """
    Parse a string into a datetime, handling a variety
    of formatting options.
    """
    if not datestr:
        return None

    if datestr.startswith('"') and datestr.endswith('"'):
        datestr = datestr[1:-1]

    formats = [
        '%Y-%m-%dT%H:%M:%SZ',    	# 2013-11-13T21:00:00Z
        '%Y-%m-%dT%H:%M:%S.%f',    	# 2012-12-04T13:48:28.427430
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%S+00:00',  # 2014-12-09T15:00:00+00:00
        '%Y-%m-%dT%H:%M',		    # 2013-02-12T19:00
        '%B %d, %Y',			    # February 25, 2013
        '%B %d, %H:%M, %Y', 		# December 12, 22:00, 2012
        '%B %d, %Y, %H:%M', 		# March 25, 2013, 22:00
        '%B %d %Y, %H:%M',		    # January 2 2013, 22:00
        '%B %d %Y', 			    # March 13 2014
        '%B %d %H:%M, %Y',		    # December 24 05:00, 2012
    ]

    for fmt in formats:
        try:
            return datetime.datetime.strptime(datestr, fmt)
        except ValueError:
            continue

    print "Date %s unparsable" % datestr
    if retbad:
        return "Bad"
    return None


class CourseInfo(object):
    """
    Gather course information from a course XML file.
    """
    def __init__(self, filename, policyfn='', course_dir=''):
        # pylint: disable=no-member
        cxml = etree.parse(filename).getroot()
        self.cxml = cxml
        self.org = cxml.get('org')
        self.course = cxml.get('course')
        self.url_name = cxml.get('url_name')
        if policyfn:
            self.load_policy(policyfn)
        else:
            pfn = course_dir / 'policies' / self.url_name + ".json"
            if not os.path.exists(pfn):
                pfn = course_dir / 'policies' / self.url_name / "policy.json"
            if os.path.exists(pfn):
                self.load_policy(pfn)
            else:
                log.error("Missing policy file {0}".format(pfn))

    def load_policy(self, pfn):
        """
        Set self.policy.
        """
        self.policy = Policy(pfn)
        if self.url_name is None:
            self.url_name = self.policy.semester

def make_axis(course_dir):
    """
    return dict of {course_id : { policy, xbundle, axis (as list of Axel elements) }}
    """
    # Because pylint thinks lxml.etree has no parse or Element members...
    # pylint: disable=no-member
    course_dir = path(course_dir)
    courses = get_courses(course_dir)

    log.debug(
        "{0} course runs found: {1}".format(
            len(courses), [c.url_name for c in courses]
        )
    )

    ret = {}

    # construct axis for each policy
    for cinfo in courses:
        policy = cinfo.policy
        course = cinfo.course
        cid = '%s/%s/%s' % (cinfo.org, course, policy.semester)
        log.debug('course_id={0}'.format(cid))
        # Generate XBundle for course.
        xml = etree.parse(
            course_dir / ('course/%s.xml' % policy.semester)
        ).getroot()
        bundle = xbundle.XBundle(
            keep_urls=True,
            skip_hidden=True,
            keep_studio_urls=True)
        bundle.policy = policy.policy
        cxml = bundle.import_xml_removing_descriptor(course_dir, xml)

        # Append metadata.
        metadata = etree.Element('metadata')
        cxml.append(metadata)
        policy_xml = etree.Element('policy')
        policy_xml.text = json.dumps(policy.policy)
        metadata.append(policy_xml)
        grading_policy_xml = etree.Element('grading_policy')
        grading_policy_xml.text = json.dumps(policy.grading_policy)
        metadata.append(grading_policy_xml)

        caxis = []

        ret[cid] = dict(
            policy=policy.policy,
            bundle=etree.tostring(cxml, pretty_print=True),
            axis=caxis,
            grading_policy=policy.grading_policy,
        )
        walk(cxml, course, cid, cinfo.org, policy, [1], caxis)

    return ret

def get_courses(course_dir):
    """
    Get list of courses.
    """
    # if roots directory exists, use that for different course versions
    if os.path.exists(course_dir / 'roots'):
        # get roots
        roots = glob.glob(course_dir / 'roots/*.xml')
        return [CourseInfo(fn, '', course_dir) for fn in roots]

    # Single course.xml file - use different policy files in policy directory,
    # though
    else:

        filename = course_dir / 'course.xml'

        # get semesters
        policies = glob.glob(course_dir / 'policies/*.json')
        assetsfn = course_dir / 'policies/assets.json'
        if str(assetsfn) in policies:
            policies.remove(assetsfn)
        if not policies:
            policies = glob.glob(course_dir / 'policies/*/policy.json')
        if not policies:
            log.debug("Error: no policy files found!")

        return [CourseInfo(filename, pfn) for pfn in policies]

def walk(
        element, course, cid, org, policy, index, caxis, seq_num=1, paths=None,
        seq_type=None, parent_start=None, parent=None, chapter=None):
    """
    Recursively traverse course tree.

    element        = current etree element
    seq_num  = sequence of current element in its parent, starting from 1
    paths     = list of url_name's to current element, following edX's hierarchy conventions
    seq_type = problemset, sequential, or videosequence
    parent_start = start date of parent of current etree element
    parent   = parent module
    chapter  = the last chapter module_id seen while walking through the tree
    """

    # Fixes dangerous-default-value.
    if paths is None:
        paths = []
    url_name = element.get(
        'url_name',
        element.get(
            'url_name_orig',
            ''))
    if not url_name:
        display_name = element.get('display_name')
        if display_name is not None:
            # 2012 convention for converting display_name to url_name
            url_name = display_name.strip().replace(
                ' ',
                '_')
            url_name = url_name.replace(':', '_')
            url_name = url_name.replace('.', '_')
            url_name = url_name.replace(
                '(', '_').replace(')', '_').replace('__', '_')

    data = None
    start = None

    if not FORCE_NO_HIDE:
        hide = policy.get_metadata(element, 'hide_from_toc')
        if hide is not None and not hide == "false":
            msg = (
                '[edx2course_axis] Skipping {0} ({1}), it has '
                'hide_from_toc={3}'
            )
            log.debug(
                msg.format(
                    element.tag, element.get('display_name', '<noname>'), hide)
            )
            return

    # special: for video, let data = youtube ID(s)
    if element.tag == 'video':
        data = element.get('youtube', '')
        if data:
            # old ytid format - extract just the 1.0 part of this
            # 0.75:JdL1Vo0Hru0,1.0:lbaG3uiQ6IY,1.25:Lrj0G8RWHKw,1.50:54fs3-WxqLs
            ytid = data.replace(' ', '').split(',')
            ytid = [
                z[1] for z in [
                    y.split(':') for y in ytid] if z[0] == '1.0']
            if ytid:
                data = ytid
        if not data:
            data = element.get('youtube_id_1_0', '')
        if data:
            data = '{"ytid": "%s"}' % data

    if element.tag == 'problem' and element.get(
            'weight') is not None and element.get('weight'):
        try:
            data = '{"weight": %f}' % float(element.get('weight'))
        except (TypeError, ValueError) as err:
            log.error("Error converting weight {0}: {1}".format(
                element.get('weight'), err,
            ))

    if element.tag == 'html':
        iframe = element.find('.//iframe')
        if iframe is not None:
            log.debug("found iframe in html {0}".format(url_name))
            src = iframe.get('src', '')
            if 'https://www.youtube.com/embed/' in src:
                match = re.search('embed/([^"/?]+)', src)
                if match:
                    data = '{"ytid": "%s"}' % match.group(1)
                    log.debug("data={0}".format(data))

    # url_name is mandatory if we are to do anything with this element
    if url_name:
        # url_name = url_name.replace(':','_')
        display_name = element.get('display_name', url_name)
        try:
            display_name = unicode(display_name)
            display_name = fix_bad_unicode(display_name)
        except Exception as ex:
            log.error(
                'unicode error, type(display_name)={0}'.format(
                    type(display_name)))
            raise ex
        # policy display_name - if given, let that override default
        pdn = policy.get_metadata(element, 'display_name')
        if pdn is not None:
            display_name = pdn

        start = date_parse(
            policy.get_metadata(
                element,
                'start',
                '',
                parent=True))

        if parent_start is not None and start < parent_start:
            if VERBOSE_WARNINGS:
                msg = (
                    "Warning: start of {0} element {1} happens before start "
                    "{2} of parent: using parent start"
                )
                log.warning(msg.format(start, element.tag, parent_start))
            start = parent_start

        # drop bad due date strings
        if date_parse(element.get('due', None), retbad=True) == 'Bad':
            element.set('due', '')

        due = date_parse(
            policy.get_metadata(
                element,
                'due',
                '',
                parent=True))
        if element.tag == "problem":
            log.debug(
                "setting problem due date: for {0} due={1}".format(
                    url_name, due))

        gformat = element.get(
            'format',
            policy.get_metadata(
                element,
                'format',
                ''))
        if url_name == 'hw0':
            log.debug("gformat for hw0 = {0}".format(gformat))

        # compute path
        # The hierarchy goes: `course > chapter > (problemset |
        # sequential | videosequence)`

        tags = set([
            'problemset', 'sequential', 'videosequence', 'proctor', 'randomize'
        ])
        if element.tag == 'chapter':
            paths = [url_name]
        elif element.tag in tags:
            seq_type = element.tag
            paths = [paths[0], url_name]
        else:
            # note arrays are passed by reference, so copy, don't
            # modify
            paths = paths[:] + [str(seq_num)]

        # compute module_id
        if element.tag == 'html':
            # module_id which appears in tracking log
            module_id = '{0}/{1}/{2}/{3}'.format(
                org, course, seq_type, '/'.join(paths[1:3]))
        else:
            module_id = '{0}/{1}/{2}/{3}'.format(
                org, course, element.tag, url_name)

        # done with getting all info for this axis element; save it
        path_str = '/' + '/'.join(paths)
        axel = Axel(
            cid, index[
                0], url_name, element.tag, gformat, start, due, display_name,
            path_str, module_id, data, chapter,
        )
        caxis.append(axel)
        index[0] += 1
    else:
        if VERBOSE_WARNINGS:
            if element.tag in ['transcript', 'wiki', 'metadata']:
                pass
            else:
                msg = (
                    "Missing url_name for element {0} "
                    "(attrib={1}, parent_tag={2})"
                )
                log.warning(
                    msg.format(
                        element, element.attrib,
                        (parent.tag if parent is not None else ''))
                )

    # chapter?
    if element.tag == 'chapter':
        the_chapter = module_id
    else:
        the_chapter = chapter

    # done processing this element, now process all its children
    tags = set([
        'html', 'problem', 'discussion', 'customtag', 'poll_question',
        'combinedopenended', 'metadata',
    ])
    if element.tag not in tags:
        # if <vertical> with no url_name then keep seq_num for children
        inherit_seq_num = (element.tag == 'vertical' and not url_name)
        if not inherit_seq_num:
            seq_num = 1
        for child in element:
            if (not str(child).startswith('<!--')) \
                    and (not child.tag in ['discussion', 'source']):
                walk(
                    child,
                    course,
                    cid,
                    org,
                    policy,
                    index,
                    caxis,
                    seq_num,
                    paths,
                    seq_type,
                    parent_start=start,
                    parent=element,
                    chapter=the_chapter)
                if not inherit_seq_num:
                    seq_num += 1

def save_data_to_mongo(cid, caset, bundle=None):
    """
    Save course axis data to mongo

    cid = course_id
    caset = list of course axis data in dict format
    bundle = XML bundle of course (everything except static files)
    """
    try:
        import save_to_mongo
    except ImportError:
        raise ImportError("Unable to import axis2bigquery.")
    save_to_mongo.do_save(cid, caset, bundle)


def save_data_to_bigquery(cid, caset, bundle=None, log_msg=None,
                          use_dataset_latest=False):
    """
    Save course axis data to bigquery

    cid = course_id
    caset = list of course axis data in dict format
    bundle = XML bundle of course (everything except static files)
    """
    try:
        import axis2bigquery
    except ImportError:
        raise ImportError("Unable to import axis2bigquery.")

    axis2bigquery.do_save(
        cid,
        caset,
        bundle,
        DATADIR,
        log_msg,
        use_dataset_latest=use_dataset_latest)


def fix_duplicate_url_name_vertical(axis):
    """
    1. Look for duplicate url_name values
    2. If a vertical has a duplicate url_name with anything else, rename that url_name
       to have a "_vert" suffix.

    axis = list of Axel objects
    """
    axis_by_url_name = defaultdict(list)
    for idx in range(len(axis)):
        ael = axis[idx]
        axis_by_url_name[ael.url_name].append(idx)

    for url_name, idxset in axis_by_url_name.items():
        if len(idxset) == 1:
            continue
        print "--> Duplicate url_name %s shared by:" % url_name
        for idx in idxset:
            ael = axis[idx]
            print "       %s" % str(ael)
            if ael.category == 'vertical':
                nun = "%s_vertical" % url_name
                print "          --> renaming url_name to become %s" % nun
                new_ael = ael._replace(url_name=nun)
                axis[idx] = new_ael


def process_course(
        course_path, use_dataset_latest=False, force_course_id=None):
    """
    if force_course_id is specified, then that value is used as the course_id
    """
    ret = make_axis(course_path)

    # Save data as csv and txt: loop through each course (multiple policies
    # can exist withing a given course dir).
    for default_cid, cdat in ret.iteritems():

        cid = force_course_id or default_cid

        # Write out xbundle to xml file.
        bfn = '%s/xbundle_%s.xml' % (DATADIR, cid.replace('/', '__'))
        write_xbundle(bfn, ret[default_cid]['bundle'])

        # Clean up xml file with xmllint if available.
        if os.system('which xmllint') == 0:
            os.system('xmllint --format %s > %s.new' % (bfn, bfn))
            os.system('mv %s.new %s' % (bfn, bfn))

        print "saving data for %s" % cid

        fix_duplicate_url_name_vertical(cdat['axis'])

        header = (
            "index", "url_name", "category", "gformat", "start", 'due',
            "name", "path", "module_id", "data", "chapter_mid",
        )
        attribute_set = [
            {x: getattr(ae, x) for x in header} for ae in cdat['axis']
        ]

        # optional save to mongodb
        if DO_SAVE_TO_MONGO:
            save_data_to_mongo(cid, attribute_set, ret[default_cid]['bundle'])

        # optional save to bigquery
        if DO_SAVE_TO_BIGQUERY:
            save_data_to_bigquery(
                cid,
                attribute_set,
                ret[default_cid]['bundle'],
                cdat['log_msg'],
                use_dataset_latest=use_dataset_latest)

        # save as text file
        textfn = '{0}/axis_{1}.txt'.format(DATADIR, cid.replace('/', '__'))

        write_text(textfn, header, attribute_set)

        # save as csv file
        csvfn = '%s/axis_%s.csv' % (DATADIR, cid.replace('/', '__'))
        write_csv(csvfn, header, cid, attribute_set)

def write_xbundle(filename, bundle):
    """
    Write xbundle XML.
    """
    codecs.open(filename, 'w', encoding='utf8').write(bundle)

    print "Writing out xbundle to %s" % filename

    # Clean up xml file with xmllint if available.
    if os.system('which xmllint') == 0:
        os.system('xmllint --format %s > %s.new' % (filename, filename))
        os.system('mv %s.new %s' % (filename, filename))

def write_text(filename, header, attribute_set):
    """
    Print out to text file
    """
    afp = codecs.open(filename, 'w', encoding='utf8')
    aformat = "%8s\t%40s\t%24s\t%16s\t%16s\t%16s\t%s\t%s\t%s\t%s\t%s\n"
    afp.write(aformat % header)
    afp.write(aformat % tuple(["--------"] * 11))
    for attributes in attribute_set:
        afp.write(aformat % tuple([attributes[x] for x in header]))
    afp.close()

def write_csv(filename, header, cid, attribute_set):
    """
    Export as CSV.
    """
    filename = '%s/axis_%s.csv' % (DATADIR, cid.replace('/', '__'))
    csv_file = open(filename, 'wb')
    writer = csv.writer(
        csv_file,
        dialect="excel",
        quotechar='"',
        quoting=csv.QUOTE_ALL)
    writer.writerow(header)
    for attributes in attribute_set:
        try:
            data = [('%s' % attributes[k]).encode('utf8') for k in header]
            writer.writerow(data)
        except UnicodeEncodeError as err:
            log.error("Failed to write row {0}: {1}".format(data, err))
    csv_file.close()
    print "Saved course axis to %s" % filename

def process_xml_tar_gz_file(
        fndir, use_dataset_latest=False, force_course_id=None):
    """
    convert *.xml.tar.gz to course axis
    This could be improved to use the python tar & gzip libraries.
    """
    fnabs = os.path.abspath(fndir)
    tdir = tempfile.mkdtemp()
    cmd = "cd %s; tar xzf %s" % (tdir, fnabs)
    print "running %s" % cmd
    os.system(cmd)
    newfn = glob.glob('%s/*' % tdir)[0]
    print "Using %s as the course xml directory" % newfn
    process_course(
        newfn,
        use_dataset_latest=use_dataset_latest,
        force_course_id=force_course_id)
    print "removing temporary files %s" % tdir
    os.system('rm -rf %s' % tdir)

def main():
    """
    Take actions based on command-line arguments.
    """
    global DATADIR
    global DO_SAVE_TO_MONGO

    if sys.argv[1] == '-mongo':
        DO_SAVE_TO_MONGO = True
        print "============================================================ Enabling Save to Mongo"
        sys.argv.pop(1)

    if sys.argv[1] == '-datadir':
        sys.argv.pop(1)
        DATADIR = sys.argv[1]
        sys.argv.pop(1)
        print "==> using %s as DATADIR" % DATADIR

    if not os.path.exists(DATADIR):
        os.mkdir(DATADIR)
    for filename in sys.argv[1:]:
        if os.path.isdir(filename):
            process_course(filename)
        else:
            # not a directory - is it a tar.gz file?
            if filename.endswith('.tar.gz') or filename.endswith('.tgz'):
                process_xml_tar_gz_file(filename)

if __name__ == '__main__':
    main()
