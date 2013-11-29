# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

#!/usr/bin/python
#
# File:   edx2course_axis.py
#
# From an edX xml file set, generate:
#
#   course_id, index, url_name,  category, gformat, start, due, name, path, module_id, data
#
# course_id = edX standard {org}/{course_num}/{semester}
# index     = integer giving temporal order of course material
# url_name  = unique key for item
# category  = (known as a "tag" in some edX docs) chapter, sequential, vertical, problem, video, html, ...
# gformat   = "grading format", ie assignment name
# start     = start date
# due       = end date
# name      = full (display) name of item
# path      = path with url_name's to this item from course root, ie chapter/sequential/position
# module_id = edX standard {org}/{course_num}/{category}/{url_name} id for an x-module
# data      = extra data for element, eg you-tube id's for videos
# chapter_mid = module_id of the chapter within which this x-module exists (empty if not within a chapter)
# 
#
# usage:   python edx2course_axis.py COURSE_DIR

# requires BeautifulSoup and path.py to be installed

# <codecell>

import os, sys, string, re
import csv
import codecs
import json
import glob
import datetime
import xbundle
from collections import namedtuple
from lxml import etree
from path import path
from fix_unicode import fix_bad_unicode

DO_SAVE_TO_MONGO = False
DATADIR = "DATA"

VERBOSE_WARNINGS = True
#FORCE_NO_HIDE = True
FORCE_NO_HIDE = False

# <codecell>

#-----------------------------------------------------------------------------

# storage class for each axis element
Axel = namedtuple('Axel', 'course_id index url_name category gformat start due name path module_id data chapter_mid')

class Policy(object):
    '''
    Handle policy.json for edX course.  Also deals with grading_policy.json if present in same directory.
    '''
    policy = None
    grading_policy = None
    InheritedSettings = ['format', 'hide_from_toc', 'start', 'due']

    def __init__(self, pfn):
        '''
        pfn = policy file name
        '''
        self.pfn = path(pfn)
        print "loading policy file %s" % pfn
        self.policy = json.loads(open(pfn).read())
        
        gfn = self.pfn.dirname() / 'grading_policy.json'
        if os.path.exists(gfn):
            self.gfn = gfn
            self.grading_policy = json.loads(open(gfn).read())
            
    @property
    def semester(self):
        # print "semester: policy keys = %s" % self.policy.keys()
        semester = [t[1] for t in [k.split('/',1) for k in self.policy.keys()] if t[0]=='course'][0]
        return semester

    def get_metadata(self, xml, setting, default=None, parent=False):
        '''
        Retrieve policy for xml element, given the policy JSON and, for a specific setting.
        Handles inheritance of certain settings (like format and hide_from_toc)

        xml = etree
        setting = string
        '''
        if parent:
            val = xml.get(setting, None)
            if val is not None and not val=='null' and ((setting in self.InheritedSettings) and not val==""):
                return val
        
        un = xml.get('url_name', xml.get('url_name_orig', '<no_url_name>'))
        pkey = '%s/%s' % (xml.tag, un)
        
        if pkey in self.policy and setting in self.policy[pkey]:
            # print " using self.policy for %s" % setting
            return self.policy[pkey][setting]
        
        if not setting in self.InheritedSettings:
            return default

        parent = xml.getparent()	# inherited metadata: try parent
        if parent is not None:
            # print "  using parent %s for policy %s" % (parent, setting)
            return self.get_metadata(parent, setting, default, parent=True)

# <codecell>

#-----------------------------------------------------------------------------

def get_from_parent(xml, attr, default):
    '''
    get attribute from parent, recursing until end or found
    '''
    parent = xml.getparent()
    if parent is not None:
        v = parent.get(attr,None)
        if v is not None:
            return v
        return get_from_parent(parent, attr, default)
    return default
        

def date_parse(datestr, retbad=False):
    if not datestr:
        return None

    formats = ['%Y-%m-%dT%H:%M:%S.%f',    	# 2012-12-04T13:48:28.427430
               '%Y-%m-%dT%H:%M:%S',
               '%Y-%m-%dT%H:%M',		# 2013-02-12T19:00
               '%B %d, %Y',			# February 25, 2013
               '%B %d, %H:%M, %Y', 		# December 12, 22:00, 2012
               '%B %d, %Y, %H:%M', 		# March 25, 2013, 22:00
               '%B %d %Y, %H:%M',		# January 2 2013, 22:00
               '%B %d %Y', 			# March 13 2014
               '%B %d %H:%M, %Y',		# December 24 05:00, 2012
               ]

    for fmt in formats:
        try:
            dt = datetime.datetime.strptime(datestr,fmt)
            return dt
        except Exception as err:
            continue

    print "Date %s unparsable" % datestr
    if retbad:
        return "Bad"
    return None

# <codecell>

#-----------------------------------------------------------------------------

class CourseInfo(object):
    def __init__(self, fn, policyfn='', dir=''):
        cxml = etree.parse(fn).getroot()
        self.cxml = cxml
        self.org = cxml.get('org')
        self.course = cxml.get('course')
        self.url_name = cxml.get('url_name')
        if policyfn:
            self.load_policy(policyfn)
        else:
            pfn = dir / 'policies' / self.url_name + ".json"
            if not os.path.exists(pfn):
                pfn = dir / 'policies' / self.url_name / "policy.json"
            if os.path.exists(pfn):
                self.load_policy(pfn)
            else:
                print "==================== ERROR!  Missing policy file %s" % pfn

    def load_policy(self, pfn):
        self.policy = Policy(pfn)
        if self.url_name is None:
            self.url_name = self.policy.semester


#-----------------------------------------------------------------------------

        
def make_axis(dir):
    '''
    return dict of {course_id : { policy, xbundle, axis (as list of Axel elements) }}
    '''
    
    courses = []

    dir = path(dir)

    if os.path.exists(dir / 'roots'):	# if roots directory exists, use that for different course versions
        # get roots
        roots = glob.glob(dir / 'roots/*.xml')
        courses = [ CourseInfo(fn, '', dir) for fn in roots ]

    else:	# single course.xml file - use differnt policy files in policy directory, though

        fn = dir / 'course.xml'
    
        # get semesters
        policies = glob.glob(dir/'policies/*.json')
        assetsfn = dir / 'policies/assets.json'
        if str(assetsfn) in policies:
            policies.remove(assetsfn)
        if not policies:
            policies = glob.glob(dir/'policies/*/policy.json')
        if not policies:
            print "Error: no policy files found!"
        
        courses = [ CourseInfo(fn, pfn) for pfn in policies ]


    print "%d course runs found: %s" % (len(courses), [c.url_name for c in courses])
    
    ret = {}

    # construct axis for each policy
    for cinfo in courses:
        policy = cinfo.policy
        semester = policy.semester
        org = cinfo.org
        course = cinfo.course
        cid = '%s/%s/%s' % (org, course, semester)
        print cid
    
        cfn = dir / ('course/%s.xml' % semester)
        
        # generate XBundle for course
        xml = etree.parse(cfn).getroot()
        xb = xbundle.XBundle(keep_urls=True, skip_hidden=True, keep_studio_urls=True)
        xb.policy = policy.policy
        cxml = xb.import_xml_removing_descriptor(dir, xml)

        # append metadata
        metadata = etree.Element('metadata')
        cxml.append(metadata)
        policy_xml = etree.Element('policy')
        metadata.append(policy_xml)
        policy_xml.text = json.dumps(policy.policy)
        grading_policy_xml = etree.Element('grading_policy')
        metadata.append(grading_policy_xml)
        grading_policy_xml.text = json.dumps(policy.grading_policy)
    
        bundle = etree.tostring(cxml, pretty_print=True)
        #print bundle[:500]
        index = [1]
        caxis = []
    
        def walk(x, seq_num=1, path=[], seq_type=None, parent_start=None, parent=None, chapter=None):
            '''
            Recursively traverse course tree.  
            
            x        = current etree element
            seq_num  = sequence of current element in its parent, starting from 1
            path     = list of url_name's to current element, following edX's hierarchy conventions
            seq_type = problemset, sequential, or videosequence
            parent_start = start date of parent of current etree element
            parent   = parent module
            chapter  = the last chapter module_id seen while walking through the tree
            '''
            url_name = x.get('url_name',x.get('url_name_orig',''))
            if not url_name:
                dn = x.get('display_name')
                if dn is not None:
                    url_name = dn.strip().replace(' ','_')     # 2012 convention for converting display_name to url_name
                    url_name = url_name.replace(':','_')
                    url_name = url_name.replace('.','_')
                    url_name = url_name.replace('(','_').replace(')','_').replace('__','_')
            
            data = None
            start = None

            if not FORCE_NO_HIDE:
                hide = policy.get_metadata(x, 'hide_from_toc')
                if hide is not None and not hide=="false":
                    print '[edx2course_axis] Skipping %s (%s), it has hide_from_toc=%s' % (x.tag, x.get('display_name','<noname>'), hide)
                    return

            if x.tag=='video':	# special: for video, let data = youtube ID(s)
                data = x.get('youtube','')
                if data:
                    # old ytid format - extract just the 1.0 part of this 
                    # 0.75:JdL1Vo0Hru0,1.0:lbaG3uiQ6IY,1.25:Lrj0G8RWHKw,1.50:54fs3-WxqLs
                    ytid = data.replace(' ','').split(',')
                    ytid = [z[1] for z in [y.split(':') for y in ytid] if z[0]=='1.0']
                    print "   ytid: %s -> %s" % (x.get('youtube',''), ytid)
                    if ytid:
                        data = ytid
                if not data:
                    data = x.get('youtube_id_1_0', '')
                if data:
                    data = '{"ytid": "%s"}' % data

            if x.tag=='problem' and x.get('weight') is not None and x.get('weight'):
                data = "{'weight': %s}" % x.get('weight')
                
            if x.tag=='html':
                iframe = x.find('.//iframe')
                if iframe is not None:
                    print "   found iframe in html %s" % url_name
                    src = iframe.get('src','')
                    if 'https://www.youtube.com/embed/' in src:
                        m = re.search('embed/([^"/?]+)', src)
                        if m:
                            data = '{"ytid": "%s"}' % m.group(1)
                            print "    data=%s" % data
                
            if url_name:              # url_name is mandatory if we are to do anything with this element
                # url_name = url_name.replace(':','_')
                dn = x.get('display_name', url_name)
                try:
                    #dn = dn.decode('utf-8')
                    dn = unicode(dn)
                    dn = fix_bad_unicode(dn)
                except Exception as err:
                    print 'unicode error, type(dn)=%s'  % type(dn)
                    raise
                pdn = policy.get_metadata(x, 'display_name')      # policy display_name - if given, let that override default
                if pdn is not None:
                    dn = pdn

                #start = date_parse(x.get('start', policy.get_metadata(x, 'start', '')))
                start = date_parse(policy.get_metadata(x, 'start', '', parent=True))
                
                if parent_start is not None and start < parent_start:
                    print "Warning: start of %s element %s happens before start %s of parent: using parent start" % (start, x, parent_start)
                    start = parent_start
                #print "start for %s = %s" % (x, start)
                
                # drop bad due date strings
                if date_parse(x.get('due',None), retbad=True)=='Bad':
                    x.set('due', '')

                due = date_parse(policy.get_metadata(x, 'due', '', parent=True))
                if x.tag=="problem":
                    print "    setting problem due date: for %s due=%s" % (url_name, due)

                gformat = x.get('format', policy.get_metadata(x, 'format', ''))
                if not gformat:
                    gformat = get_from_parent(x, 'format', '')

                # compute path
                # The hierarchy goes: `course > chapter > (problemset | sequential | videosequence)`
                if x.tag=='chapter':
                    path = [url_name]
                elif x.tag in ['problemset', 'sequential', 'videosequence']:
                    seq_type = x.tag
                    path = [path[0], url_name]
                else:
                    path = path[:] + [str(seq_num)]      # note arrays are passed by reference, so copy, don't modify
                    
                # compute module_id
                if x.tag=='html':
                    module_id = '%s/%s/%s/%s' % (org, course, seq_type, '/'.join(path[1:3]))  # module_id which appears in tracking log
                else:
                    module_id = '%s/%s/%s/%s' % (org, course, x.tag, url_name)
                
                # done with getting all info for this axis element; save it
                path_str = '/' + '/'.join(path)
                ae = Axel(cid, index[0], url_name, x.tag, gformat, start, due, dn, path_str, module_id, data, chapter)
                caxis.append(ae)
                index[0] += 1
            else:
                if VERBOSE_WARNINGS:
                    print "Missing url_name for element %s (attrib=%s, parent_tag=%s)" % (x, x.attrib, (parent.tag if parent is not None else ''))

            # chapter?
            if x.tag=='chapter':
                the_chapter = module_id
            else:
                the_chapter = chapter

            # done processing this element, now process all its children
            if not x.tag in ['html', 'problem', 'discussion', 'customtag', 'poll_question']:
                inherit_seq_num = (x.tag=='vertical' and not url_name)    # if <vertical> with no url_name then keep seq_num for children
                if not inherit_seq_num:
                    seq_num = 1
                for y in x:
                    walk(y, seq_num, path, seq_type, parent_start=start, parent=x, chapter=the_chapter)
                    if not inherit_seq_num:
                        seq_num += 1
                
        walk(cxml)
        ret[cid] = dict(policy=policy.policy, bundle=bundle, axis=caxis, grading_policy=policy.grading_policy)
    
    return ret

# <codecell>

def save_data_to_mongo(cid, cdat, caset):
    '''
    Save course axis data to mongo
    
    cid = course_id
    cdat = course axis data
    caset = list of course axis data in dict format
    '''
    import save_to_mongo
    save_to_mongo.do_save(cid, caset)

# <codecell>

#-----------------------------------------------------------------------------

def process_course(dir):
    ret = make_axis(dir)

    # save data as csv and txt: loop through each course (multiple policies can exist withing a given course dir)
    for cid, cdat in ret.iteritems():

        # write out xbundle to xml file
        bfn = '%s/xbundle_%s.xml' % (DATADIR, cid.replace('/','_'))
        codecs.open(bfn,'w',encoding='utf8').write(ret[cid]['bundle'])
        
        # clean up xml file with xmllint if available
        if os.system('which xmllint')==0:
            os.system('xmllint --format %s > %s.new' % (bfn, bfn))
            os.system('mv %s.new %s' % (bfn, bfn))

        print "saving data for %s" % cid

        header = ("index", "url_name", "category", "gformat", "start", 'due', "name", "path", "module_id", "data", "chapter_mid")
        caset = [{ x: getattr(ae,x) for x in header } for ae in cdat['axis']]

        # optional save to mongodb
        if DO_SAVE_TO_MONGO:
            save_data_to_mongo(cid, cdat, caset)
        
        # print out to text file
        afp = codecs.open('%s/axis_%s.txt' % (DATADIR, cid.replace('/','_')),'w', encoding='utf8')
        aformat = "%8s\t%40s\t%24s\t%16s\t%16s\t%16s\t%s\t%s\t%s\t%s\t%s\n"
        afp.write(aformat % header)
        afp.write(aformat % tuple(["--------"] *11))
        for ca in caset:
            afp.write(aformat % tuple([ca[x] for x in header]))
        afp.close()
        
        # save as csv file
        csvfn = '%s/axis_%s.csv' % (DATADIR, cid.replace('/','_'))
        fp = open(csvfn, 'wb')
        writer = csv.writer(fp, dialect="excel", quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(header)
        for ca in caset:
            try:
                data = [ ('%s' % ca[k]).encode('utf8') for k in header]
                writer.writerow(data)
            except Exception as err:
                print "Failed to write row %s" % data
                print "Error=%s" % err
        fp.close()
        print "Saved course axis to %s" % csvfn

# <codecell>

if __name__=='__main__':
    if sys.argv[1]=='-mongo':
        DO_SAVE_TO_MONGO = True
        print "============================================================ Enabling Save to Mongo"
        sys.argv.pop(1)
    for dir in sys.argv[1:]:
        process_course(dir)

