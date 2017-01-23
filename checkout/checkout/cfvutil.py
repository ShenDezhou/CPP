#!/usr/bin/python
# -*- coding: gbk -*-   
# vim:set si sw=4 ts=4 et fenc=gbk:

import sys, os, os.path, copy, tempfile, ConfigParser, new, shutil, re, urllib2
import StringIO, new

import logging
import logging.config

VERSION = '0.8.1'
STAGES = ( 'prep', 'build', 'install', 'check', 'pack', 'doxygen' )
OPTIONAL_STAGES = ( 'dist', 'distcheck', 'checkrpms' )

#basepath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
basepath = os.path.join(os.path.dirname(__file__))
conf_filename = os.path.join(basepath, 'cfvutil.conf')

settings = new.module('settings')
execfile(conf_filename, {}, settings.__dict__)

log = logging.getLogger()
options = None

class InvalidConfigFileError(Exception):
    pass
class VersionControlError(Exception):
    pass

class Configuration(object):
    """
        the object describe a configuation of build
    """
    StringAttrs = ( 'cfvname', 'parents', 'parent', 'type', 'virtual', 'build_target', 'build_requires', 'mailto') + \
            STAGES + OPTIONAL_STAGES
            #'dist', 'distcheck', 'onupdate', 'distdir', 'get_distdir', 'distpkg', 'get_distpkg' 
    DictAttrs = ( 'svn', 'cvs', 'wget', 'env' )
    __slots__ = StringAttrs + DictAttrs 
    def __init__(self, **args):
        object.__init__(self)
        for k in args:
            setattr(self, k, args[k])
    def __repr__(self):
        ret = 'Configuration(';
        for k in Configuration.__slots__:
            if hasattr(self, k):
                ret += '\n\t%s = %s, ' % (k, repr(getattr(self,k)))
        ret += ')\n'
        return ret

def read_conf(conffilename):
    """
        read a .ini file and return a dictionary consist of serval Configuration objects
    """
    if not re.match(r'[a-z]+:', conffilename):
        conffilename = 'file:' + conffilename
    iniconf = ConfigParser.RawConfigParser()
    iniconf.optionxform = lambda x : x
    iniconf.readfp(StringIO.StringIO(settings.default_template))
    iniconf.readfp(urllib2.urlopen(conffilename))
    ret={}
    for cfvname in iniconf.sections():
        if cfvname.count('.') != 0: # this is not a confver section
            continue
        cfv = Configuration()
        ret[cfvname] = cfv
        for k in Configuration.DictAttrs:
            if iniconf.has_section(cfvname+'.'+k):
                d = dict(iniconf.items(cfvname+'.'+k))
                setattr(cfv, k, d)
        for k in Configuration.StringAttrs:
            if iniconf.has_option(cfvname, k):
                v = iniconf.get(cfvname, k)
                lines = v.split('\n')
                if len(lines) > 1 and len(lines[0]) == 1:
                    nl = []
                    for l in lines[1:]:
                        if l.startswith(lines[0]): 
                            nl.append(l[1:])
                        else:
                            nl.append(l)
                    setattr(cfv, k, '\n'.join(nl))
                else:
                    setattr(cfv, k, v)
    return ret

def validate_conf(conf, cfvname, childrens = list()):
    """
        验证conf文件不存在循环继承和死链
    """
    if cfvname in childrens:
        raise InvalidConfigFileError("CONFVER %s is child of himself %s" % (repr(cfvname), childrens))

    if not cfvname in conf:
        raise InvalidConfigFileError("CONFVER %s is not exist, its children are %s" % (repr(cfvname), childrens))
    
    cfv = conf[cfvname]

    if hasattr(cfv, 'parent'):
        parent = cfv.parent
        validate_conf(conf, parent, childrens + [cfvname])
    if hasattr(cfv, 'parents'):
        for parent in re.split(r'[ \t\n,;]+', cfv.parents.strip(' \t\n,;')):
            validate_conf(conf, parent, childrens + [cfvname])

def _do_merge_cfv(cfv, pcfv):
    """
       merge configuation from parent to child 
    """
    newcfv = Configuration()
    for k in Configuration.DictAttrs:
        newone = {}
        if hasattr(pcfv, k):
            newone = copy.copy(getattr(pcfv, k))
        newone.update(getattr(cfv, k, {}))
        if newone:
            setattr(newcfv, k, newone)
    for k in Configuration.StringAttrs:
        newone = None
        if hasattr(pcfv, k):
            newone = getattr(pcfv, k)
        if hasattr(cfv, k):
            newone = getattr(cfv, k)
        if newone is not None:
            setattr(newcfv, k, newone)
    # virtual属性需要特别处理
    if hasattr(cfv, 'virtual'):
        newcfv.virtual = cfv.virtual
    else:
        newcfv.virtual = 0
    return newcfv
    

def _get_merged_cfv(conf, cfvname):
    """
        处理configuration的继承关系
    """
    cfv = conf[cfvname]
    cfv.cfvname = cfvname
    pcfv = None
    if hasattr(cfv, 'parents'):
        parents = re.split(r'[ \t\n,;]+', cfv.parents.strip(' \t\n,;'))
        pcfv = _get_merged_cfv(conf, parents[0])
        for p in parents[1:]:
            tcfv = _get_merged_cfv(conf, p)
            pcfv = _do_merge_cfv(tcfv, pcfv)
        newcfv = _do_merge_cfv(cfv, pcfv)
    elif hasattr(cfv, 'parent'):
        pcfv = _get_merged_cfv(conf, cfv.parent)
        newcfv = _do_merge_cfv(cfv, pcfv)
    elif hasattr(cfv, 'virtual') and cfv.virtual:
        newcfv = _do_merge_cfv(cfv, None)
    else:
        pcfv = conf['__default__']
        newcfv = _do_merge_cfv(cfv, pcfv)
    return newcfv

def merge_cfv(conf):
    n={}
    for cfvname in conf:
        n[cfvname] = _get_merged_cfv(conf, cfvname)
    conf.update(n)

def read_cfv(conffilename, cfvname):
    conf = read_conf(conffilename)
    if (options.debug >= 2):
        print conf

    try:
        validate_conf(conf, cfvname)
    except InvalidConfigFileError, (msg,):
        print >> sys.stderr, "InvalidConfigFileError: %s %s" %(options.conf, msg)
        sys.exit(1)

    cfv = _get_merged_cfv(conf,cfvname)
    if (options.debug >= 1):
        print cfv

    return cfv

def _gen_script_header(cfv, topdir, destdir = None, tracescript = True, exportenv = True, createDir = False, scriptfp = None):
    if not scriptfp:
       scriptfp = tempfile.NamedTemporaryFile()
       log.info("scriptfp = %s", scriptfp.name)
       os.chmod(scriptfp.name, 0555)

    print >> scriptfp, '#!/bin/bash' 
    print >> scriptfp, 'set -e' 
    if tracescript:
        print >> scriptfp, 'set -x' 
    print >> scriptfp, 'umask 0002'
    if createDir:
        print >> scriptfp, 'mkdir -p %s' % (topdir,)
    print >> scriptfp, 'cd %s' % (topdir,)

    if exportenv:
        execenv = {}
        if hasattr(cfv, 'env'):
            execenv.update(cfv.env)
        for e in execenv:
            if not e.startswith('.'):
                print >> scriptfp, "%s=\"%s\"" % (e, execenv[e])
        if '.export' in execenv:
                print >> scriptfp, "export %s" % (execenv['.export'].replace(',', ' '),)

    if destdir:
        print >> scriptfp, "DESTDIR=\"%s\"" % (destdir,)

    return scriptfp

def do_checkout(cfv, topdir):
    scriptfp = _gen_script_header(cfv, topdir = topdir, tracescript = options.debug >= 1, exportenv = False, createDir = True)

    svnflags = ''
    if 'SVNFLAGS' in os.environ:
        svnflags += ' ' + os.environ['SVNFLAGS']
    for d in sorted(cfv.svn):
        url = cfv.svn[d]
        print >> scriptfp, "mkdir -p %s" % (d,)
        print >> scriptfp, 'svn %s checkout -q %s %s' % (svnflags, url, d)
    print >> scriptfp, 'true'

    scriptfp.file.close() # close without del
    if options.debug >= 2:
        os.system('cat '+scriptfp.name)
    r = os.system(scriptfp.name)
    if os.WIFEXITED(r):
        return os.WEXITSTATUS(r)
    elif os.WIFSIGNALED(r):
        return -os.WTERMSIG(r)
    else:
        return 127

def do_update(cfv, topdir):
    scriptfp = _gen_script_header(cfv, topdir = topdir, tracescript = options.debug >= 1, exportenv = False)

    svnflags = ''
    if 'SVNFLAGS' in os.environ:
        svnflags += ' ' + os.environ['SVNFLAGS']
    for d in sorted(cfv.svn):
        url = cfv.svn[d]
        #print >> scriptfp, "mkdir -p %s" % (d,)
        print >> scriptfp, 'svn %s update -q %s' % (svnflags, d)
    print >> scriptfp, 'true'

    scriptfp.file.close() # close without del
    if options.debug >= 2:
        os.system('cat '+scriptfp.name)
    r = os.system(scriptfp.name)
    if os.WIFEXITED(r):
        return os.WEXITSTATUS(r)
    elif os.WIFSIGNALED(r):
        return -os.WTERMSIG(r)
    else:
        return 127

url_with_rev_pattern = re.compile(r'^.*@r?[0-9]+$')
def do_checkupdate(cfv, topdir):
    scriptfp = _gen_script_header(cfv, topdir = topdir, tracescript = options.debug >= 1, exportenv = False)
    
    svnflags = ''
    if 'SVNFLAGS' in os.environ:
        svnflags += ' ' + os.environ['SVNFLAGS']
    for d in sorted(cfv.svn):
        url = cfv.svn[d]
        print >> scriptfp, """[ -d %s ] || { echo "directory %s is not exist" 1>&2; exit 119; } """ % (d,d)
        print >> scriptfp, """[ -d %s/.svn ] || { echo "directory %s is not in svn" 1>&2; exit 119; } """ % (d,d)
        print >> scriptfp, """oldv=$(svn %s info %s | awk '/^Last Changed Rev: [0-9]*$/ {print $4}' )""" % (svnflags, d)
        print >> scriptfp, """[ -n "$oldv" ]"""
        if options.verbose:
            print >> scriptfp, """echo "svn:%s=%s is at r$oldv" """ % (d, url)
        if url_with_rev_pattern.match(url):
            print >> scriptfp, """# %s is fixed revisison """ % (url)
        else:
            print >> scriptfp, """newv=$(svn %s info %s -r HEAD | awk '/^Last Changed Rev: [0-9]*$/ {print $4}' )""" % (svnflags, url)
            print >> scriptfp, """[ -n "$newv" ]"""
            print >> scriptfp, """[ "$oldv" = "$newv" ] || { echo "svn:%s=%s changed from $oldv to $newv" 1>&2; exit 119; }""" % (d, url)
    
    print >> scriptfp, 'echo nothing changed 1>&2'
    print >> scriptfp, 'true'

    scriptfp.file.close() # close without del
    if options.debug >= 2:
        os.system('cat '+scriptfp.name)
    r = os.system(scriptfp.name)
    if os.WIFEXITED(r):
        return os.WEXITSTATUS(r)
    elif os.WIFSIGNALED(r):
        return -os.WTERMSIG(r)
    else:
        return 127

def do_getversion(cfv, topdir):
    scriptfp = _gen_script_header(cfv, topdir = topdir, tracescript = options.debug >= 1, exportenv = False)
    
    svnflags = ''
    if 'SVNFLAGS' in os.environ:
        svnflags += ' ' + os.environ['SVNFLAGS']
    if '.' in cfv.svn:
        pass
    else:
        raise InvalidConfigFileError("'.' is not under version control")

    print >> scriptfp, """svn info"""
    print >> scriptfp, 'true'

    scriptfp.file.close() # close without del
    if options.debug >= 2:
        os.system('cat '+scriptfp.name)

    vout = os.popen('sh '+scriptfp.name)
    vtext = vout.read()
    r = vout.close()
    if r is None:
        r = 0

    if r == 0:
        m = re.search(r'^Last Changed Rev: ([0-9]*)$', vtext, re.M)
        if m:
            version_str=m.group(1)
            print version_str
        else:
            r = 127

    if os.WIFEXITED(r):
        return os.WEXITSTATUS(r)
    elif os.WIFSIGNALED(r):
        return -os.WTERMSIG(r)
    else:
        return 127

def do_mockjob(cfv, conffilename, topdir, installdir, outputfilename = None, jobname = None, joburl = None, 
        extramessage = None, extramailto = None):
    if not os.path.exists(topdir):
        raise InvalidConfigFileError("topdir %s is not exist" % (topdir))

    if not installdir:
        installdir = topdir + '/_result_'
        if not os.path.exists(installdir):
            os.mkdir(installdir)
    else:
        if not os.path.exists(installdir):
            raise InvalidConfigFileError("installdir %s is not exist" % (installdir))

    topdir = os.path.abspath(topdir)
    installdir = os.path.abspath(installdir)

    # for doxygen
    f2 = open(os.path.join(topdir, 'template.doxygen'), 'w')
    f2.write(settings.doxygen_template)
    f2.close()

    run=[]
    for stage in STAGES:
        s = getattr(cfv, stage, '')
        if s:
            scriptfp = _gen_script_header(cfv, topdir = '/builddir/build', destdir = '/builddir/install', scriptfp = StringIO.StringIO())
            print >> scriptfp, """echo "starting %s" 1>&2""" % (stage,)
            print >> scriptfp, s
            print >> scriptfp, """true"""
            run.append( (stage, scriptfp.getvalue().split('\n')) )

    mkdir = [ '/builddir/build', '/builddir/install' ]
    bind = [ (topdir, 'builddir/build'), (installdir, 'builddir/install') ]
    writein = []
    yuminstall = cfv.build_requires

    f = sys.stdout
    if outputfilename:
        f = open(outputfilename, "w")
    if not jobname:
        jobname = conffilename + ':' + cfv.cfvname
    if not joburl:
        joburl = 'file://' + conffilename

    print >> f, 'jobname = ' + repr(jobname)
    print >> f, 'joburl = ' + repr(joburl)
    print >> f, 'mock_label = ' + repr(cfv.build_target)
    print >> f, 'mkdir = ' + repr(mkdir)
    print >> f, 'bind = ' + repr(bind)
    if writein:
        print >> f, 'writein = ' + repr(writein)
    print >> f, 'yuminstall = ' + repr(yuminstall)
    print >> f, 'run = ' + repr(run)
    print >> f, 'log = ' + repr(os.path.join(installdir, 'buildlog.txt'))
    print >> f, 'retcode = ' + repr(os.path.join(installdir, 'retcode.txt'))
    print >> f, 'lock = ' + repr(os.path.join(installdir, '.mocklock'))
    print >> f, 'mailto = ' + repr(cfv.mailto)
    if extramailto:
        print >> f, 'mailto += ' + repr(extramailto)
    if extramessage:
        print >> f, 'extramessage = ' + repr(extramessage)

def do_doxygen(cfv, topdir, installdir = None):
    if not installdir:
        installdir = topdir + '/_result_'
        if not os.path.exists(installdir):
            os.mkdir(installdir)
    else:
        if not os.path.exists(installdir):
            raise InvalidConfigFileError("installdir %s is not exist" % (installdir))
    print installdir

    topdir = os.path.abspath(topdir)
    installdir = os.path.abspath(installdir)

    # for doxygen
    f2 = open(os.path.join(topdir, 'template.doxygen'), 'w')
    f2.write(settings.doxygen_template)
    f2.close()

    for stage in ('doxygen',):
        s = getattr(cfv, stage, '')
        if s:
            scriptfp = _gen_script_header(cfv, topdir = topdir, destdir = installdir)
            print >> scriptfp, """echo "starting %s" 1>&2""" % (stage,)
            print >> scriptfp, s
            print >> scriptfp, """true"""

            scriptfp.file.close() # close without del
            if options.debug >= 2:
                os.system('cat '+scriptfp.name)
    
            r = os.system(scriptfp.name)
            if r:
                if os.WIFEXITED(r):
                    return os.WEXITSTATUS(r)
                elif os.WIFSIGNALED(r):
                    return -os.WTERMSIG(r)
                else:
                    return 127

def do_build(cfv, topdir, installdir = None):
    if not installdir:
        installdir = topdir + '/_result_'
        if not os.path.exists(installdir):
            os.mkdir(installdir)
    else:
        if not os.path.exists(installdir):
            raise InvalidConfigFileError("installdir %s is not exist" % (installdir))

    topdir = os.path.abspath(topdir)
    installdir = os.path.abspath(installdir)

    # for doxygen
    f2 = open(os.path.join(topdir, 'template.doxygen'), 'w')
    f2.write(settings.doxygen_template)
    f2.close()

    cfv.env['BUILD_REQUIRES'] = cfv.build_requires

    for stage in ('checkrpms',)+STAGES:
        s = getattr(cfv, stage, '')
        if s:
            scriptfp = _gen_script_header(cfv, topdir = topdir, destdir = installdir)
            print >> scriptfp, """echo "starting %s" 1>&2""" % (stage,)
            print >> scriptfp, s
            print >> scriptfp, """true"""

            scriptfp.file.close() # close without del
            if options.debug >= 2:
                os.system('cat '+scriptfp.name)

            r = os.system(scriptfp.name)
            if r:
                if os.WIFEXITED(r):
                    return os.WEXITSTATUS(r)
                elif os.WIFSIGNALED(r):
                    return -os.WTERMSIG(r)
                else:
                    return 127

def do_listcfv(conf, listhidden):
    for k in conf:
        cfv = conf[k]
        if listhidden or \
                hasattr(cfv, 'svn') or \
                hasattr(cfv, 'cvs') or \
                hasattr(cfv, 'wget') or \
                getattr(cfv, 'virtual', 0) == 0:
            try:
                validate_conf(conf, k)
                print k
            except InvalidConfigFileError, (msg,):
                print >> sys.stderr, msg

def do_status(cfv, topdir):
    scriptfp = _gen_script_header(cfv, topdir = topdir, tracescript = options.debug >= 1, exportenv = False)

    print >> scriptfp, "# do_status"
    
    svnflags = ''
    if 'SVNFLAGS' in os.environ:
        svnflags += ' ' + os.environ['SVNFLAGS']
    for d in sorted(cfv.svn):
        url = cfv.svn[d]
        print >> scriptfp, """if [ ! -d %s ]; then""" % (d,)
        print >> scriptfp, """  echo "svn:%s=%s is no exist" 1>&2""" % (d,url)
        print >> scriptfp, """else"""
        print >> scriptfp, """  oldv=$(svn %s info %s | awk '/^Last Changed Rev: [0-9]*$/ {print $4}' )""" % (svnflags, d)
        print >> scriptfp, """  if [ -n "$oldv" ]; then"""
        print >> scriptfp, """    echo "svn:%s=%s is at r$oldv" """ % (d, url)
        print >> scriptfp, """  else"""
        print >> scriptfp, """    echo "svn:%s=%s is broken" """ % (d, url)
        print >> scriptfp, """  fi"""
        print >> scriptfp, """fi"""
    
    print >> scriptfp, 'true'

    scriptfp.file.close() # close without del
    if options.debug >= 2:
        os.system('cat '+scriptfp.name)
    r = os.system(scriptfp.name)
    if os.WIFEXITED(r):
        return os.WEXITSTATUS(r)
    elif os.WIFSIGNALED(r):
        return -os.WTERMSIG(r)
    else:
        return 127



