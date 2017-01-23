#!/usr/bin/python
# -*- coding: gbk -*-   
# vim:set si sw=4 ts=4 et fenc=gbk:

import logging, sys, optparse, os, os.path
import cfvutil

def parse_args(args):
    """
        返回一个描述命令行参数的对象，具有如下属性: debug, cfvname, conf, topdir
    """
    parser = optparse.OptionParser(usage = "%prog [options] configfile topsrcdir", version=cfvutil.VERSION)
    parser.add_option("-v", "--verbose", help="show more infomation",
        action="count", dest="verbose", default=0)
    parser.add_option("-d", "--debug", help="show debug information",
        action="count", dest="debug", default=0)
    parser.add_option("-c", "--cfv", help="the name of configuration version",
        action="store", dest="cfvname", default="main")
    opts, args = parser.parse_args(list(args)); 

    if len(args) != 2:
        parser.print_usage()
        sys.exit(1)

    opts.conf = args[0]
    opts.topdir = os.path.abspath(args[1])

    return opts

def main(*args):
    """Main executable entry point of mock"""
    options = parse_args(args)
    cfvutil.options = options

    if (options.debug >= 2):
        print options
    
    cfv = cfvutil.read_cfv(options.conf, options.cfvname)

    topdir = options.topdir

    if topdir and not os.path.exists(topdir):
        os.makedirs(topdir)

    r = cfvutil.do_checkout(cfv, topdir)
    return r

if __name__ == "__main__":
    # fix for python 2.4 logging module bug:
    logging.raiseExceptions = 0
    r = main(*sys.argv[1:])
    logging.shutdown()
    sys.exit(r)

