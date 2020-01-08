#!/usr/bin/env python2

import os
import sys
import subprocess

import pwd
import grp
import getpass

debug = False

def get_args():
    
    # get user
    user = getpass.getuser()

    # get group
    try:
        group_id = pwd.getpwnam(user).pw_gid
        group_name = grp.getgrgid(group_id).gr_name
    except:
        sys.exit('Unknown user: '+user)

    return user, group_name

def construct_dirs(user, group):

    dirs = {}

    dirs['grace'] = {'home': '/gpfs/loomis/home.grace/{0}/{1}'.format(group, user),
                     'project': '/gpfs/loomis/project/{0}/{1}'.format(group, user),
                     'scratch60': '/gpfs/loomis/scratch60/{0}/{1}'.format(group, user)
                     }

    dirs['farnam'] = {'home': '/gpfs/ysm/home/{0}'.format(user),
                      'project': '/gpfs/ysm/project/{0}/{1}'.format(group, user),
                      'scratch60': '/gpfs/ysm/scratch60/{0}/{1}'.format(group, user)
                      }

    dirs['ruddle']  = {'home': '/gpfs/ycga/home/{0}'.format(user),
                      'project': '/gpfs/ycga/project/{0}/{1}'.format(group, user),
                      'scratch60': '/gpfs/ycga/scratch60/{0}/{1}'.format(group, user)
                      }

    return dirs


def print_output(dirs):

    print "Full directory paths for {}:\n".format(user)

    for cluster in ['grace', 'farnam', 'ruddle']:
    
        if (os.path.exists(dirs[cluster]['home']) or os.path.exists(dirs[cluster]['project']) or
                os.path.exists(dirs[cluster]['scratch60'])):

            print(cluster.title())
            print('=====')
            if os.path.exists(dirs[cluster]['home']):
                print('{0:9} {1}'.format('home', dirs[cluster]['home']))
            if os.path.exists(dirs[cluster]['project']):
                print('{0:9} {1}'.format('project', dirs[cluster]['project']))
            if os.path.exists(dirs[cluster]['scratch60']):
                print('{0:9} {1}'.format('scratch60', dirs[cluster]['scratch60']))
            print(' ')


if (__name__ == '__main__'):

    user, group = get_args()

    dirs = construct_dirs(user, group)

    print_output(dirs)
