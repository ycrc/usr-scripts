#!/usr/bin/env python2

import os

home = os.environ['HOME']

metagroup, group, user = home.split('/')[-3:]

print "Full directory paths for {}".format(user)
print "These directories are available from both Grace and Omega\n"

dirs = {}

dirs['omega'] = {'home': '/gpfs/loomis/home.omega/{0}/{1}/{2}'.format(metagroup, group, user),
                        'scratch': '/gpfs/loomis/home.omega/{0}/{1}/{2}'.format(metagroup, group, user)}

dirs['grace'] = {'home': '/gpfs/loomis/home.grace/{0}/{1}/{2}'.format(metagroup, group, user),
                        'project': '/gpfs/loomis/project/{0}/{1}/{2}'.format(metagroup, group, user),
                        'scratch60': '/gpfs/loomis/scratch60/{0}/{1}/{2}'.format(metagroup, group, user)
                        }



if os.path.exists(dirs['omega']['home']) or os.path.exists(dirs['omega']['scratch']):
    print 'Omega'
    print '====='
    if os.path.exists(dirs['omega']['home']):
        print'{0:9} {1}'.format('home', dirs['omega']['home'])
    if os.path.exists(dirs['omega']['scratch']):
        print'{0:9} {1}'.format('scratch', dirs['omega']['scratch'])
    print ' '

if (os.path.exists(dirs['grace']['home']) or os.path.exists(dirs['grace']['project']) or
    os.path.exists(dirs['grace']['scratch60'])):

    print 'Grace'
    print '====='
    if os.path.exists(dirs['grace']['home']):
        print'{0:9} {1}'.format('home', dirs['grace']['home'])
    if os.path.exists(dirs['grace']['project']):
        print'{0:9} {1}'.format('project', dirs['grace']['project'])
    if os.path.exists(dirs['grace']['scratch60']):
        print'{0:9} {1}'.format('scratch60', dirs['grace']['scratch60'])




