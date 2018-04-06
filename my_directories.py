#!/usr/bin/env python2

import os

home = os.environ['HOME']

metagroup, group, user = home.split('/')[-3:]

print "Full directory paths for {}".format(user)
print "These directories are available from both Grace and Omega\n"

print 'Omega'
print '====='
print'{0:9} /gpfs/loomis/home.omega/{0}/{1}/{2}'.format('home', metagroup, group, user)
print '{0:9} /gpfs/loomis/scratch.omega/{0}/{1}/{2}'.format('scratch', metagroup, group, user)
print ' '
print 'Grace'
print '====='
print '{0:9} /gpfs/loomis/home.grace/{1}/{2}/{3}'.format('home', metagroup, group, user)
print '{0:9} /gpfs/loomis/project/{1}/{2}/{3}'.format('project', metagroup, group, user)
print '{0:9} /gpfs/loomis/scratch60/{1}/{2}/{3}'.format('scratch60', metagroup, group, user)




