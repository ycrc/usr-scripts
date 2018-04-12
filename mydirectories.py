#!/usr/bin/env python2

import os
import sys
import subprocess


def get_cluster():

    with open('/etc/yalehpc', 'r') as f:
        cluster = f.readline().split('=')[1].replace('"', '').rstrip()

    return cluster


def get_group(home, cluster):
    if cluster in ['grace', 'omega']:
        return home.split('/')[-3:]

    elif cluster == 'farnam':

        with open('/etc/yalehpc', 'r') as f:
            f.readline()
            mgt = f.readline().split('=')[1].replace('"', '').rstrip()

        netid = home.split('/')[-1]

        for cluster in ['grace', 'omega']:

            query = "LDAPTLS_REQCERT=never ldapsearch -xLLL -H ldaps://{0}  -b o=hpc.yale.edu -D".format(mgt)
            query += " cn=client,o=hpc.yale.edu -w hpc@Client"
            query += " '(uid={0})'".format(netid)
            query += " {0}HomeDirectory | grep '^{0}HomeDirectory'".format(cluster)
            result = subprocess.check_output([query], shell=True)

            if len(result) > 0:
                result = result.replace(cluster+'HomeDirectory: ', '').rstrip('\n')
                return result.split('/')[-3:]

        return 0, 0, netid

    else:
        sys.exit('Unknown cluster')


home = os.environ['HOME']

metagroup, group, user = get_group(home, get_cluster())

print "Full directory paths for {}".format(user)
print "Loomis directories are available from all clusters.\n"

dirs = {}

dirs['omega'] = {'home': '/gpfs/loomis/home.omega/{0}/{1}/{2}'.format(metagroup, group, user),
                 'scratch': '/gpfs/loomis/scratch.omega/{0}/{1}/{2}'.format(metagroup, group, user)}

dirs['grace'] = {'home': '/gpfs/loomis/home.grace/{0}/{1}/{2}'.format(metagroup, group, user),
                 'project': '/gpfs/loomis/project/{0}/{1}/{2}'.format(metagroup, group, user),
                 'scratch60': '/gpfs/loomis/scratch60/{0}/{1}/{2}'.format(metagroup, group, user)
                 }

dirs['farnam'] = {'home': '/gpfs/ysm/home/{0}'.format(user),
                  'project': '/gpfs/ysm/project/{0}'.format(user),
                  'scratch60': '/gpfs/ysm/scratch60/{0}'.format(user)
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
    print ' '


if (os.path.exists(dirs['farnam']['home']) or os.path.exists(dirs['farnam']['project']) or
        os.path.exists(dirs['farnam']['scratch60'])):

    print 'Farnam (only accessible from Farnam)'
    print '====='
    if os.path.exists(dirs['farnam']['home']):
        print'{0:9} {1}'.format('home', dirs['farnam']['home'])
    if os.path.exists(dirs['farnam']['project']):
        print'{0:9} {1}'.format('project', dirs['farnam']['project'])
    if os.path.exists(dirs['farnam']['scratch60']):
        print'{0:9} {1}'.format('scratch60', dirs['farnam']['scratch60'])
    print ' '
