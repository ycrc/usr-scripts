#!/usr/bin/env python

import pwd
import getpass
import socket

import subprocess
import os
import time


def get_cluster():

    name = socket.gethostname()

    host = name.split('.')
    if (len(host) < 2):
        idx = 0
    else:
        idx = 1
    return host[idx]


def get_group_members(this_user):

    gid = pwd.getpwnam(this_user).pw_gid

    with open('/etc/yalehpc', 'r') as f:
        f.readline()
        mgt = f.readline().split('=')[1].replace('"', '').rstrip()

    query = "LDAPTLS_REQCERT=never ldapsearch -xLLL -H ldaps://{0}  -b o=hpc.yale.edu -D cn=client,o=hpc.yale.edu -w hpc@Client 'gidNumber={1}' uid | grep '^uid'".format(mgt, gid)
    result = subprocess.check_output([query], shell=True)

    group_members = result.replace('uid: ','').split('\n')

    return group_members[:-1]


def read_usage_file(filename, this_user):

    quota_data = {}
    user_filesets = set()

    with open(filename, 'r') as f:
        header = f.readline()
        for line in f:
            split = line.split(':')
            if split[7] != 'USR':
                continue

            user = split[9]
            fileset = split[24]
            if fileset == 'root' or user == 'root':
                continue

            if fileset not in quota_data.keys():
                quota_data[fileset] = {}
            quota_data[fileset][user] = [int(split[10])/1024/1024,
                                         int(split[15])]

            if user == this_user:
                user_filesets.add(fileset)

    return quota_data, list(user_filesets)

def parse_quota_line(line):

    split = line.split(':')

    quota_type = split[7]
    if quota_type == 'FILESET':
        fileset = split[9]
    else:
        fileset = split[22]

    data = [int(split[10])/1024/1024, int(split[11])/1024/1024,
            int(split[15]), int(split[16])]    

    output = '{0:14}{1:6}{2:12}{3:12}{4:14,}{5:14,}'.format(fileset, quota_type, data[0], data[1],
                                                            data[2], data[3])

    return fileset, output


def generate_usage_output(this_user, filesets, group_members, cluster, data):

    output = ['', '', '']

    for fileset in sorted(filesets):
        section = []

        if 'pi' in fileset:
            for user in sorted(data[fileset].keys()):
                section.append('{0:14}{1:6}{2:10}{3:14,}'.format(fileset, user,
                                                                 data[fileset][user][0],
                                                                 data[fileset][user][1]))
            output.append('\n'.join(section))
        else:
            for group_member in sorted(group_members):
                if group_member not in data[fileset].keys():
                    continue
                section.append('{0:14}{1:6}{2:10}{3:14,}'.format(fileset, group_member,
                                                                 data[fileset][group_member][0],
                                                                 data[fileset][group_member][1]))
            if 'home' in fileset:
                output[0] = '\n'.join(section)
            elif 'scratch.' in fileset or 'project' in fileset:
                output[1] = '\n'.join(section)
            elif 'scratch60' in fileset:
                output[2] = '\n'.join(section)

    return '\n----\n'.join(output)

def fetch_quota_data(device, filesets, user):

    quota_script = '/usr/lpp/mmfs/bin/mmlsquota'
    output = ['', '', '']

    query = '{0} -eg $(id -g) -Y --block-size auto {1}'.format(quota_script, device)
    result = subprocess.check_output([query], shell=True)

    # add user quotas for LS home directories
    if cluster in ['farnam', 'ruddle']:
        query = '{0} -eu $(id -u) -Y --block-size auto {1} | grep home'.format(quota_script, device)
        result += subprocess.check_output([query], shell=True)

    for quota in result.split('\n'):
        if 'HEADER' in quota or 'root' in quota or len(quota) < 10:
            continue
        fileset, section = parse_quota_line(quota)

        if 'home' in fileset:
           output[0] = section

        elif 'scratch.' in fileset or 'project' in fileset:
            output[1] = section

        elif 'scratch60' in fileset:
            output[2] = section

    for fileset in filesets:
        if 'pi' in fileset:
            query = '{0} -ej {1} -Y --block-size auto {2}'.format(quota_script, fileset, device)
            pi_quota = subprocess.check_output([query], shell=True)
            output.append(format_quota(pi_quota.split('\n')[1]))

    return '\n'.join(output)
 

if (__name__ == '__main__'):

    user = getpass.getuser()

    cluster = get_cluster()

    filesystem = {'farnam': '/gpfs/ysm',
                  'grace': '/gpfs/loomis',
                  }
    device = {'farnam': 'ysm-gpfs'}

    quota_filename = filesystem[cluster] + '/.mmrepquota/current'
    timestamp = time.strftime('%b %d %Y %H:%M', time.gmtime(os.path.getmtime(quota_filename)))
    group_members = get_group_members(user)

    usage_data, filesets = read_usage_file(quota_filename, user)

    quota_output = fetch_quota_data(device[cluster], filesets, user)
    usage_output = generate_usage_output(user, filesets, group_members, cluster, usage_data)

    header = "This script shows information about your quotas on the current gpfs filesystem.\n"
    header += "If you plan to poll this sort of information extensively, please use alternate means\n"
    header += "and/or contact us for help at hpc@yale.edu\n\n"
    header += '## Usage Details (as of {})\n'.format(timestamp)
    header += '{0:14}{1:6}{2:10}{3:14}\n'.format('Fileset', 'User', 'Usage (GB)', '  File Count')
    header += '{0:14}{1:6}{2:10}{3:14}'.format('-'*13, '-'*5, '-'*10, ' '+'-'*13)

    print(header)
    print(usage_output)

    header = '\n## Quota Summary (as of right now)\n'
    header += '{0:14}{1:6}{2:12}{3:12}{4:14}{5:14}\n'.format('Fileset', 'Type', ' Usage (GB)', ' Quota (GB)', ' File Count', ' File Limit')
    header += '{0:14}{1:6}{2:12}{3:12}{4:14}{5:14}'.format('-'*13, '-'*6, ' '+'-'*11, ' '+'-'*11, ' '+'-'*13, ' '+'-'*13)

    print(header)
    print(quota_output)

