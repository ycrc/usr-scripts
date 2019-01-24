#!/usr/bin/env python2

import os
import sys
import subprocess
import time

import pwd
import grp
import getpass


def get_args():

    cluster = None

    if len(sys.argv) == 3:
        if sys.argv[1] == '-u':
            user = sys.argv[2]
            is_me = False

        elif sys.argv[1] == '-g':
            user = None
            try:
                group_id = grp.getgrnam(sys.argv[2]).gr_gid
            except:
                sys.exit('Unknown group: '+sys.argv[2])
            is_me = False

        elif sys.argv[1] == '-c':
            cluster = sys.argv[2]
            user = getpass.getuser()
            is_me = True
        else:
            sys.exit("Unknown argument. Use -u <user>, -g <group> or no argument for current user")

    else:
        user = getpass.getuser()
        is_me = True

    if user is not None:
        try:
            group_id = pwd.getpwnam(user).pw_gid
        except:
            sys.exit('Unknown user: '+user)

    return user, group_id, cluster, is_me


def get_cluster():

    with open('/etc/yalehpc', 'r') as f:
        cluster = f.readline().split('=')[1].replace('"', '').rstrip()

    return cluster


def get_netid(uid):

    with open('/etc/yalehpc', 'r') as f:
        f.readline()
        mgt = f.readline().split('=')[1].replace('"', '').rstrip()

    try:
        query = "LDAPTLS_REQCERT=never ldapsearch -xLLL -H ldaps://{0}  -b o=hpc.yale.edu -D".format(mgt)
        query += " cn=client,o=hpc.yale.edu -w hpc@Client"
        query += " '(& ({0}HomeDirectory=*) (uidNumber={1}))'".format(cluster, uid)
        query += " uid | grep '^uid'"
        result = subprocess.check_output([query], shell=True)
        name = result.replace('uid: ', '').rstrip('\n')

    except:
        name = uid

    return name


def get_group_members(group_id, cluster):

    with open('/etc/yalehpc', 'r') as f:
        f.readline()
        mgt = f.readline().split('=')[1].replace('"', '').rstrip()

    query = "LDAPTLS_REQCERT=never ldapsearch -xLLL -H ldaps://{0}  -b o=hpc.yale.edu -D".format(mgt)
    query += " cn=client,o=hpc.yale.edu -w hpc@Client"
    query += " '(& ({0}HomeDirectory=*) (gidNumber={1}))'".format(cluster, group_id)
    query += " uid | grep '^uid'"
    result = subprocess.check_output([query], shell=True)

    group_members = result.replace('uid: ', '').split('\n')

    # remove blank line
    if group_members[-1] == '':
        group_members.pop(-1)

    return group_members


def parse_quota_line(line, usage):

    split = line.split(':')

    # Fileset
    quota_type = split[7]
    if quota_type == 'FILESET':
        # name
        fileset = split[9]
        name = ''
    else:
        # name
        name = split[9]
        fileset = split[-2]

    # blockUsage+blockInDoubt, blockQuota
    # filesUsage+filesInDoubt, filesQuota
    data = [int(split[10])/1024/1024+int(split[13])/1024/1024, int(split[11])/1024/1024,
            int(split[15])+int(split[18]), int(split[16])]

    # if name and name[0].isdigit():
    #     name = get_netid(name)

    if usage:
        output = format_for_usage(fileset, name, data)

    else:
        output = format_for_summary(fileset, quota_type, data)

    return fileset, name, output


def place_output(output, section, cluster, fileset):
    if 'home' in fileset:
        if (('omega' in fileset and cluster != 'omega') or
                ('grace' in fileset and cluster != 'grace')):
            pass
        else:
            output[0] = section

    elif 'scratch.' in fileset or 'project' in fileset:
        if 'omega' in fileset and cluster != 'omega':
            pass
        else:
            output[1] = section

    # scratch60 or scratch on Milgram
    elif 'scratch' in fileset:
        if cluster == 'milgram':
            output[1] = section
        else:
            output[2] = section


def is_pi_fileset(fileset, section=None):
    if section is not None and 'FILESET' not in section:
        return False

    if 'pi' in fileset:
            return True
    elif 'scratch' in fileset or 'home' in fileset or 'project' in fileset:
        return False
    elif 'apps' in fileset:
        return False
    else:
        return True


def validate_filesets(filesets, cluster, group, all_filesets):

    if cluster == 'milgram':
        if 'scratch' not in filesets:
            filesets.append('scratch')
    if cluster in ['farnam', 'ruddle']:
        if 'project' not in filesets:
            filesets.append('project')
    if cluster in ['farmam', 'ruddle', 'grace']:
        if 'scratch60' not in filesets:
            filesets.append('scratch60')

    for fileset in all_filesets.keys():
        if group in fileset and fileset not in filesets:
            filesets.append(fileset)


def format_for_usage(fileset, user, data):

    return '{0:14}{1:6}{2:10}{3:14,}'.format(fileset, user,
                                             data[0], data[2])


def format_for_summary(fileset, quota_type, data):

    return '{0:14}{1:8}{2:12}{3:12}{4:14,}{5:14,}'.format(fileset, quota_type,
                                                          data[0], data[1],
                                                          data[2], data[3])


def read_usage_file(filesystems, this_user, group_members):

    quota_data = {}
    user_filesets = set()
    all_filesets = {}

    for filesystem in filesystems:
        filename = filesystem + '/.mmrepquota/current'

        with open(filename, 'r') as f:
            f.readline()
            for line in f:

                if 'USR' not in line or 'root' in line:
                    continue

                fileset, user, output = parse_quota_line(line, True)

                if fileset not in quota_data.keys():
                    quota_data[fileset] = {}

                quota_data[fileset][user] = output

                if user == this_user or (this_user is None and user in group_members):
                    user_filesets.add(fileset)

                if fileset not in all_filesets.keys():
                    all_filesets[fileset] = filesystem

    return quota_data, list(user_filesets), all_filesets


def compile_usage_output(filesets, group_members, cluster, data):

    if cluster == 'milgram':
        output = ['', '']
    else:
        output = ['', '', '']

    for fileset in sorted(filesets):
        section = []

        if is_pi_fileset(fileset):
            for user in sorted(data[fileset].keys()):
                section.append(data[fileset][user])
            output.append('\n'.join(section))

        else:
            for group_member in sorted(group_members):
                if group_member not in data[fileset].keys():
                    section.append(format_for_usage(fileset, group_member, [0, 0, 0, 0]))
                else:
                    section.append(data[fileset][group_member])

            place_output(output, '\n'.join(section), cluster, fileset)

    return '\n----\n'.join(output)


def live_quota_data(devices, filesystems, filesets, all_filesets, user, group):

    quota_script = '/usr/lpp/mmfs/bin/mmlsquota'
    if cluster == 'milgram':
        output = ['', '']
    else:
        output = ['', '', '']

    for device, filesystem in zip(devices, filesystems):

        query = '{0} -eg {1} -Y --block-size auto {2}'.format(quota_script, group, device)
        result = subprocess.check_output([query], shell=True)

        # add user quotas for LS home directories
        if cluster in ['farnam', 'ruddle'] and device not in ['slayman']:
            query = '{0} -eu {1} -Y --block-size auto {2} | grep home'.format(quota_script, user, device)
            result += subprocess.check_output([query], shell=True)

        for quota in result.split('\n'):
            if 'HEADER' in quota or 'root' in quota or len(quota) < 10:
                continue
            fileset, _, section = parse_quota_line(quota, False)

            place_output(output, section, cluster, fileset)

        for fileset in filesets:
            # query all the pi filesets
            if is_pi_fileset(fileset):
                # check if this fileset is on this device
                if all_filesets[fileset] == filesystem:
                    query = '{0} -ej {1} -Y --block-size auto {2}'.format(quota_script, fileset, device)
                    pi_quota = subprocess.check_output([query], shell=True)
                    output.append(parse_quota_line(pi_quota.split('\n')[1], False)[-1])

    return '\n'.join(output)


def cached_quota_data(filesystems, filesets, group, user):

    if cluster == 'milgram':
        output = ['', '']
    else:
        output = ['', '', '']

    for filesystem in filesystems:

        filename = filesystem + '/.mmrepquota/current'
        with open(filename, 'r') as f:
            f.readline()
            for line in f:

                if 'root' in line:
                    continue
                if 'USR' in line:
                    if cluster not in ['farnam', 'ruddle'] or user is None:
                        continue

                fileset, name, section = parse_quota_line(line, False)

                if fileset in filesets:
                    if fileset == 'home' and cluster in ['farnam', 'ruddle']:
                        if 'USR' in line and name == user:
                            place_output(output, section, cluster, fileset)
                        continue

                    if name == group:
                        place_output(output, section, cluster, fileset)

                    elif is_pi_fileset(fileset, section=section):
                        output.append(section)

    return '\n'.join(output)


def print_output(usage_output, quota_output, group_name, timestamp, is_me):

    header = "This script shows information about your quotas on the current gpfs filesystem.\n"
    header += "If you plan to poll this sort of information extensively, please contact us\n"
    header += "for help at hpc@yale.edu\n\n"

    header += '## Usage Details for {0} (as of {1})\n'.format(group_name, timestamp)
    header += '{0:14}{1:6}{2:10}{3:14}\n'.format('Fileset', 'User', 'Usage (GB)', ' File Count')
    header += '{0:14}{1:6}{2:10}{3:14}'.format('-'*13, '-'*5, '-'*10, ' '+'-'*13)

    print(header)
    print(usage_output)

    if is_me:
        time = 'right now'
    else:
        time = timestamp

    header = '\n## Quota Summary for {0} (as of {1})\n'.format(group_name, time)
    header += '{0:14}{1:8}{2:12}{3:12}{4:14}{5:14}\n'.format('Fileset', 'Type', 'Usage (GB)',
                                                             ' Quota (GB)', ' File Count', ' File Limit')
    header += '{0:14}{1:8}{2:12}{3:12}{4:14}{5:14}'.format('-'*13, '-'*7, '-'*12,
                                                           ' '+'-'*11, ' '+'-'*13, ' '+'-'*13)

    print(header)
    print(quota_output)


if (__name__ == '__main__'):

    user, group_id, cluster, is_me = get_args()
    group_name = grp.getgrgid(group_id).gr_name

    if cluster is None:
        cluster = get_cluster()

    filesystems = {'farnam': ['/gpfs/ysm', '/gpfs/slayman'],
                   'ruddle': ['/gpfs/ycga'],
                   'grace': ['/gpfs/loomis'],
                   'milgram': ['/gpfs/milgram'],
                   'omega': ['/gpfs/loomis']
                   }
    devices = {'farnam': ['ysm-gpfs', 'slayman'],
               'ruddle': ['ycga-gpfs'],
               'milgram': ['milgram'],
               'grace': ['loomis'],
               'omega': ['loomis']}

    # usage details
    timestamp = time.strftime('%b %d %Y %H:%M', time.gmtime(os.path.getmtime(filesystems[cluster][0]
                                                                             + '/.mmrepquota/current')))

    group_members = get_group_members(group_id, cluster)
    usage_data, filesets, all_filesets = read_usage_file(filesystems[cluster], user, group_members)
    validate_filesets(filesets, cluster, group_name, all_filesets)
    usage_output = compile_usage_output(filesets, group_members, cluster, usage_data)

    # quota summary
    if is_me:
        quota_output = live_quota_data(devices[cluster], filesystems[cluster], filesets, all_filesets, user, group_id)
    else:
        quota_output = cached_quota_data(filesystems[cluster], filesets, group_name, user)

    print_output(usage_output, quota_output, group_name, timestamp, is_me)
