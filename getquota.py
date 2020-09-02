#!/usr/bin/env python2
import os
import sys
import subprocess
import time
import shlex
import fcntl
from threading import Timer
import pwd
import grp
import getpass
import re

user_quotas_clusters = ['farnam', 'ruddle', 'milgram', 'grace']

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
        query += " 'uidNumber={1}'".format(cluster, uid)
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


def parse_quota_line(line, details, cluster):

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
    data = [fileset, name, quota_type, int(split[10])/1024/1024+int(split[13])/1024/1024, int(split[11])/1024/1024,
            int(split[15])+int(split[18]), int(split[16])]

    return fileset, name, data


def place_output(output, section, cluster, fileset):
    if 'home' in fileset:
        #if (('omega' in fileset and cluster != 'omega') or
        #        ('grace' in fileset and cluster != 'grace')):
        #    pass
        #else:
            output[0] = section

    elif 'project' in fileset:
        output[1] = section

    # scratch60
    elif 'scratch60' in fileset:
        if cluster == 'milgram':
            output[1] = section
        else:
            output[2] = section


def is_pi_fileset(fileset, section=None):
    if section is not None and 'FILESET' not in section:
        return False

    if 'pi' in fileset:
            return True
    elif 'scratch60' in fileset or 'home' in fileset or 'project' in fileset:
        return False
    elif 'apps' in fileset:
        return False
    else:
        return True


def validate_filesets(filesets, cluster, group, all_filesets):

    if cluster in ['farnam', 'ruddle', 'grace']:
        if 'project' not in filesets:
            filesets.append('project')
    if 'scratch60' not in filesets:
        filesets.append('scratch60')

    for fileset in all_filesets.keys():
        if group in fileset and fileset not in filesets:
            filesets.append(fileset)


def format_for_details(data):

    # fileset, user, bytes, file count
    return '{0:14}{1:6}{2:10}{3:14,}'.format(data[0], data[1],
                                             data[3], data[5])


def format_for_summary(data, cluster):

    backup = 'No'
    purge = 'No'

    fileset = data[1]

    if 'home' in fileset or cluster == 'milgram':
        backup = 'Yes'

    if 'scratch60' in fileset:
        purge = '60 days'

    # fileset, userid, quota_type, bytes, byte quota, file count, file limit
    return '{0:14}{1:8}{2:12}{3:12}{4:14,}{5:14,} {6:10}{7:10}'.format(data[0], data[2],
                                                                       data[3], data[4], 
                                                                       data[5], data[6],
                                                                       backup, purge)


def check_limits(summary_data):

    at_limit = {'byte': False,
                 'file': False}
    
    # if you can, avoid the possiblity of dividing by zero
    if summary_data[4] == 0:
        return at_limit
    if summary_data[6] == 0:
        return at_limit
    
    if (summary_data[4]-summary_data[3])/float(summary_data[4]) <= 0.05:
        at_limit['byte'] = True
    if (summary_data[6]-summary_data[5])/float(summary_data[6]) <= 0.05:
        at_limit['file'] = True

    return at_limit


def limits_warnings(summary_data):

    at_limit = check_limits(summary_data)
    warnings = []

    if at_limit['byte']:
        warnings.append("Warning!!! You are at or near your storage limit in the %s fileset. "
                        "Reduce your storage usage to avoid issues." % summary_data[0])
    # file limit
    if at_limit['file']:
        warnings.append("Warning!!! You are at or near your file count limit in the %s fileset. "
                         "Reduce the number of files to avoid issues." % summary_data[0])
    return warnings


def read_usage_file(filesystems, this_user, group_members, cluster):

    usage_data = {}
    user_filesets = set()
    all_filesets = {}

    for filesystem in filesystems:
        filename = filesystem + '/.mmrepquota/current'

        with open(filename, 'r') as f:
            f.readline()
            for line in f:

                if 'USR' not in line or 'root' in line:
                    continue

                fileset, user, user_data = parse_quota_line(line, True, cluster)

                if fileset not in usage_data.keys():
                    usage_data[fileset] = {}

                usage_data[fileset][user] = user_data

                if user == this_user or (this_user is None and user in group_members):
                    user_filesets.add(fileset)

                if fileset not in all_filesets.keys():
                    all_filesets[fileset] = filesystem

    return usage_data, list(user_filesets), all_filesets


def compile_usage_details(filesets, group_members, cluster, data):

    if cluster == 'milgram':
        output = ['', '']
    else:
        output = ['', '', '']

    for fileset in sorted(filesets):
        section = []

        if is_pi_fileset(fileset):
            for user in sorted(data[fileset].keys()):
                section.append(format_for_details(data[fileset][user]))
            output.append('\n'.join(section))

        else:
            for group_member in sorted(group_members):
                if group_member not in data[fileset].keys():
                    section.append(format_for_details([fileset, group_member, 0, 0, 0, 0]))
                else:
                    section.append(format_for_details(data[fileset][group_member]))

        place_output(output, '\n'.join(section), cluster, fileset)

    # don't show home data
    output.pop(0)

    return '\n----\n'.join(output)

# try to end something cleanly, ..for whatever reason
def kill_cmd(cmd):
    try:
        cmd.stdout.close()
        cmd.stderr.close()
        cmd.kill()
    except:
        pass

# try to read something, ..but don't assume that you can
def nonblocking_read(output):
    try:
        fd = output.fileno()
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        out= output.read()
        if out !=None:
            return out
        else:
            return b''
    except:
        return b''

# run something, but discard any errors it may generate and give it a 4-second deadline to complete 
def external_program_filter(cmd):
    timeout = 4
    result = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    timer = Timer(timeout, kill_cmd, [result])
    timer.start()
    command_output = ''
    while result.poll() is None :
        time.sleep(0.5)
        command_output+= str(nonblocking_read(result.stdout).decode("utf-8"))
    timer.cancel()
    return (command_output)

# try to collect and return live quota data or return nothing. This function calls the expensive external
# command mmgetquota several times, ..which is sub-optimal for a number of reasons. Pobody's nerfect, so I'm not 
# going to bother to address this, but if anyone has some free time on their hands then improving this function would 
# be an enjoyable way to spend some of it.
def live_quota_data(devices, filesystems, filesets, all_filesets, user, group, cluster):
    quota_script = '/usr/lpp/mmfs/bin/mmlsquota'
    if cluster == 'milgram':
        output = ['', '']
    else:
        output = ['', '', '']
    for device, filesystem in zip(devices, filesystems):
        query = '{0} -eg {1} -Y --block-size auto {2}'.format(quota_script, group, device)
        result = external_program_filter(query)
        # user based home quotas
        if cluster in user_quotas_clusters and device not in ['slayman']:
            query = '{0} -eu {1} -Y --block-size auto {2} '.format(quota_script, user, device)
            result += external_program_filter(query)
        # make sure that result holds valid data
        if not re.match("^mmlsq", result):
          result = None

        for quota in result.split('\n'):
            if 'HEADER' in quota or 'root' in quota or len(quota) < 10:
                continue
            if ( 'USR' in quota and not 'home' in quota ):
                continue
            fileset, _, section = parse_quota_line(quota, False, cluster)
            place_output(output, section, cluster, fileset)
        for fileset in filesets:
            # query all the pi filesets
            if is_pi_fileset(fileset):
                # check if this fileset is on this device
                if all_filesets[fileset] == filesystem:
                    query = '{0} -ej {1} -Y --block-size auto {2}'.format(quota_script, fileset, device)
                    pi_quota = external_program_filter(query)
                    output.append(parse_quota_line(pi_quota.split('\n')[1], False, cluster)[-1])
    return output


def cached_quota_data(filesystems, filesets, group, user, cluster):

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
                    if cluster not in user_quotas_clusters or user is None:
                        continue

                fileset, name, section = parse_quota_line(line, False, cluster)

                if fileset in filesets:
                    if 'home' in fileset and cluster in user_quotas_clusters:
                        if 'USR' in line and name == user:
                            place_output(output, section, cluster, fileset)
                        continue

                    if name == group:
                        place_output(output, section, cluster, fileset)

                    elif is_pi_fileset(fileset, section=section):
                        output.append(section)

    return output


def print_output(details_data, summary_data, group_name, timestamp, is_me, cluster):

    header = "This script shows information about your quotas on the current gpfs filesystem.\n"
    header += "If you plan to poll this sort of information extensively, please contact us\n"
    header += "for help at hpc@yale.edu\n\n"

    header += '## Usage Details for {0} (as of {1})\n'.format(group_name, timestamp)
    header += '{0:14}{1:6}{2:10}{3:14}\n'.format('Fileset', 'User', 'Usage (GiB)', ' File Count')
    header += '{0:14}{1:6}{2:10}{3:14}'.format('-'*13, '-'*5, '-'*10, ' '+'-'*13)

    print(header)
    print(details_data)

    if is_me:
        time = 'right now'
    else:
        time = timestamp

    header = '\n## Quota Summary for {0} (as of {1})\n'.format(group_name, time)
    header += '{0:14}{1:8}{2:12}{3:12}{4:14}{5:14}{6:10}{7:10}\n'.format('Fileset', 'Type', 'Usage (GiB)',
                                                             ' Quota (GiB)', ' File Count', ' File Limit', ' Backup', ' Purged')
    header += '{0:14}{1:8}{2:12}{3:12}{4:14}{5:14}{6:10}{7:10}'.format('-'*13, '-'*7, '-'*12,
                                                           ' '+'-'*11, ' '+'-'*13, ' '+'-'*13,
                                                           ' '+'-'*9, ' '+'-'*9)

    print(header)
    for summary in summary_data:
        if summary:
            print(format_for_summary(summary, cluster))

    warnings = []
    for summary in summary_data:
        if summary:
            warnings += limits_warnings(summary)

    if len(warnings):
        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print('\n'.join(warnings))
        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!')


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
    usage_data, filesets, all_filesets = read_usage_file(filesystems[cluster], user, group_members, cluster)
    validate_filesets(filesets, cluster, group_name, all_filesets)

    details_data = compile_usage_details(filesets, group_members, cluster, usage_data)

    # quota summary
    if is_me:
        try:
            summary_data = live_quota_data(devices[cluster], filesystems[cluster], filesets, all_filesets, user, group_id, cluster)
        except:
            summary_data = cached_quota_data(filesystems[cluster], filesets, group_name, user, cluster)
            is_me = False
    else:
        summary_data = cached_quota_data(filesystems[cluster], filesets, group_name, user, cluster)

    print_output(details_data, summary_data, group_name, timestamp, is_me, cluster)
