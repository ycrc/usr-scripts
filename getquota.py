#!/usr/bin/env python2
import fcntl
import getpass
import grp
import os
import pickle
import json
import pwd
import re
import shlex
import stat
import subprocess
import sys
import time
from datetime import datetime
from threading import Timer

gpfs_device_names = {'/gpfs/ysm': 'ysm-gpfs',
                     '/gpfs/gibbs': 'gibbs',
                     '/gpfs/slayman': 'slayman',
                     '/gpfs/loomis': 'loomis',
                     '/gpfs/milgram': 'milgram',
                     '/gpfs/ycga': 'ycga-gpfs'
                     }

common_filespaces = {'grace': ['home.grace', 'project', 'scratch60'],
                     'mccleary': ['home.mccleary', 'project', 'scratch'],
                     'farnam': ['home', 'project', 'scratch60'],
                     'ruddle': ['home', 'project', 'scratch60'],
                     'milgram': ['home', 'project', 'scratch60']
                     }

#### TO DO #####
""""
- refactor all general "fileset" logic to accomodate home, scratch not on GPFS
- prefix home, project, scratch with filesystem

"""


def get_args():

    global debug
    global active_users_only
    debug = False
    active_users_only = False
    cluster = None
    is_me = True
    print_format = 'cli'

    i = 1

    user = getpass.getuser()
    while i < len(sys.argv):
        if sys.argv[i] == '-d':
            debug = True
            i += 1

        elif sys.argv[i] == '-a':
            active_users_only = True
            i += 1

        elif sys.argv[i] == '-u':
            user = sys.argv[i+1]
            is_me = False
            i += 2

        elif sys.argv[i] == '-g':
            user = None
            try:
                group_id = grp.getgrnam(sys.argv[i+1]).gr_gid
            except:
                sys.exit('Unknown group: '+sys.argv[i+1])
            is_me = False
            i += 2

        elif sys.argv[i] == '-c':
            cluster = sys.argv[i+1]
            user = getpass.getuser()
            is_me = True
            i += 2

        elif sys.argv[i] == '-e':
            is_me = True
            print_format = 'email'
            i += 1

        else:
            sys.exit("Unknown argument. Use -u <user>, -g <group> or no argument for current user")

    if user is not None:
        try:
            group_id = pwd.getpwnam(user).pw_gid
        except:
            sys.exit('Unknown user: '+user)

    return user, group_id, cluster, is_me, print_format


def get_cluster():

    with open('/etc/yalehpc', 'r') as f:
        cluster = f.readline().split('=')[1].replace('"', '').rstrip()

    return cluster


def get_netid(uid):

    with open('/etc/yalehpc', 'r') as f:
        f.readline()
        mgt = f.readline().split('=')[1].replace('"', '').rstrip()

    try:
        query = 'LDAPTLS_REQCERT=never LDAPTLS_CACERTDIR="" ldapsearch'
        query += "-xLLL -H ldaps://{0}  -b o=hpc.yale.edu -D".format(mgt)
        query += " cn=client,o=hpc.yale.edu -w hpc@Client"
        query += " 'uidNumber={1}'".format(cluster, uid)
        query += " uid | grep '^uid'"
        result = subprocess.check_output([query], shell=True)
        name = result.replace('uid: ', '').rstrip('\n')

    except:
        name = uid

    return name


def get_group_members(group_id, cluster):

    global active_users_only

    with open('/etc/yalehpc', 'r') as f:
        f.readline()
        mgt = f.readline().split('=')[1].replace('"', '').rstrip()

    query = "LDAPTLS_REQCERT=never ldapsearch -xLLL -H ldaps://{0} -b o=hpc.yale.edu -D".format(mgt)
    query += " cn=client,o=hpc.yale.edu -w hpc@Client"

    if active_users_only:
        query += " '(& ({0}HomeDirectory=*) (gidNumber={1}))'".format(cluster, group_id)
    else:
        query += " '(gidNumber={0})'".format(group_id)
    query += " uid | grep '^uid'"
    result = subprocess.check_output([query], shell=True)

    group_members = result.replace('uid: ', '').split('\n')

    # remove blank line
    if group_members[-1] == '':
        group_members.pop(-1)

    return group_members

def validate_filesets(filesets, cluster, group, all_filesets):

    # fix for forcing pi filesets to show up for all primary group members
    for fileset in all_filesets.keys():
        if group in fileset and fileset not in filesets:
            filesets.append(fileset)

        if fileset == 'loomis:pi_balou':
            if fileset in filesets:
                filesets.remove(fileset)

def is_pi_fileset(fileset, section=None):
    # only applicable for GPFS
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

def prefix_filesystem(filesystem, fileset):

    if 'gpfs' in filesystem:
        fileset = filesystem.replace('/gpfs/', '')+':'+fileset
    elif 'vast' in filesystem:
        fileset = 'palmer'+':'+fileset

    return fileset

def parse_quota_line(line, details, filesystem):

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

    fileset = prefix_filesystem(filesystem, fileset)

    # blockUsage+blockInDoubt, blockQuota
    # filesUsage+filesInDoubt, filesQuota
    data = [fileset, name, quota_type, int(split[10])/1024/1024+int(split[13])/1024/1024, int(split[12])/1024/1024,
            int(split[15])+int(split[18]), int(split[17])]

    return fileset, name, data


def place_output(output, section, cluster, fileset):
    if 'home' in fileset:
        output[0] = section

    elif 'project' in fileset and 'pi' not in fileset:
        output[1] = section

    # scratch60
    elif 'scratch' in fileset:
        output[2] = section

def format_for_details(data):

    # fileset, user, bytes, file count
    return '{0:23}{1:6}{2:10}{3:14,}'.format(data[0], data[1],
                                             data[3], data[5])


def format_for_summary(data, cluster):

    backup = 'No'
    purge = 'No'

    fileset = data[0]

    if 'home' in fileset or cluster == 'milgram':
        if 'scratch' not in fileset:
            backup = 'Yes'

    if 'scratch' in fileset:
        purge = '60 days'

    # fileset, userid, quota_type, bytes, byte quota, file count, file limit
    return '{0:23}{1:8}{2:12}{3:12}{4:14,}{5:14,} {6:10}{7:10}'.format(data[0], data[2],
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

def read_usage_files(filesystems, this_user, group_members, cluster):
    # collects all usage details for gpfs systems
    usage_details = {}
    # collects list of all filesets and filesets where this_user has data
    user_filesets = set()
    all_filesets = {}

    for filesystem in filesystems:
        if 'gpfs' in filesystem:
            read_usage_file_gpfs(filesystem, this_user, group_members, cluster,
                                 usage_details, user_filesets, all_filesets)            
        elif 'vast' in filesystem:
            pass

    return usage_details, list(user_filesets), all_filesets


def read_usage_file_gpfs(filesystem, this_user, group_members, cluster, usage_details, user_filesets, all_filesets):

    filename = filesystem + '/.mmrepquota/current'

    if not os.path.exists(filename):
        print("%s is not available at the moment" % filesystem)
    else:    
        with open(filename, 'r') as f:
            f.readline()
            for line in f:

                if 'USR' not in line or 'root' in line:
                    continue

                fileset, user, user_data = parse_quota_line(line, True, filesystem)

                if fileset not in usage_details.keys():
                    usage_details[fileset] = {}

                usage_details[fileset][user] = user_data

                if user == this_user or (this_user is None and user in group_members):
                    user_filesets.add(fileset)

                if fileset not in all_filesets.keys():
                    all_filesets[fileset] = filesystem

    return usage_details, user_filesets, all_filesets


def compile_usage_details(filesets, group_members, cluster, data):
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
    for i in range(len(output)-1):
        if len(output[i]) == 0:
            output.pop(i) 

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
        out = output.read()
        if out is not None:
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
    while result.poll() is None:
        time.sleep(0.5)
        command_output += str(nonblocking_read(result.stdout).decode("utf-8"))
    timer.cancel()
    return (command_output)


def localcache_quota_data(user):
    output = ''
    if os.path.isfile('/tmp/.'+user+'gqlc'):
        lcmtime = time.time() - os.stat('/tmp/.'+user+'gqlc')[stat.ST_MTIME]

        if lcmtime > 0 and lcmtime <= 300:
            file = open('/tmp/.%s' % user+'gqlc', 'r')
            output = pickle.load(file)

    return output

def collect_quota_data(filesystems, filesets, all_filesets, user, group_id, cluster, is_live):

    output = ['', '', '']
    group_name = grp.getgrgid(group_id).gr_name
    for filesystem in filesystems:
        if 'gpfs' in filesystem:
            if is_live:
                output = live_quota_data_gpfs(filesystem, filesets, all_filesets, user, group_id, cluster, output)
            else:
                output = cached_quota_data_gpfs(filesystem, filesets, user, group_name, cluster, output)
        elif 'vast' in filesystem:
            # vast doesn't (yet?) return live data so just return cached data
            output = cached_quota_data_vast(filesystem, filesets, user, group_name, cluster, output)

    if is_live:
        file = open('/tmp/.%s' % user+'gqlc', 'w')
        pickle.dump(output, file)
        file.close()

    print(output)

    return output

# try to collect and return live quota data or return nothing. This function calls the expensive external
# command mmgetquota several times, ..which is sub-optimal for a number of reasons. Pobody's nerfect, so I'm not
# going to bother to address this, but if anyone has some free time on their hands then improving this function would
# be an enjoyable way to spend some of it.

def live_quota_data_gpfs(filesystem, filesets, all_filesets, user, group, cluster, output):

    global debug
    quota_script = '/usr/lpp/mmfs/bin/mmlsquota'

    device = gpfs_device_names[filesystem]
    query = '{0} -g {1} -Y --block-size auto {2}'.format(quota_script, group, device)
    if debug:
        result = subprocess.check_output([query], shell=True)
    else:
        result = external_program_filter(query)

    # user based home quotas
    if device not in ['slayman', 'gibbs']:
        query = '{0} -u {1} -Y --block-size auto {2} '.format(quota_script, user, device)
    if debug:
        result += subprocess.check_output([query], shell=True)
    else:
        result += external_program_filter(query)
    # make sure that result holds valid data
    if not re.match("^mmlsq", result):
        result = None

    for quota in result.split('\n'):
        if 'HEADER' in quota or 'root' in quota or len(quota) < 10:
            continue
        if ('USR' in quota and 'home' not in quota):
                continue
        fileset, _, section = parse_quota_line(quota, False, filesystem)
        place_output(output, section, cluster, fileset)
    for fileset in filesets:
        # query all the pi filesets
        if is_pi_fileset(fileset):
            # check if this fileset is on this device
            if all_filesets[fileset] == filesystem:
                fileset_name = re.search('[^:]+?:(.*)', fileset).group(1)
                query = '{0} -j {1} -Y {2}'.format(quota_script, fileset_name, device)
        if debug:
            pi_quota = subprocess.check_output([query], shell=True)
        else:
            pi_quota = external_program_filter(query)
            output.append(parse_quota_line(pi_quota.split('\n')[1], False, filesystem)[-1])

    return output

def cached_quota_data_gpfs(filesystem, filesets, user, group, cluster, output):

    filename = filesystem + '/.mmrepquota/current'

    if not os.path.exists(filename):
         return output

    with open(filename, 'r') as f:
        f.readline()
        for line in f:

            if 'root' in line:
                continue

            fileset, name, section = parse_quota_line(line, False, filesystem)

            if fileset in filesets:
                if 'home' in fileset:
                    if 'USR' in line and name == user:
                        place_output(output, section, cluster, fileset)
                    continue

                if name == group:
                        place_output(output, section, cluster, fileset)

                elif is_pi_fileset(fileset, section=section):
                    output.append(section)

    return output

def cached_quota_data_vast(filesystem, filesets, user, group, cluster, output):

    filename = filesystem + '/.quotas/current'
    if not os.path.exists(filename):
        return output

    with open(filename, 'r') as f:
        all_quota_data = json.load(f)

        for quota in all_quota_data:

            if ':' in quota['name']:
                fileset, name = quota['name'].split(':')
                if name == group:
                    fileset = prefix_filesystem(filesystem, fileset)
                    data = [fileset, name, 'GRP', quota['used_effective_capacity']/1024/1024/1024,
                            quota['hard_limit']/1024/1024/1024, quota['used_inodes'], quota['hard_limit_inodes']]

                    output.append(data)

    return output

def print_cli_output(details_data, summary_data, group_name, timestamp, is_me, cluster):

    header = "This script shows information about your quotas on {0}.\n".format(cluster)
    header += "If you plan to poll this sort of information extensively,\n"
    header += "please contact us for help at hpc@yale.edu\n"

    print(header)

    details_header = '## Usage Details for {0} (as of {1})\n'.format(group_name, timestamp)
    details_header += '{0:23}{1:6}{2:10}{3:14}\n'.format('Fileset', 'User', 'Usage (GiB)', ' File Count')
    details_header += '{0:23}{1:6}{2:10}{3:14}'.format('-'*22, '-'*5, '-'*10, ' '+'-'*13)

    print(details_header)
    print(details_data)

    if is_me:
        time = 'right now'
    else:
        time = timestamp

    summary_header = '\n## Quota Summary for {0} (as of {1})\n'.format(group_name, time)
    summary_header += '{0:23}{1:8}{2:12}{3:12}{4:14}{5:14}{6:10}{7:10}\n'.format('Fileset', 'Type', 'Usage (GiB)',
                                                                                 ' Quota (GiB)', ' File Count',
                                                                                 ' File Limit', ' Backup', ' Purged')
    summary_header += '{0:23}{1:8}{2:12}{3:12}{4:14}{5:14}{6:10}{7:10}'.format('-'*22, '-'*7, '-'*12,
                                                                               ' '+'-'*11, ' '+'-'*13, ' '+'-'*13,
                                                                               ' '+'-'*9, ' '+'-'*9)

    print(summary_header)
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


def print_email_output(details_data, summary_data, group_name, timestamp, is_me, cluster):

    header = "Our system has detected that you are approaching or have hit a \n"
    header += "storage quota on {0}.\n".format(cluster)
    header += "See below for details on your usage."

    print(header)

    warnings = []
    for summary in summary_data:
        if summary:
            warnings += limits_warnings(summary)

    if len(warnings):
        print('\n'.join(warnings))

    time = datetime.now().strftime("%b %d %Y, %H:%M:%S")

    summary_header = '\n## Quota Summary for {0} (as of {1})\n'.format(group_name, time)
    summary_header += '{0:23}{1:8}{2:12}{3:12}{4:14}{5:14}{6:10}{7:10}\n'.format('Fileset', 'Type', 'Usage (GiB)',
                                                                                 ' Quota (GiB)', ' File Count',
                                                                                 ' File Limit', ' Backup', ' Purged')
    summary_header += '{0:23}{1:8}{2:12}{3:12}{4:14}{5:14}{6:10}{7:10}'.format('-'*22, '-'*7, '-'*12,
                                                                               ' '+'-'*11, ' '+'-'*13, ' '+'-'*13,
                                                                               ' '+'-'*9, ' '+'-'*9)

    print(summary_header)
    for summary in summary_data:
        if summary:
            print(format_for_summary(summary, cluster))

    print('\n')

    details_header = '## Usage Details for {0} (as of {1})\n'.format(group_name, timestamp)
    details_header += '{0:23}{1:6}{2:10}{3:14}\n'.format('Fileset', 'User', 'Usage (GiB)', ' File Count')
    details_header += '{0:23}{1:6}{2:10}{3:14}'.format('-'*22, '-'*5, '-'*10, ' '+'-'*13)

    print(details_header)
    print(details_data)


if (__name__ == '__main__'):
    global debug

    user, group_id, cluster, is_me, print_format = get_args()
    group_name = grp.getgrgid(group_id).gr_name

    if cluster is None:
        cluster = get_cluster()

    filesystems = {'farnam': ['/gpfs/ysm', '/gpfs/gibbs', '/gpfs/slayman'],
                   'ruddle': ['/gpfs/ycga', '/gpfs/gibbs'],
                   'grace': ['/gpfs/loomis', '/gpfs/gibbs', '/gpfs/slayman', '/vast/palmer'],
                   'milgram': ['/gpfs/milgram'],
                   'slayman': ['/gpfs/slayman'],
                   'gibbs': ['/gpfs/gibbs']
                   }

    # usage details
    timestamp = time.strftime('%b %d %Y %H:%M', time.localtime(os.path.getmtime(filesystems[cluster][0]
                                                                                + '/.mmrepquota/current')))

    group_members = get_group_members(group_id, cluster)
    usage_data, filesets, all_filesets = read_usage_files(filesystems[cluster], user, group_members, cluster)
    validate_filesets(filesets, cluster, group_name, all_filesets)

    details_data = compile_usage_details(filesets, group_members, cluster, usage_data)

    # quota summary
    if debug:
        print("**Debug Output Enabled**")  
        summary_data = collect_quota_data(filesystems[cluster], filesets,
                                          all_filesets, user, group_id, cluster, True)
    else:
        if is_me:
            summary_data = localcache_quota_data(user)
            if summary_data is '':
                ## fix is_me toggle if data is not actually live
                summary_data = collect_quota_data(filesystems[cluster], filesets,
                                                   all_filesets, user, group_id, cluster, True)
        else:
             summary_data = collect_quota_data(filesystems[cluster], filesets, all_filesets, user, group_id, cluster, False)

    if print_format == 'cli':
        print_cli_output(details_data, summary_data, group_name, timestamp, is_me, cluster)
    elif print_format == 'email':
        print_email_output(details_data, summary_data, group_name, timestamp, is_me, cluster)
    else:
        sys.exit('unknown print format: ', print_format)
