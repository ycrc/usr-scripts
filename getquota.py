#!/usr/bin/env python3
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
import argparse
import sys
import time
from datetime import datetime
from threading import Timer

gpfs_device_names = {'/gpfs/ysm': 'ysm-gpfs',
                     '/gpfs/gibbs': 'gibbs',
                     '/gpfs/slayman': 'slayman',
                     '/gpfs/milgram': 'milgram',
                     '/gpfs/ycga': 'ycga'
                     }

common_filespaces = {'grace': ['home.grace', 'project', 'scratch'],
                     'mccleary': ['home.mccleary', 'project', 'scratch'],
                     'farnam': ['home', 'project', 'scratch60'],
                     'ruddle': ['home', 'project', 'scratch60'],
                     'milgram': ['home', 'project', 'scratch60']
                     }

#### TO DO #####
""""
- refactor all general "fileset" logic to accomodate home, scratch not on GPFS
- refactor "this_filesystem" to be a dictionary that contains all filesets on that filesystem
"""


def get_args():

    global debug
    global active_users_only
    is_me = False

    parser = argparse.ArgumentParser(
                    prog = 'getquota',
                    description = 'Reports storage usage and quotas of YCRC HPCs.  Use -u <user>, -g <group> or no argument for current user"',
                    epilog = 'For issues and questions, contact hpc@yale.edu')

    parser.add_argument('-d', '--debug', action='store_true', help='debug mode')
    parser.add_argument('-a', '--active-users', action='store_true',
                        help='only display usage for active users')
    parser.add_argument('-u', '--user', help='usage and quotas for specific user')
    parser.add_argument('-g', '--group', help='usage and quotas for specific group')
    parser.add_argument('-c', '--cluster', default=get_cluster(),
                        help='usage and quotas on alternate cluster')

    args = parser.parse_args()

    debug = args.debug
    active_users_only = args.active_users

    if args.group is None:
        if args.user is None:
            # get current user
            user = getpass.getuser()
            is_me = True
        else:
            user = args.user

        # make sure user is valid, and if so get gid
        try:
            group_id = pwd.getpwnam(user).pw_gid
        except:
            sys.exit('Unknown user: '+user)

    else:
        # make sure group is valid, and if so get gid
        try:
            group_id = grp.getgrnam(args.group).gr_gid
        except:
            sys.exit('Unknown group: '+args.group)

        # if group is set, no user is set
        user = None

    ## REMOVE ME
    print_format='cli'

    return user, group_id, args.cluster, is_me, print_format


def get_cluster():

    with open('/etc/yalehpc', 'r') as f:
        cluster = f.readline().split('=')[1].replace('"', '').rstrip()

    return cluster


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

    result = subprocess.check_output([query], shell=True, encoding='UTF-8')

    group_members = result.replace('uid: ', '').split('\n')

    # remove blank line
    if group_members[-1] == '':
        group_members.pop(-1)

    return group_members

### ADAM'S CACHING ###

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

### END ADAM'S CACHING ###


## PI FILESET CHECKS

def add_missing_pi_filesets(user_filesets, group, filesets_by_filesystems):

    # fix for forcing pi filesets to show up for all primary group members
    for fileset in sum(filesets_by_filesystems.values(), []):
        #if group in fileset and fileset not in user_filesets:
        if re.search(rf"[^:]+?:pi_{group}$", fileset) and fileset not in user_filesets:
            #print(fileset, group)
            user_filesets.append(fileset)


def is_pi_fileset(fileset, section=None):
    # only applicable for GPFS
    if section is not None and 'FILESET' not in section:
        return False

    if 'pi' in fileset:
        return True
    elif 'scratch' in fileset or 'home' in fileset or 'project' in fileset or 'work' in fileset:
        return False
    elif 'apps' in fileset:
        return False
    else:
        return True

### HELPER FUNCTIONS

def prefix_filesystem(filesystem, fileset):

    if 'gpfs' in filesystem:
        fileset = filesystem.replace('/gpfs/', '')+':'+fileset
    elif 'vast' in filesystem:
        fileset = 'palmer'+':'+fileset

    return fileset


def place_output(output, section, fileset):
    if 'home' in fileset:
        output[0] = section

    elif 'project' in fileset and 'pi' not in fileset:
        output[1] = section

    # scratch60
    elif 'scratch' in fileset:
        output[2] = section

    elif fileset == 'ycga:work':
        output.append(section)

### COLLECT USAGE DATA AND QUOTAS

##### GPFS

def parse_gpfs_mmrepquota_line(line, details, filesystem):

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


def read_mmrepquota_gpfs(filesystem, this_user, group_members, cluster, usage_details, user_filesets, all_filesets):

    filename = filesystem + '/.mmrepquota/current'

    if not os.path.exists(filename):
        print("%s is not available at the moment" % filesystem)
    else:    
        with open(filename, 'r') as f:
            f.readline()
            for line in f:

                if 'USR' not in line or 'root' in line or 'apps' in line:
                    continue

                fileset, user, user_data = parse_gpfs_mmrepquota_line(line, True, filesystem)
                if fileset == 'gibbs:project' and cluster in ['ruddle', 'farnam']:
                    continue
                if fileset == 'milgram:globus':
                    continue

                if fileset not in usage_details.keys():
                    usage_details[fileset] = {}

                usage_details[fileset][user] = user_data

                if user == this_user or (this_user is None and user in group_members):
                    user_filesets.add(fileset)

                if fileset not in all_filesets:
                    all_filesets.append(fileset)


def validate_gpfs_returned_values(result):
    if not re.match("^mmlsq", result):
        if debug:
            print("Invalid results returned:", result)
        return None
    else:
        return result


def live_quota_data_gpfs(filesets, filesystem, all_filesets, user, group, cluster, output):

    global debug
    quota_script = '/usr/lpp/mmfs/bin/mmlsquota'

    # get group level usage
    device = gpfs_device_names[filesystem]
    query = '{0} -g {1} -Y --block-size auto {2}'.format(quota_script, group, device)
    if debug:
        result = subprocess.check_output([query], shell=True, encoding='UTF-8')
    else:
        result = external_program_filter(query)

    # user based home quotas
    if device not in ['slayman', 'gibbs']:
        query = '{0} -u {1} -Y --block-size auto {2} '.format(quota_script, user, device)
        if debug:
            result += subprocess.check_output([query], shell=True, encoding='UTF-8')
        else:
            result += external_program_filter(query)

    # make sure that result thus far holds valid data
    result = validate_gpfs_returned_values(result)

    for quota in result.split('\n'):
        if 'HEADER' in quota or 'root' in quota or 'apps' in quota or len(quota) < 10:
            continue
        if ('USR' in quota and 'home' not in quota):
            continue
        if (device == 'gibbs' and 'project' in quota and cluster in ['ruddle', 'farnam']):
            continue

        fileset, _, section = parse_gpfs_mmrepquota_line(quota, False, filesystem)

        place_output(output, section, fileset)

    # now add pi filesets previously identified in read_mmrepquota_gpfs and add_missing_pi_filesets
    for fileset in filesets:
        # check if this fileset is on this device
        if is_pi_fileset(fileset):
            # check if this fileset is on this device
            if fileset in all_filesets:
                # query the local pi filesets
                fileset_name = re.search('[^:]+?:(.*):', fileset).group(1)

                query = '{0} -j {1} -Y {2}'.format(quota_script, fileset_name, device)
                if debug:
                    pi_quota = subprocess.check_output([query], shell=True, encoding='UTF-8')
                else:
                    pi_quota = external_program_filter(query)
                pi_quota = validate_gpfs_returned_values(pi_quota)

                output.append(parse_gpfs_mmrepquota_line(pi_quota.split('\n')[1], False, filesystem)[-1])

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

            fileset, name, section = parse_gpfs_mmrepquota_line(line, False, filesystem)

            if fileset in filesets:
                if 'home' in fileset:
                    if cluster == "grace":
                        continue
                    if 'USR' in line and name == user:
                        if filesystem == 'gibbs' and cluster in ['ruddle', 'farnam'] and fileset == 'project':
                            continue
                        place_output(output, section, fileset)
                    continue

                if name == group:
                        place_output(output, section, fileset)

                elif is_pi_fileset(fileset, section=section):
                    output.append(section)

    return output

### VAST

def cached_quota_data_vast(filesystem, user, group, cluster, output):

    filenames = [filesystem + '/.quotas/current']
    if cluster == 'mccleary':
        filenames.append(filesystem + '/.quotas/mccleary_current')
    if cluster == 'grace':
        filenames.append(filesystem + '/.quotas/grace_current')

    for filename in filenames:
        if not os.path.exists(filename):
            return output

        with open(filename, 'r') as f:
            all_quota_data = json.load(f)

            for quota in all_quota_data:
                if 'mccleary' in filename or 'grace' in filename:
                    if user is not None and user in quota['entity_identifier']:
                        fileset = 'palmer:home.'+cluster
                        ### FIX: REPLACE used_effective_capacity instead of used_capacity
                        data = [fileset, quota['entity_identifier'], 'USR', quota['used_capacity']/1024/1024/1024,
                                            quota['hard_limit']/1024/1024/1024, quota['used_inodes'], quota['hard_limit_inodes']]
                        place_output(output, data, fileset)
                else:
                    if ':' in quota['name']:
                        fileset, name = quota['name'].split(':')

                       # if user is not None and cluster == 'grace':
                       #     if 'home' in fileset and user in quota['name']:
                       #         data = ['palmer:'+fileset, name, 'USR', quota['used_effective_capacity']/1024/1024/1024,
                       #                 quota['hard_limit']/1024/1024/1024, quota['used_inodes'], quota['hard_limit_inodes']]
                       #         place_output(output, data, fileset)

                        if name == group:
                            fileset = prefix_filesystem(filesystem, fileset)
                            data = [fileset, name, 'GRP', quota['used_effective_capacity']/1024/1024/1024,
                                    quota['hard_limit']/1024/1024/1024, quota['used_inodes'], quota['hard_limit_inodes']]
                            place_output(output, data, fileset)

    return output

# Outputs generated by cron on monitor1.grace that runs starfish_vast_usage.py
def read_user_details_vast(group, usage_details, user_filesets, all_filesets):

    fileset = 'palmer:scratch'
    usage_details[fileset] = {}

    filename = '/vast/palmer/.quotas/scratch.details'
    if not os.path.exists(filename):
            return

    with open(filename, 'r') as f:
        f.readline()
        for line in f:
            # group, username, filecount, usage (kb), usage (string)
            split = line.split(',')
            if split[0] != group:
                continue
            else:
                user = split[1]
                usage_details[fileset][user] = [fileset, user, '', int(split[3])/1024/1024/1024, '', int(split[2]), '']  

                user_filesets.add(fileset)

                if fileset not in all_filesets:
                    all_filesets.append(fileset)


## OVERALL USAGE AND QUOTA COLLECTION
def collect_usage_details(filesystems, this_user, group_members, group, cluster):

    # collects all usage details for gpfs systems
    usage_details = {}
    # collects list of all filesets and filesets where this_user has data
    user_filesets = set()
    filesets_by_filesystem = {filesystem: [] for filesystem in filesystems}

    for filesystem in filesystems:
        if 'gpfs' in filesystem:
            read_mmrepquota_gpfs(filesystem, this_user, group_members, cluster,
                                 usage_details, user_filesets, filesets_by_filesystem[filesystem])
        elif 'vast' in filesystem:
            if cluster in ['grace', 'mccleary']:
                read_user_details_vast(group, usage_details, user_filesets, filesets_by_filesystem[filesystem])
            else:
                pass

    return usage_details, list(user_filesets), filesets_by_filesystem


def collect_quota_data(filesets, filesets_by_filesystem, user, group_id, cluster, is_live):

    global debug
    if debug:
        print("**Debug Output Enabled**")

    output = ['', '', '']
    group_name = grp.getgrgid(group_id).gr_name
    for filesystem in filesets_by_filesystem.keys():
        if 'gpfs' in filesystem:
            if is_live:
                if debug:
                    # if debug mode, force live query
                    live_quota_data_gpfs(filesets, filesystem, filesets_by_filesystem[filesystem],
                                         user, group_id, cluster, output)
                else:
                    #if not debug mode, fail over silently
                    try:
                        live_quota_data_gpfs(filesets, filesystem, filesets_by_filesystem[filesystem],
                                             user, group_id, cluster, output)
                    except:
                        is_live = False
                        cached_quota_data_gpfs(filesystem, filesets, user, group_name, cluster, output)
            else:
                cached_quota_data_gpfs(filesystem, filesets, user, group_name, cluster, output)
        elif 'vast' in filesystem:
            # vast doesn't (yet?) return live data so just return cached data
             cached_quota_data_vast(filesystem, user, group_name, cluster, output)

    if is_live:
        file = open('/tmp/.%sgqlc' % user, 'wb')
        pickle.dump(output, file)
        file.close()

    return output

## USER BREAKDOWN ##
def compile_usage_details(filesets, group_members, data):
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
                    continue
                else:
                    section.append(format_for_details(data[fileset][group_member]))
        place_output(output, '\n'.join(section), fileset)

    # don't show home data
    output.pop(0)
    for i in range(len(output)-1):
        if len(output[i]) == 0:
            output.pop(i)

    return '\n----\n'.join(output)


### OUTPUT FORMATTING

def format_for_details(data):

    # fileset, user, bytes, file count
    return '{0:30.29}{1:14.13}{2:10.0f}{3:14,}'.format(data[0], data[1],
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
    return '{0:30.29}{1:8}{2:12.0f}{3:12.0f}{4:14,}{5:14,} {6:10}{7:10}'.format(data[0], data[2],
                                                                       data[3], data[4],
                                                                       data[5], data[6],
                                                                       backup, purge)

### LIMIT CHECKS

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

#### PRINT FORMATS
def print_cli_output(details_data, summary_data, group_name, timestamp, is_live, cluster):

    header = "This script shows information about your quotas on {0}.\n".format(cluster)
    header += "If you plan to poll this sort of information extensively,\n"
    header += "please contact us for help at hpc@yale.edu\n"

    print(header)

    details_header = '## Usage Details for {0} (as of {1})\n'.format(group_name, timestamp)
    details_header += '{0:30}{1:14}{2:10}{3:14}\n'.format('Fileset', 'User', 'Usage (GiB)', ' File Count')
    details_header += '{0:30}{1:14}{2:10}{3:14}'.format('-'*29, '-'*13, '-'*10, ' '+'-'*13)

    print(details_header)
    print(details_data)

    if is_live:
        time = 'right now [*palmer stats are gathered once a day]'
    else:
        time = timestamp

    summary_header = '\n## Quota Summary for {0} (as of {1})\n'.format(group_name, time)
    summary_header += '{0:30}{1:8}{2:12}{3:12}{4:14}{5:14}{6:10}{7:10}\n'.format('Fileset', 'Type', 'Usage (GiB)',
                                                                                 ' Quota (GiB)', ' File Count',
                                                                                 ' File Limit', ' Backup', ' Purged')
    summary_header += '{0:30}{1:8}{2:12}{3:12}{4:14}{5:14}{6:10}{7:10}'.format('-'*29, '-'*7, '-'*12,
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


def print_email_output(details_data, summary_data, group_name, timestamp, cluster):

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

### MAIN ###

if (__name__ == '__main__'):
    global debug

    user, group_id, cluster, is_me, print_format = get_args()
    group_name = grp.getgrgid(group_id).gr_name

    filesystems = {'farnam': ['/gpfs/ysm', '/gpfs/gibbs'],
                   'ruddle': ['/gpfs/ycga', '/gpfs/gibbs'],
                   'grace': ['/gpfs/gibbs', '/vast/palmer'],
                   'mccleary': ['/gpfs/gibbs', '/vast/palmer', '/gpfs/ycga'],
                   'milgram': ['/gpfs/milgram'],
                   'slayman': ['/gpfs/slayman'],
                   'gibbs': ['/gpfs/gibbs']
                   }

    if cluster in ['farnam', 'grace', 'mccleary'] and group_name == 'gerstein':
        filesystems[cluster].append('/gpfs/slayman')

    # usage details
    timestamp = time.strftime('%b %d %Y %H:%M', time.localtime(os.path.getmtime(filesystems[cluster][0]
                                                                                + '/.mmrepquota/current')))

    group_members = get_group_members(group_id, cluster)

    usage_data, user_filesets, filesets_by_filesystem = collect_usage_details(filesystems[cluster], user,
                                                               group_members, group_name, cluster)

    add_missing_pi_filesets(user_filesets, group_name, filesets_by_filesystem)
    
    details_data = compile_usage_details(user_filesets, group_members, usage_data)
    
    is_live = False
    if is_me:
        is_live = True

    # usage and quota summary
    summary_data = None
#    if is_me:
#        summary_data = localcache_quota_data(user)
    if summary_data is None or debug:
        summary_data = collect_quota_data(user_filesets, filesets_by_filesystem,
                                          user, group_id, cluster, is_live)

    # print
    if print_format == 'cli':
        print_cli_output(details_data, summary_data, group_name, timestamp, is_live, cluster)
    elif print_format == 'email':
        print_email_output(details_data, summary_data, group_name, timestamp, cluster)
    else:
        sys.exit('unknown print format: ', print_format)
