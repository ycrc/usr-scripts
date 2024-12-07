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

gpfs_device_names = {'gibbs': 'gibbs',
                     'milgram': 'milgram',
                     'ycga': 'ycga',
                     'radev': 'radev',
                     'marilyn': 'radev'
                     }

vast_paths = {'palmer': '/vast/palmer/',
              'roberts': '/nfs/roberts/',
              'weston': '/nfs/weston/'
             }

common_filespaces = {'grace': ['home.grace', 'project', 'scratch'],
                     'mccleary': ['home.mccleary', 'project', 'scratch'],
                     'milgram': ['home', 'project', 'scratch60'],
                     'misha': ['home','project','scratch'],
                     }

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
    group = {}

    if args.group is None:
        if args.user is None:
            # get current user
            user = getpass.getuser()
            is_me = True
        else:
            user = args.user

        # make sure user is valid, and if so get gid
        try:
            group['id'] = pwd.getpwnam(user).pw_gid
        except:
            sys.exit('Unknown user: '+user)

    else:
        # make sure group is valid, and if so get gid
        try:
            group['id'] = grp.getgrnam(args.group).gr_gid
        except:
            sys.exit('Unknown group: '+args.group)

        # if group is set, no user is set
        user = None

    ## REMOVE ME
    print_format='cli'

    return user, group, args.cluster, is_me, print_format


def get_cluster():

    with open('/etc/yalehpc', 'r') as f:
        cluster = f.readline().split('=')[1].replace('"', '').rstrip()

    return cluster


def get_group_members(group, cluster):

    global active_users_only

    with open('/etc/yalehpc', 'r') as f:
        f.readline()
        mgt = f.readline().split('=')[1].replace('"', '').rstrip()

    query = "LDAPTLS_REQCERT=never ldapsearch -xLLL -H ldaps://{0} -b o=hpc.yale.edu -D".format(mgt)
    query += " cn=client,o=hpc.yale.edu -w hpc@Client"

    if active_users_only:
        query += " '(& ({0}HomeDirectory=*) (gidNumber={1}))'".format(cluster, group['id'])
    else:
        query += " '(gidNumber={0})'".format(group['id'])
    query += " uid | grep '^uid'"

    result = subprocess.check_output([query], shell=True, encoding='UTF-8')

    group['members'] = result.replace('uid: ', '').split('\n')

    # remove blank line
    if group['members'][-1] == '':
        group['members'].pop(-1)

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

def add_missing_pi_filesets(user_filesets, group):
#### BROKEN BUT MAYBE WE DONT
    # fix for forcing pi filesets to show up for all primary group members
    for fileset in sum(filesets_by_filesystems.values(), []):
        #if group in fileset and fileset not in user_filesets:
        if re.search(rf"[^:]+?:pi_{group['name']}$", fileset) and fileset not in user_filesets:
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

def place_output(output, quota):
    if 'home' in quota['fileset']:
        output[0] = quota

    elif 'project' in quota['fileset'] and 'pi' not in quota['fileset']:
        output[1] = quota

    # scratch60
    elif 'scratch' in quota['fileset']:
        output[2] = quota

    elif 'ycga:work' in quota['fileset']:
        output.append(quota)

### COLLECT USAGE DATA AND QUOTAS

##### GPFS

def parse_gpfs_mmrepquota_line(line, filesystem):

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

    fileset = filesystem+':'+fileset

    # blockUsage+blockInDoubt, blockQuota
    # filesUsage+filesInDoubt, filesQuota

    quota = {'fileset': fileset,
            'name': name, # netid is quota_type = USR, groupname if quota_type = GRP, '' if FILESET
            'used_gib': int(split[10])/1024/1024+int(split[13])/1024/1024, # blockUsage+blockInDoubt
            'quota_gib': int(split[12])/1024/1024, # blockQuota
            'used_files': int(split[15])+int(split[18]), # filesUsage+filesInDoubt
            'quota_files': int(split[17]) # filesQuota
           }

    return quota


def read_mmrepquota_gpfs(filesystem, this_user, cluster, group, usage_details, user_filesets):

    filename = '/gpfs/'+filesystem + '/.mmrepquota/current'

    if not os.path.exists(filename):
        print("%s is not available at the moment" % filesystem)
    else:    
        with open(filename, 'r') as f:
            f.readline()
            for line in f:

                if 'USR' not in line or 'root' in line or 'apps' in line:
                    continue

                user_data = parse_gpfs_mmrepquota_line(line, filesystem)
                
                if user_data['fileset'] == 'milgram:globus':
                    continue

                if user_data['fileset'] not in usage_details.keys():
                    usage_details[user_data['fileset']] = {}

                usage_details[user_data['fileset']][user_data['name']] = user_data

                if user_data['name'] == this_user or (this_user is None and user_data['name'] in group['members']):
                    user_filesets.add(user_data['fileset'])


def validate_gpfs_returned_values(result):
    if not re.match("^mmlsq", result):
        if debug:
            print("Invalid results returned:", result)
        return None
    else:
        return result


def quota_data_gpfs(filesets, filesystem, user, group, cluster, output, is_live=True):

    global debug
    quota_script = '/usr/lpp/mmfs/bin/mmlsquota'

    if is_live:
        # get group level usage
        device = gpfs_device_names[filesystem]
        query = '{0} -g {1} -Y --block-size auto {2}'.format(quota_script, group['name'], device)
        if debug:
            result = subprocess.check_output([query], shell=True, encoding='UTF-8')
        else:
            result = external_program_filter(query)

        # user based home quotas
        if device not in ['gibbs', 'ycga']:
            query = '{0} -u {1} -Y --block-size auto {2} '.format(quota_script, user, device)
            if debug:
                result += subprocess.check_output([query], shell=True, encoding='UTF-8')
            else:
                result += external_program_filter(query)

        # now add pi filesets previously identified in read_mmrepquota_gpfs
        for fileset in filesets:
            if is_pi_fileset(fileset) and filesystem in fileset:
                fileset_name = fileset.split(':')[1]

                query = '{0} -j {1} -Y {2}'.format(quota_script, fileset_name, device)
                if debug:
                    result += subprocess.check_output([query], shell=True, encoding='UTF-8')
                else:
                    result += external_program_filter(query)

        # make sure that result holds valid data
        result = validate_gpfs_returned_values(result).split('\n')

        for line in result:
            sort_gpfs_quota(line, filesystem, filesets, user, group, output)

    #read from flat file instead of gpfs query
    else:

        filename = '/gpfs/'+filesystem + '/.mmrepquota/current'

        if not os.path.exists(filename):
             return output

        with open(filename, 'r') as f:
            for line in f:
                sort_gpfs_quota(line, filesystem, filesets, user, group, output)

    return output

def sort_gpfs_quota(line, filesystem, filesets, user, group, output):

    # filter for just relevant rows
    if 'HEADER' in line or 'root' in line or 'apps' in line or len(line) < 10:
        return
    if ('USR' in line and 'home' not in line):
        return
    if ('GRP' in line and ('scratch' not in line and 'project' not in line and 'work' not in line)):
        return

    quota = parse_gpfs_mmrepquota_line(line, filesystem)

    if quota['fileset'] in filesets:
        if (('home' in quota['fileset'] and quota['name'] == user) or quota['name'] == group['name']):
            place_output(output, quota)

        elif is_pi_fileset(quota['fileset']):
            output.append(quota)

### VAST

def quota_data_vast(filesystem, user, group, cluster, output, is_live=False):

    filenames = [vast_paths[filesystem] + '/.quotas/current']
    if cluster == 'mccleary':
        filenames.append(vast_paths[filesystem] + '/.quotas/mccleary_current')
    if cluster == 'grace':
        filenames.append(vast_paths[filesystem] + '/.quotas/grace_current')

    if user is not None:
        uid = str(pwd.getpwnam(user).pw_uid)
    else:
        uid = ""

    for filename in filenames:
        if not os.path.exists(filename):
            return output
        
        with open(filename, 'r') as f:
            vast_quota_data = json.load(f)

            for this_quota in vast_quota_data:

                if 'mccleary' in filename or 'grace' in filename:

                    if user is not None and (user == this_quota['entity_identifier'] or uid == this_quota['entity_identifier']):
                        fileset = 'palmer:home.'+cluster
                        ### FIX: REPLACE used_effective_capacity instead of used_capacity
                        quota = {'fileset': fileset,
                                'name': this_quota['entity_identifier'],
                                'used_gib': this_quota['used_capacity']/1024/1024/1024,
                                'quota_gib': this_quota['hard_limit']/1024/1024/1024,
                                'used_files': this_quota['used_inodes'],
                                'quota_files': this_quota['hard_limit_inodes']
                                }

                    #    [fileset, quota['entity_identifier'], 'USR', quota['used_capacity']/1024/1024/1024,
                    #                        quota['hard_limit']/1024/1024/1024, quota['used_inodes'], quota['hard_limit_inodes']]
                        place_output(output, quota)
                else:
                    if ':' in this_quota['name']:

                        fileset, name = this_quota['name'].split(':')
                        if group['name'] == name:
                            if 'scratch' in fileset:
                                fileset = filesystem+':'+fileset
                                quota = {'fileset': fileset,
                                        'name': group['name'],
                                        'used_gib': this_quota['used_effective_capacity']/1024/1024/1024,
                                        'quota_gib': this_quota['hard_limit']/1024/1024/1024,
                                        'used_files': this_quota['used_inodes'],
                                        'quota_files': this_quota['hard_limit_inodes']
                                        }
                             #   data = [fileset, group['name'], 'GRP', quota['used_effective_capacity']/1024/1024/1024,
                                   #     quota['hard_limit']/1024/1024/1024, quota['used_inodes'], quota['hard_limit_inodes']]
                                place_output(output, quota)
                            elif fileset == 'pi':
                                quota = {'fileset': 'palmer:pi_'+group['name'],
                                        'name': group['name'],
                                        'used_gib': this_quota['used_effective_capacity']/1024/1024/1024,
                                        'quota_gib': this_quota['hard_limit']/1024/1024/1024,
                                        'used_files': this_quota['used_inodes'],
                                        'quota_files': this_quota['hard_limit_inodes']
                                        }
                               # data = ['palmer:pi_'+group['name'], group['name'], 'FILESET', quota['used_effective_capacity']/1024/1024/1024,
                               #         quota['hard_limit']/1024/1024/1024, quota['used_inodes'], quota['hard_limit_inodes']]
                                output.append(quota)
    return output

# Outputs generated by cron on monitor1.grace that runs starfish_vast_usage.py
def read_vast_line(line):
    data = {}

    # group, username, filecount, usage (kb), usage (string)
    split = line.split(',')
    data['group'] = split[0]
    data['user'] = split[1]
    data['usage_GiB'] = int(split[3])/1024/1024/1024
    data['usage_files'] = int(split[2])

    return data


def read_user_details_vast(filesystem, this_user, group, user_based_usage, user_filesets):

    read_user_details_vast_scratch(filesystem, group, user_based_usage, user_filesets)
    read_user_details_vast_pi(filesystem, this_user, group, user_based_usage, user_filesets)


def read_user_details_vast_scratch(filesystem, group, user_based_usage, user_filesets):

    # scratch
    fileset = 'palmer:scratch'
    user_based_usage[fileset] = {}

    filename = '/vast/palmer/.quotas/scratch.details'
    if not os.path.exists(filename):
            return

    with open(filename, 'r') as f:
        f.readline()
        for line in f:
            # group, username, filecount, usage (kb), usage (string)
            data = read_vast_line(line)
            if data['group'] != group['name']:
                continue
            else:
                user_based_usage[fileset][data['user']] = {'used_gib':  data['usage_GiB'],
                                                           'used_files':  data['usage_files'],
                                                         }
                user_filesets.add(fileset)


def read_user_details_vast_pi(filesystem, this_user, group, user_based_usage, user_filesets):

    # pi filesets
    filename = '/vast/palmer/.quotas/pi.details'
    if not os.path.exists(filename):
            return

    with open(filename, 'r') as f:
        f.readline()
        for line in f:

            data = read_vast_line(line)
            user = data['user']
            fileset = 'palmer:pi_'+data['group']
            if fileset not in user_based_usage.keys():
                    user_based_usage[fileset] = {}

            user_based_usage[fileset][data['user']] = {'used_gib':  data['usage_GiB'],
                                                       'used_files':  data['usage_files'],
                                                       }

            if user == this_user or (this_user is None and user in group['members']):
                user_filesets.add(fileset)


## OVERALL USAGE AND QUOTA COLLECTION
def collect_usage_details(filesystems, this_user, group, cluster):

    # collects all usage details for gpfs systems
    user_based_usage = {}
    # collects list of all filesets and filesets where this_user has data
    user_filesets = set()

    for filesystem in filesystems:
        if filesystem in gpfs_device_names.keys():
            read_mmrepquota_gpfs(filesystem, this_user, cluster, group,
                                 user_based_usage, user_filesets)

        elif filesystem in ['palmer', 'roberts', 'weston']:
            read_user_details_vast(filesystem, this_user, group, user_based_usage, user_filesets)
        else:
            print('Unknown filesystem, '+filesystem+', on '+cluster)

    return user_based_usage, list(user_filesets)


def collect_quota_data(filesets, filesystems, user, group, cluster, is_live):

    global debug
    if debug:
        print("**Debug Output Enabled**")

    output = ['', '', '']
    for filesystem in filesystems:
        if filesystem in gpfs_device_names.values():
            if is_live:
                if debug:
                    # if debug mode, force live query
                    quota_data_gpfs(filesets, filesystem,
                                    user, group, cluster, output, is_live=True)
                else:
                    #if not debug mode, fail over silently
                    try:
                        quota_data_gpfs(filesets, filesystem,
                                             user, group, cluster, output, is_live=True)
                    except:
                        is_live = False
                        quota_data_gpfs(filesets, filesystem,
                                             user, group, cluster, output, is_live=False)
            else:
                quota_data_gpfs(filesets, filesystem, user, group, cluster, output, is_live=False)

        elif filesystem in ['palmer', 'roberts', 'weston']:
            # vast doesn't (yet?) return live data so just return cached data
             quota_data_vast(filesystem, user, group, cluster, output)

    if is_live:
        file = open('/tmp/.%sgqlc' % user, 'wb')
        pickle.dump(output, file)
        file.close()

    return output

## USER BREAKDOWN ##
def compile_usage_details(filesets, group, user_based_usage):
    output = ['', '']

    for fileset in sorted(filesets):
        section = []

        if is_pi_fileset(fileset):
            for user in sorted(user_based_usage[fileset].keys()):
                section.append(format_for_details(fileset, user, user_based_usage[fileset][user]))
            output.append('\n'.join(section))

        else:
            for group_member in sorted(group['members']):
                if group_member not in user_based_usage[fileset].keys():
                    continue
                else:
                    section.append(format_for_details(fileset, group_member, user_based_usage[fileset][group_member]))

        section = '\n'.join(section)

        if 'project' in section:
            output[0] = section
        elif 'scratch' in section:
            output[1] = section
        elif 'work' in section:
            output.append(section)

    return '\n----\n'.join(output)


### OUTPUT FORMATTING

def format_for_details(fileset, user, user_based_usage):

    # fileset, user, bytes, file count
    return '{0:30.29}{1:14.13}{2:10.0f}{3:14,}'.format(fileset, user,
                                             user_based_usage['used_gib'], user_based_usage['used_files'])


def format_for_summary(quotas, cluster):

    backup = 'No'
    purge = 'No'

    fileset = quotas['fileset']

    if 'home' in fileset:
        type = 'USR'
    else:
        type = 'GRP'

    if 'home' in fileset or cluster == 'milgram':
        if 'scratch' not in fileset:
            backup = 'Yes'

    if 'scratch' in fileset:
        purge = '60 days'

    # fileset, userid, quota_type, bytes, byte quota, file count, file limit
    return '{0:30.29}{1:8}{2:12.0f}{3:12.0f}{4:14,}{5:14,} {6:10}{7:10}'.format(fileset, type,
                                                                       quotas['used_gib'], quotas['quota_gib'],
                                                                       quotas['used_files'], quotas['quota_files'],
                                                                       backup, purge)

### LIMIT CHECKS

def check_limits(summary_data):

    at_limit = {'byte': 0,
                'file': 0}

    # if you can, avoid the possiblity of dividing by zero
    if summary_data['quota_gib'] == 0:
        return at_limit
    if summary_data['quota_files'] == 0:
        return at_limit

    if (summary_data['quota_gib']-summary_data['used_gib'])/float(summary_data['quota_gib']) <= 0.05:
        at_limit['byte'] = True
    if (summary_data['quota_files']-summary_data['used_files'])/float(summary_data['quota_files']) <= 0.05:
        at_limit['file'] = True

    return at_limit


def limits_warnings(summary_data):

    at_limit = check_limits(summary_data)
    warnings = []

    if at_limit['byte']:
        warnings.append("Warning!!! You are at or near your storage limit in the %s fileset. "
                        "Reduce your storage usage to avoid issues." % summary_data['fileset'])
    # file limit
    if at_limit['file']:
        warnings.append("Warning!!! You are at or near your file count limit in the %s fileset. "
                        "Reduce the number of files to avoid issues." % summary_data['fileset'])
    return warnings

#### PRINT FORMATS

def get_quota_status(summary_data):

    warnings = []
    for summary in summary_data:
        if summary:
            at_limit = check_limits(summary)
            if at_limit['byte']:
                warnings.append([summary_data['fileset'], summary_data['used_gib'], summary_data['quota_gib']])
            if at_limit['file']:
                warnings.append([summary_data['fileset'], summary_data['used_files'], summary_data['quota_files']])
    print(warnings)

def print_cli_output(details_data, summary_data, group, timestamp, is_live, cluster):

    header = "This script shows information about your quotas on {0}.\n".format(cluster)
    header += "If you plan to poll this sort of information extensively,\n"
    header += "please contact us for help at hpc@yale.edu\n"

    print(header)

    details_header = '## Usage Details for {0} (as of {1})\n'.format(group['name'], timestamp)
    details_header += '{0:30}{1:14}{2:10}{3:14}\n'.format('Fileset', 'User', 'Usage (GiB)', ' File Count')
    details_header += '{0:30}{1:14}{2:10}{3:14}'.format('-'*29, '-'*13, '-'*10, ' '+'-'*13)

    print(details_header)
    print(details_data)

    if is_live:
        time = 'right now [*palmer stats are gathered once a day]'
    else:
        time = timestamp

    summary_header = '\n## Quota Summary for {0} (as of {1})\n'.format(group['name'], time)
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

### MAIN ###

if (__name__ == '__main__'):
    global debug

    user, group, cluster, is_me, print_format = get_args()
    group['name'] = grp.getgrgid(group['id']).gr_name

    filesystems = {
                   'grace': ['gibbs', 'palmer'],
                   'mccleary': ['gibbs', 'palmer'],
                   'milgram': ['milgram'],
                   'misha': ['radev'],
                   }

    # check if user in ycga group
    if cluster in ['mccleary'] and 10266 in os.getgroups():
        filesystems[cluster].append('ycga')
    
    # usage details
    timestamp = time.strftime('%b %d %Y %H:%M', time.localtime(os.path.getmtime('/gpfs/'+filesystems[cluster][0]
                                                                                + '/.mmrepquota/current')))

    get_group_members(group, cluster)

    user_based_usage, user_filesets = collect_usage_details(filesystems[cluster], user,
                                                               group, cluster)
    # add_missing_pi_filesets(user_filesets, group) # CURRENTLY BROKEN 
    
    details_data = compile_usage_details(user_filesets, group, user_based_usage)
    
    is_live = False
    if is_me:
        is_live = True

    # usage and quota summary
    summary_data = None
#    if is_me:
#        summary_data = localcache_quota_data(user)
    if summary_data is None or debug:
        summary_data = collect_quota_data(user_filesets, filesystems[cluster],
                                          user, group, cluster, is_live)

    # print
    if print_format == 'cli':
        print_cli_output(details_data, summary_data, group, timestamp, is_live, cluster)
    elif print_format == 'query':
        get_quota_status(summary_data)
    else:
        sys.exit('unknown print format: ', print_format)
