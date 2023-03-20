#!/usr/bin/env python3

import os
import json
import subprocess

# filter for groups with data
groups_with_usage = []
filename = '/vast/palmer/.quotas/current'
with open(filename, 'r') as f:
    all_quota_data = json.load(f)
    for quota in all_quota_data:
        if ':' in quota['name']:
            fileset, name = quota['name'].split(':')
            if quota['used_effective_capacity'] > 0:
                groups_with_usage.append(name)

with open('/tmp/scratch_details', 'w') as f:
    for group in groups_with_usage:
        directory = '/vast/palmer/scratch/'+group
        print(group)
        query = 'sf query --type f --group-by "username" '+directory+' --csv -d , -H'
        result = subprocess.check_output([query], shell=True, encoding='UTF-8').replace('"','')

        for user_usage in result.split('\n'):
            if user_usage:
                f.write(group+','+user_usage+'\n')



