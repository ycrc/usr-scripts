#!/bin/bash

hostname
echo "Has $(nproc) cores and ~ $(free -g | awk '{print $2}' | tail -n +2 | head -1)GB of RAM"

