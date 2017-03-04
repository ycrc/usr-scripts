#!/bin/bash
#Ben Evans did this.

QUOTA="/usr/lpp/mmfs/bin/mmlsquota"
DEVICE="ysm-gpfs"

echo "This script should show you information about your quotas on the current gpfs filesystem."
echo "If you plan to poll this sort of information extensively, please use alternate means"
echo " and/or contact us for help at hpc@yale.edu"
echo ""

#get pi fileset if your group(s) have any
PI_SETS=()
for g in $( groups )
do 
	for i in $( ${QUOTA} -eg $(id -g) --block-size auto ${DEVICE} | awk '{print $2}' )
	do
		if [ "pi_$g" == "$i" ]; then
			PI_SETS+=("$i")	
		fi
	done
done

# for testing multiple filesets
# PI_SETS=( "pi_gerstein" "pi_sindelar" )

#this spacing about right
echo -e "\t\t\t\tBlock Limits\t\t\t\t\t\tFile Limits"

#space all lines everything nicely, together
column -t <( ${QUOTA} -eu $(id -u) --block-size auto ${DEVICE} | grep -v "no limits" | tail -n+2 | column -t ) \
<( ${QUOTA} -eg $(id -g) --block-size auto ${DEVICE} | grep -v "no limits" | tail -n+5 | column -t ) \
<( for pi_set in "${PI_SETS[@]}"
	do 
		${QUOTA} -ej $pi_set --block-size auto ${DEVICE} | tail -n+3 \
                | awk -v pi="$pi_set" '{print $1,pi,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14}'
	done ) 

