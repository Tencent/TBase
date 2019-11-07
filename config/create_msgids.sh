#/usr/bin/env sh

#
# Run this script when configuring with --enable-genmsgids
#
# Recurse through all subdiectories, collecting information about all subdirs
# which has a Makefile/GNUmakefile with "subdir = <subdir_name>" entry and
# assign a module_id for all such subdirs. The Makefile.global then looks up
# this catalog and uses the module_id configured.
#
# The script assumes that every subdir's Makefile has a specific pattern of
# "^subdir = .*", which is thankfully true for subdirs that we care for
#
# We could be a lot smarter than what we are doing, especially avoiding
# module_id assignment for subdirs which do not directly compile any files with
# elog() messages.
#
MSG_MODULE=0
handle_dir()
{
	MSG_MODULE=`expr $2 + 1`
	for subdir in `ls $1`; do
		if [ -d $1/$subdir ]; then
			makefile1=$1/$subdir/Makefile
			makefile2=$1/$subdir/GNUmakefile
			if [ -f $makefile1 ]; then
				cat $makefile1 | grep -E "^subdir = " > /dev/null
				if [ $? -ne 0 ]; then
					if [ -f $makefile2 ]; then
						cat $makefile2 | grep -E "^subdir = " > /dev/null
						if [ $? -eq 0 ]; then
							makefile=$makefile2
						else
							continue
						fi
					else
						continue
					fi
				else
					makefile=$makefile1
				fi
			else
				continue
			fi
			cat $makefile | grep -E "^subdir = " > /dev/null
			if [ $? -eq 0 ]; then
				moduledir=`cat $makefile | grep -E '^subdir = '`
				echo $moduledir:${MSG_MODULE}
			fi
			handle_dir "$1/$subdir" $MSG_MODULE
		fi
	done
}

handle_dir "." $MSG_MODULE
