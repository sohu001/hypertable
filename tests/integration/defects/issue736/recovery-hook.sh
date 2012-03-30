#!/usr/bin/env bash
#
# Copyright (C) 2007-2012 Hypertable, Inc.
#
# This file is part of Hypertable.
#
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or any later version.
#
# Hypertable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Hypertable. If not, see <http://www.gnu.org/licenses/>
#

#
# This script uses the unix "mail" tool to send an email whenever a 
# RangeServer fails and gets recovered.
#
# 
# Configuration setting
#
# The recipient of the email
recipient=root

###############################################################################

rs_down=$1
rs_hostname=$2
servers_total=$3
servers_up=$4
servers_down=$5
servers_required=$6
quorum_percent=$7

text=$(cat <<EOF
Dear administrator,\\n
\\n
The RangeServer ${rs_down} (${rs_hostname}) is no longer available and is\\n
about to be recovered. All ranges of ${rs_down} are moved to other machines.\\n
After you fixed the failing node please manually delete the file "run/location"\\n
in the Hypertable directory on ${rs_hostname} (usually /opt/hypertable/<version>)\\n
before restarting Hypertable on this node.\\n
\\n
Current statistics:\\n
\\n
${servers_total} server(s) total\\n
${servers_up} server(s) up\\n
${servers_down} server(s) down\\n
\\n
Recovery will only continue if at least ${servers_required} RangeServers
(${quorum_percent}%) are running.\\n This setting can be overwritten with the
parameter\\n --Hypertable.Failover.Quorum.\\n
\\n
The Hypertable.RangeServer.log logfile on ${rs_hostname} will have reasons why\\n
the RangeServer had to be recovered.\\n
EOF
)

# now send the mail
#echo -e $text | mail -s "Hypertable alert" ${recipient}
echo -e $text > /tmp/issue736-output

