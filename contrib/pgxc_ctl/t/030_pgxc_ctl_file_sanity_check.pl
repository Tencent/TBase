use strict;
use warnings;
use Cwd;
use Config;
use TestLib;
use Test::More tests => 6;

my $dataDirRoot="~/DATA/pgxl/nodes/";
$ENV{'PGXC_CTL_HOME'} = '/tmp/pgxc_ctl';
my $PGXC_CTL_HOME=$ENV{'PGXC_CTL_HOME'};

#delete related dirs for cleanup
system("rm -rf $dataDirRoot");
system("rm -rf $PGXC_CTL_HOME");

program_help_ok('pgxc_ctl');
program_version_ok('pgxc_ctl');

my $GTM_HOST = "localhost";
my $COORD1_HOST = "localhost";
my $COORD2_HOST = "localhost";
my $COORD3_HOST = "localhost";
my $DN1_HOST = "localhost";
my $DN2_HOST = "localhost";
my $DN3_HOST = "localhost";

system_or_bail 'pgxc_ctl', 'prepare', 'config', 'empty' ;

system_or_bail 'pgxc_ctl', 'add', 'gtm', 'master', 'gtm', "$GTM_HOST", '20001', "$dataDirRoot/gtm" ;

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord1', "$COORD1_HOST", '30001', '30011', "$dataDirRoot/coord_master.1", 'none', 'none';

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord2', "$COORD2_HOST", '30002', '30012', "$dataDirRoot/coord_master.2", 'none', 'none';

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn1', "$DN1_HOST", '40001', '40011', "$dataDirRoot/dn_master.1", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn2', "$DN2_HOST", '40002', '40012', "$dataDirRoot/dn_master.2", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn3', "$DN3_HOST", '40003', '40013', "$dataDirRoot/dn_master.3", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord3', "$COORD3_HOST", '30003', '30013', "$dataDirRoot/coord_master.3", 'none', 'none' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'remove', 'coordinator', 'master', 'coord3', 'clean' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'remove', 'datanode', 'master', 'dn3', 'clean' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#Datanode slave test

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'slave', 'dn1', "$DN1_HOST", '40101', '40111', "$dataDirRoot/dn_slave.1", 'none', "$dataDirRoot/datanode_archlog.1" ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'stop', "-m", 'immediate', 'datanode', 'master', 'dn1' ;

system_or_bail 'pgxc_ctl', 'failover', 'datanode', 'dn1' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#GTM standby test

system_or_bail 'pgxc_ctl', 'add', 'gtm', 'slave', 'gtm_slave', "$GTM_HOST", '20101', "$dataDirRoot/gtm_slave" ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'stop', "-m", 'immediate', 'gtm', 'master', 'gtm' ;

system_or_bail 'pgxc_ctl', 'failover', 'gtm', 'gtm' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#add cleanup
system_or_bail 'pgxc_ctl', 'clean', 'all' ;


#delete related dirs for cleanup
system("rm -rf $dataDirRoot");
system("rm -rf $PGXC_CTL_HOME");

system_or_bail 'pgxc_ctl', 'prepare', 'config', 'empty' ;

system_or_bail 'pgxc_ctl', 'add', 'gtm', 'master', 'gtm', "$GTM_HOST", '20001', "$dataDirRoot/gtm" ;

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord1', "$COORD1_HOST", '30001', '30011', "$dataDirRoot/coord_master.1", 'none', 'none';

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord2', "$COORD2_HOST", '30002', '30012', "$dataDirRoot/coord_master.2", 'none', 'none';

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn1', "$DN1_HOST", '40001', '40011', "$dataDirRoot/dn_master.1", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn2', "$DN2_HOST", '40002', '40012', "$dataDirRoot/dn_master.2", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn3', "$DN3_HOST", '40003', '40013', "$dataDirRoot/dn_master.3", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord3', "$COORD3_HOST", '30003', '30013', "$dataDirRoot/coord_master.3", 'none', 'none' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'remove', 'coordinator', 'master', 'coord3', 'clean' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'remove', 'datanode', 'master', 'dn3', 'clean' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#Datanode slave test

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'slave', 'dn1', "$DN1_HOST", '40101', '40111', "$dataDirRoot/dn_slave.1", 'none', "$dataDirRoot/datanode_archlog.1" ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'stop', "-m", 'immediate', 'datanode', 'master', 'dn1' ;

system_or_bail 'pgxc_ctl', 'failover', 'datanode', 'dn1' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#GTM standby test

system_or_bail 'pgxc_ctl', 'add', 'gtm', 'slave', 'gtm_slave', "$GTM_HOST", '20101', "$dataDirRoot/gtm_slave" ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'stop', "-m", 'immediate', 'gtm', 'master', 'gtm' ;

system_or_bail 'pgxc_ctl', 'failover', 'gtm', 'gtm' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#add cleanup
system_or_bail 'pgxc_ctl', 'clean', 'all' ;


#delete related dirs for cleanup
system("rm -rf $dataDirRoot");
system("rm -rf $PGXC_CTL_HOME");

system_or_bail 'pgxc_ctl', 'prepare', 'config', 'empty' ;

system_or_bail 'pgxc_ctl', 'add', 'gtm', 'master', 'gtm', "$GTM_HOST", '20001', "$dataDirRoot/gtm" ;

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord1', "$COORD1_HOST", '30001', '30011', "$dataDirRoot/coord_master.1", 'none', 'none';

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord2', "$COORD2_HOST", '30002', '30012', "$dataDirRoot/coord_master.2", 'none', 'none';

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn1', "$DN1_HOST", '40001', '40011', "$dataDirRoot/dn_master.1", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn2', "$DN2_HOST", '40002', '40012', "$dataDirRoot/dn_master.2", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn3', "$DN3_HOST", '40003', '40013', "$dataDirRoot/dn_master.3", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord3', "$COORD3_HOST", '30003', '30013', "$dataDirRoot/coord_master.3", 'none', 'none' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'remove', 'coordinator', 'master', 'coord3', 'clean' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'remove', 'datanode', 'master', 'dn3', 'clean' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#Datanode slave test

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'slave', 'dn1', "$DN1_HOST", '40101', '40111', "$dataDirRoot/dn_slave.1", 'none', "$dataDirRoot/datanode_archlog.1" ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'stop', "-m", 'immediate', 'datanode', 'master', 'dn1' ;

system_or_bail 'pgxc_ctl', 'failover', 'datanode', 'dn1' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#GTM standby test

system_or_bail 'pgxc_ctl', 'add', 'gtm', 'slave', 'gtm_slave', "$GTM_HOST", '20101', "$dataDirRoot/gtm_slave" ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'stop', "-m", 'immediate', 'gtm', 'master', 'gtm' ;

system_or_bail 'pgxc_ctl', 'failover', 'gtm', 'gtm' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#add cleanup
system_or_bail 'pgxc_ctl', 'clean', 'all' ;


#delete related dirs for cleanup
system("rm -rf $dataDirRoot");
system("rm -rf $PGXC_CTL_HOME");

system_or_bail 'pgxc_ctl', 'prepare', 'config', 'empty' ;

system_or_bail 'pgxc_ctl', 'add', 'gtm', 'master', 'gtm', "$GTM_HOST", '20001', "$dataDirRoot/gtm" ;

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord1', "$COORD1_HOST", '30001', '30011', "$dataDirRoot/coord_master.1", 'none', 'none';

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord2', "$COORD2_HOST", '30002', '30012', "$dataDirRoot/coord_master.2", 'none', 'none';

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn1', "$DN1_HOST", '40001', '40011', "$dataDirRoot/dn_master.1", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn2', "$DN2_HOST", '40002', '40012', "$dataDirRoot/dn_master.2", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn3', "$DN3_HOST", '40003', '40013', "$dataDirRoot/dn_master.3", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord3', "$COORD3_HOST", '30003', '30013', "$dataDirRoot/coord_master.3", 'none', 'none' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'remove', 'coordinator', 'master', 'coord3', 'clean' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'remove', 'datanode', 'master', 'dn3', 'clean' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#Datanode slave test

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'slave', 'dn1', "$DN1_HOST", '40101', '40111', "$dataDirRoot/dn_slave.1", 'none', "$dataDirRoot/datanode_archlog.1" ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'stop', "-m", 'immediate', 'datanode', 'master', 'dn1' ;

system_or_bail 'pgxc_ctl', 'failover', 'datanode', 'dn1' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#GTM standby test

system_or_bail 'pgxc_ctl', 'add', 'gtm', 'slave', 'gtm_slave', "$GTM_HOST", '20101', "$dataDirRoot/gtm_slave" ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'stop', "-m", 'immediate', 'gtm', 'master', 'gtm' ;

system_or_bail 'pgxc_ctl', 'failover', 'gtm', 'gtm' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#add cleanup
system_or_bail 'pgxc_ctl', 'clean', 'all' ;


#delete related dirs for cleanup
system("rm -rf $dataDirRoot");
system("rm -rf $PGXC_CTL_HOME");


system_or_bail 'pgxc_ctl', 'prepare', 'config', 'empty' ;

system_or_bail 'pgxc_ctl', 'add', 'gtm', 'master', 'gtm', "$GTM_HOST", '20001', "$dataDirRoot/gtm" ;

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord1', "$COORD1_HOST", '30001', '30011', "$dataDirRoot/coord_master.1", 'none', 'none';

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord2', "$COORD2_HOST", '30002', '30012', "$dataDirRoot/coord_master.2", 'none', 'none';

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn1', "$DN1_HOST", '40001', '40011', "$dataDirRoot/dn_master.1", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn2', "$DN2_HOST", '40002', '40012', "$dataDirRoot/dn_master.2", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn3', "$DN3_HOST", '40003', '40013', "$dataDirRoot/dn_master.3", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord3', "$COORD3_HOST", '30003', '30013', "$dataDirRoot/coord_master.3", 'none', 'none' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'remove', 'coordinator', 'master', 'coord3', 'clean' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'remove', 'datanode', 'master', 'dn3', 'clean' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#Datanode slave test

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'slave', 'dn1', "$DN1_HOST", '40101', '40111', "$dataDirRoot/dn_slave.1", 'none', "$dataDirRoot/datanode_archlog.1" ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'stop', "-m", 'immediate', 'datanode', 'master', 'dn1' ;

system_or_bail 'pgxc_ctl', 'failover', 'datanode', 'dn1' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#GTM standby test

system_or_bail 'pgxc_ctl', 'add', 'gtm', 'slave', 'gtm_slave', "$GTM_HOST", '20101', "$dataDirRoot/gtm_slave" ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'stop', "-m", 'immediate', 'gtm', 'master', 'gtm' ;

system_or_bail 'pgxc_ctl', 'failover', 'gtm', 'gtm' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#add cleanup
system_or_bail 'pgxc_ctl', 'clean', 'all' ;


#delete related dirs for cleanup
system("rm -rf $dataDirRoot");
system("rm -rf $PGXC_CTL_HOME");


system_or_bail 'pgxc_ctl', 'prepare', 'config', 'empty' ;

system_or_bail 'pgxc_ctl', 'add', 'gtm', 'master', 'gtm', "$GTM_HOST", '20001', "$dataDirRoot/gtm" ;

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord1', "$COORD1_HOST", '30001', '30011', "$dataDirRoot/coord_master.1", 'none', 'none';

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord2', "$COORD2_HOST", '30002', '30012', "$dataDirRoot/coord_master.2", 'none', 'none';

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn1', "$DN1_HOST", '40001', '40011', "$dataDirRoot/dn_master.1", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn2', "$DN2_HOST", '40002', '40012', "$dataDirRoot/dn_master.2", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'master', 'dn3', "$DN3_HOST", '40003', '40013', "$dataDirRoot/dn_master.3", 'none', 'none', 'none' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'add', 'coordinator', 'master', 'coord3', "$COORD3_HOST", '30003', '30013', "$dataDirRoot/coord_master.3", 'none', 'none' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'remove', 'coordinator', 'master', 'coord3', 'clean' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'remove', 'datanode', 'master', 'dn3', 'clean' ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#Datanode slave test

system_or_bail 'pgxc_ctl', 'add', 'datanode', 'slave', 'dn1', "$DN1_HOST", '40101', '40111', "$dataDirRoot/dn_slave.1", 'none', "$dataDirRoot/datanode_archlog.1" ;
system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'stop', "-m", 'immediate', 'datanode', 'master', 'dn1' ;

system_or_bail 'pgxc_ctl', 'failover', 'datanode', 'dn1' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#GTM standby test

system_or_bail 'pgxc_ctl', 'add', 'gtm', 'slave', 'gtm_slave', "$GTM_HOST", '20101', "$dataDirRoot/gtm_slave" ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

system_or_bail 'pgxc_ctl', 'stop', "-m", 'immediate', 'gtm', 'master', 'gtm' ;

system_or_bail 'pgxc_ctl', 'failover', 'gtm', 'gtm' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#add cleanup
system_or_bail 'pgxc_ctl', 'clean', 'all' ;


#delete related dirs for cleanup
system("rm -rf $dataDirRoot");
system("rm -rf $PGXC_CTL_HOME");
