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

system_or_bail 'pgxc_ctl', 'prepare', 'minimal' ;

system_or_bail 'pgxc_ctl', 'init', 'all' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#add cleanup
system_or_bail 'pgxc_ctl', 'clean', 'all' ;

#delete related dirs for cleanup
system("rm -rf $dataDirRoot");
system("rm -rf $PGXC_CTL_HOME");
