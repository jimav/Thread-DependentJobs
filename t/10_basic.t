#!/usr/bin/env perl
use threads; use threads::shared;

use FindBin qw($Bin);
use lib $Bin;
use t_Common qw/oops/; # strict, warnings, Carp
use t_TestCommon # Test2::V0 etc.
  qw/$silent $verbose $debug run_perlscript/;

use feature 'signatures';

use Spreadsheet::Edit::Log qw/:btw :time set_logdest/;
btw_rjust_prefix(1);

use POSIX ();
use Time::HiRes qw/sleep/;
STDERR->autoflush();
STDOUT->autoflush();
set_logdest(\*STDOUT);

use Threads::DependentJobs;
Threads::DependentJobs->Configure(DEBUG => $debug);
use Thread::Semaphore ();

{
my $sema1 = Thread::Semaphore->new() // oops; # default count==1

my $jobsch = Threads::DependentJobs->new(NUMTHREADS => undef);
note "Created job scheduler having ",$jobsch->get_numthreads, " threads.\n";

note "Created ws ",addrvis($jobsch);

my $sema2 = Thread::Semaphore->new(2) // oops; # default count==1

my $j1 = $jobsch->sched(subname => "jobfunc", inputs => ["xxx"], semas => [$sema1]);

#btw visnew->dviso('$jobsch');

my $j2 = $jobsch->sched(subname => "bomb", outputs => ["xxx"], semas => [[$sema2,2,2], $sema1]);

my $j3 = $jobsch->sched(subname => "jobfunc", args=>[11..15], outputs => ["ccc"], inputs => ["bbb", "aaa"]);
my $j4 = $jobsch->sched(subname => "jobfunc", args=>[1..5], outputs => ["aaa"], inputs => []);
my $j5 = $jobsch->sched(subname => "jobfunc", args=>[6..10], outputs => ["bbb"], inputs => ["aaa"]);
#$jobsch->abort_job($j5);
#btw dvis '$j1 $j2 $j3 $j4 $j5';

}
note "Goodbye.";
exit 0;

sub jobfunc(%params) {
  my $tid = threads->tid;
  if (1 or $debug) {
    btw "## [$tid] jobfunc ###############################################\n",
        dvis('jobfunc: %params\n'),"wantarray=",u(wantarray);
  }
  sleep 1;
  (11,12,13,14)
}
sub bomb(%params) {
  my %unshared = (uuu1 => [33..36], uuu2 => "foo");
  sleep 0.5;
  die {msg => "death bomb: ", data => \%unshared};
}
