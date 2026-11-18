#!/usr/bin/env perl
use threads; use threads::shared;

use FindBin qw($Bin);
use lib $Bin;
use t_Common qw/oops btw/; # strict, warnings, Carp
use t_TestCommon # Test2::V0 etc.
  qw/$silent $verbose $debug run_perlscript ts tsstr timed_run_hresult/;

STDERR->autoflush();
STDOUT->autoflush();

use POSIX ();
use Time::HiRes qw/sleep/;

use feature 'signatures';

#################################################################
# Check that jobs actually run on concurrent threads
#################################################################

use Threads::DependentJobs;
Threads::DependentJobs->Configure(DEBUG => $debug);

use constant NT => 2;

use constant MIN_INTERVAL_SECS => 1;
use constant SLEEP_SECS => int( (NT+7)/8 ) / MIN_INTERVAL_SECS; # 1 up to 8 threads, 2 up to 16 etc.
use constant SPIN_SECS => SLEEP_SECS / 2;

#----------------------------------------------------------
sub _inner_spin($count) {
  for (my $n=0; $n < $count; $n++) {
    my $t=0; for (my $i=0;$i<10000;++$i) { 
      $t += (((((((((($i*1.001)/1.01)/1)/123)/1)/1)*1)*1)*1)*1); 
    }
  }
}
use constant CYCLES_PER_SPIN => do{ # calibrate CPU speed
  _inner_spin(10); # warmup
  my $t0 = Time::HiRes::time();
  my $tot;
  for ($tot=0; ;$tot++) {
    _inner_spin(1);
    my $t1 = Time::HiRes::time();
    last if ($t1-$t0) >= SPIN_SECS/10;
  }
  ($tot+1) * 10
};
sub spin() { 
  my $t0 = Time::HiRes::time();
  _inner_spin(CYCLES_PER_SPIN); 
  my $t1 = Time::HiRes::time();
  note "## spin actutal dwell = ", $t1-$t0, "\n";
}
if (1 or $debug) {
  note "NT = ",NT,"\n";
  note "MIN_INTERVAL_SECS = ",MIN_INTERVAL_SECS,"\n";
  note "SLEEP_SECS = ",SLEEP_SECS,"\n";
  note "CYCLES_PER_SPIN = ",CYCLES_PER_SPIN,"\n";
  note "SPIN_SECS = ",SPIN_SECS,"\n";
} 
#----------------------------------------------------------

#note "AAA"; spin() for (1..10); note "BBB";

my $which_tid :shared = shared_clone([ (undef) x NT ]);
my $tstart_times :shared = shared_clone([ (undef) x NT ]);
my $tpostspin_times :shared = shared_clone([ (undef) x NT ]);
my $tpostsleep_times :shared = shared_clone([ (undef) x NT ]);

my $jobsch = Threads::DependentJobs->new(NUMTHREADS => NT);

my $base_time = tsstr();

my @jobids = map{ $jobsch->sched(subname => "jobfunc", args => [$_]) } 0..(NT-1);
$jobsch->wait2();

like([sort { $a <=> $b } @$which_tid], [1..NT]);

note dvis '$base_time\n';
note dvis '$tstart_times\n';
note dvis '$tpostspin_times\n';
note dvis '$tpostsleep_times\n';

foreach my $tn (1..NT) {
  ok( $tstart_times->[$tn-1] < $base_time+0.2,
      "Thread $tn start time:     ".$tstart_times->[$tn-1]." < base_time+0.2" );

  my $spindelta = $tpostspin_times->[$tn-1] - $tstart_times->[$tn-1];

  ok( $spindelta >= 0.8*SPIN_SECS, 
      "Thread $tn postspin delta: ".$spindelta." >= 0.8*SPIN_SECS(=".(0.8*SPIN_SECS).")" );

  my $sleepdelta = $tpostsleep_times->[$tn-1] - $tpostspin_times->[$tn-1];

  ok( $sleepdelta >= 0.95*SLEEP_SECS, 
      "Thread $tn postsleep delta:".$sleepdelta." >= 0.95*SLEEP_SECS(=".(0.95*SLEEP_SECS).")" );
}

done_testing;
exit 0;

sub jobfunc(%params) {
  my $tid = threads->tid;
  my $cputask = do{};
  my $ix = $params{args}->[0] // oops;
  { lock $which_tid;
    $which_tid->[$ix] = $tid;
    $tstart_times->[$ix] = tsstr();
  }
  note tsstr," ## [$tid] spin...";
  spin();
  note tsstr," ## [$tid] after.";
  { #lock $which_tid;
    $tpostspin_times->[$ix] = tsstr();
  }
  sleep SLEEP_SECS;
  lock $which_tid;
  $tpostsleep_times->[$ix] = tsstr();
}

