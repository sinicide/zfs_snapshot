#!/usr/bin/perl

# This perl script will create and delete snapshots based on frequency and retention time, allowing snapshot timeline going back hourly, daily, weekly, monthyl and etc.
# instructions:
# Modify variables and frequency of snaps to meet your needs, add an additional array item to @snaps if you want to have a yearly snapshot, my needs do not go beyond one monthly snapshot.
# schedule this script as a cronjob that runs every 15 minutes or so
#
# zfs snapshot, specific to Solaris OpenIndiana in my case.
# 0,15,30,45 * * * * /bin/perl /location/to/zfs_snapshot.pl
#
# on Openindiana add the following to /etc/logadm.conf if you want to rotate logs
# /var/log/zfs/snapshot/zfs_snapshot.log -C 14 -c -p 1d -t /var/log/zfs/snapshot/snapshot.log.%Y-%m-%d /var/log/zfs/snapshot/snapshot/snapshot.log

# written in perl 5, version 16, subversion 1 (v5.16.1)

use strict;
use warnings;
use POSIX;
use Time::Piece;

# Snaps I want to maintain
my @snaps = (
	{
		frequency => "frequent",	# Frequency Name
		rotateTo => "hourly",		# Next Frequency Name, that this will rotate to, leave blank if nothing to rotate to
		retentionThreshold => 4,	# Number of snapshots to retain before rotation
		retentionTime => 58,		# Time in Minutes before rotation
		rotationTime => 58,             # Time before rotating into next Frequency
	},
	{
		frequency => "hourly",
		rotateTo => "daily",
		retentionThreshold => 8,
		retentionTime => 1440,
		rotationTime => 480,
	},
	{
		frequency => "daily",
		rotateTo => "weekly",
		retentionThreshold => 7,
		retentionTime => 10080,
		rotationTime => 10080,
	},
	{
		frequency => "weekly",
		rotateTo => "monthly",
		retentionThreshold => 4,
		retentionTime => 43200,
		rotationTime => 43200,
	},
	{
		frequency => "monthly",
		rotateTo => "",
		retentionThreshold => 1,
		retentionTime => 43200,
		rotationTime => 43200,
	}
);

# zpools to be ignored, in my case, I don't really care to snapshot rpool, not much changes in the main OS Filesystem, so manual backups every so often is fine.
my @ignoredPools = ('rpool');

# unused currently, future goal would be to specify dataSets to be ignored from Snapshot Process
my @ignoredDataSets = ();

# Minimum time in which a Snapshot can be created since last, in Minutes
my $snapshotCreationThreshold = 14;

# Above parameter converted into seconds for use later, DO NOT Modify
$snapshotCreationThreshold *= 60;

# Gets Current Time value, when script is executed
my $currentTimeString = localtime->strftime("%Y-%m-%d-%H:%M:%S");

# Converts string to a time format that can be manipulated
my $currentTimeDate = Time::Piece->strptime($currentTimeString, "%Y-%m-%d-%H:%M:%S");

# Logs directory
my $logdirectory = "/var/log/zfs/snapshot";

# Log File
my $logfile = "${logdirectory}/snapshot.log";

# Log rotation command
my $logrotatecmd = "\n${logfile} -C 14 -c -p 1d -t ${logfile}.%Y-%m-%d ${logfile}\n\n";


# must be ran as root
die "This script must be ran as root\n" if $< != 0;

# create log file if missing
unless (-e $logfile) {
	print "Log File does not exist at ${logfile}\n";
	print "Creating Log File now...\n";
	system(`/usr/bin/mkdir -p ${logdirectory} && /usr/bin/touch ${logfile}`);
	print "Log File created!\n";
}

sub addLogRotation {
	my $ostype = `/sbin/uname`;
	chomp $ostype;
	
	# if Solaris
	if ( lc($ostype) eq "sunos" ) {
		# check for /etc/logadm.conf
		if ( -e '/etc/logadm.conf' && -R '/etc/logadm.conf' ) {
			# grep .conf for any indication of snapshot.log
			if ( `cat /etc/logadm.conf | grep -ic $logfile` == 0 ) {
			
				# backup logadm.conf
				system(`cp /etc/logadm.conf /etc/logadm.conf.bk`);
				
				# update existing file
				open my $fh, ">>", '/etc/logadm.conf' or die("Could not open /etc/logadm.conf for writing. $!");
				print $fh $logrotatecmd;
				close $fh;
			}
		}
	}
}

open(STDOUT, "| tee -ai $logfile") or die $!;

sub start {

	# gather a list of all pools
	my @zfsvolumes = `/sbin/zpool list -H -o name`;
	# removes new lines
	chomp @zfsvolumes;
	
	# cycle through each ZFS Pool
	foreach my $pool (@zfsvolumes) {
	
		# remove new lines
		chomp $pool;
		
		# if $pool is not an ignored pool, being the snapshot process
		unless ( grep { $pool eq $_ } @ignoredPools ) {
			
			# $pool does not match any of the @ignoredPools
			print "Checking for Datasets in: ",uc($pool),"\n";
			
			# get all dataSets for given pool
			my @currentDataSets = `/sbin/zfs list -r -H -o name $pool`;
			chomp @currentDataSets;
			
			# check to ensure dataSets exist for given $pool
			if (@currentDataSets) {
				
				print "Cycling through DataSets in: ",uc($pool),"\n";
				
				# cycle through current dataSet and only process end level dataSets
				foreach my $dataSet (@currentDataSets) {
				
					chomp $dataSet;
					
					# if it only matches against it's self, let's proceed
					unless ( (grep { $_ =~ /$dataSet/ } @currentDataSets) > 1 ) {
						
						print "\nProcessing DataSet: ",uc($dataSet),"\n";
			
						# Call createSnapshot on specific $dataSet
						createSnapshot($snaps[0]->{frequency}, $dataSet);
						
						# Calling Snapshot Rotation on specific $dataSet for each snap
						foreach my $snap (@snaps) {
							rotateSnapshot($snap->{frequency}, $snap->{rotateTo}, $dataSet, $snap->{retentionThreshold}, $snap->{retentionTime});
						}
					}
				}
			} else {
				# no dataSets exist for given $pool
				print "No dataSets exist for ",uc($pool)," so skipping...\n";
			}
		}
	}
}

sub findSnap {

	my ($youngOrOld, $dataSet, $frequency, $snapshot_ref) = @_;
	my @snapshots = @$snapshot_ref;

	# Uncomment for debugging
	#print "YoungOrOld: $youngOrOld\n";
	#print "DataSet: $dataSet\n";
	#print "Frequency: $frequency\n";
	
	# set youngest/oldest date
	my $targetSnap = $snapshots[0];
	# strip everything but the date/time
	$targetSnap =~ s/$dataSet\@bk_$frequency-//;
	# convert it to a date time format to be manipulated
	my $targetSnapDate = Time::Piece->strptime($targetSnap, "%Y-%m-%d-%H:%M:%S");
	
	# check each snapshot to find youngest/oldest date
	foreach my $currentSnap (@snapshots) {
	
		# strip everything but the date/time from the current snapshot
		$currentSnap =~ s/$dataSet\@bk_$frequency-//;
		# convert it to a date time format to be manipulated
		my $currentSnapDate = Time::Piece->strptime($currentSnap, "%Y-%m-%d-%H:%M:%S");
		
		# if youngest or oldest do the following
		if ( $youngOrOld eq "youngest" ) {
			# Uncomment for debugging
			#print "Youngest check\nCurrentSnapDate: $currentSnapDate\ntargetSnapDate: $targetSnapDate\n";
			if ( $currentSnapDate > $targetSnapDate ) {
				# Uncomment for debugging
				#print "Current Snapshot is younger then Youngest Snapshot so far.\n";
				$targetSnap = $currentSnap;
				$targetSnapDate = Time::Piece->strptime($targetSnap, "%Y-%m-%d-%H:%M:%S");
			}
		} elsif ( $youngOrOld eq "oldest" ) {
			# Uncomment for debugging
			#print "Oldest check\nCurrentSnapDate: $currentSnapDate\ntargetSnapDate: $targetSnapDate\n";
			if ( $currentSnapDate < $targetSnapDate ) {
				# Uncomment for debugging
				#print "Current Snapshot is older then Oldest Snapshot so far.\n";
				$targetSnap = $currentSnap;
				$targetSnapDate = Time::Piece->strptime($targetSnap, "%Y-%m-%d-%H:%M:%S");
			}
		} else {
			die "cannot determine which snap you want to find\n";
		}
	}
	
	# Uncomment for debugging
	#print "targetSnap: $targetSnap\n";
	#print "targetSnapDate: $targetSnapDate\n";
	
	return ($targetSnap, $targetSnapDate);
}

sub createSnapshot {

	# set passed in variables
	my ($frequency, $dataSet) = @_;
	
	print "Running createSnapshot on $dataSet: ",uc($frequency),"\n";
	
	# check to see if frequency snapshots is empty
	if ( `/sbin/zfs list -r -t snapshot -H -o name $dataSet | grep -c 'bk_$frequency'` > 0 ) {
		
		print "There are $frequency snapshots, so building array...\n";
		
		# get list of $frequency snapshot
		my @snapshots = `/sbin/zfs list -r -t snapshot -H -o name $dataSet | grep 'bk_$frequency'`;
		chomp @snapshots;
		
		# call findSnap and set the $youngestSnap and $youngestSnapDate
		my ($youngestSnap, $youngestSnapDate) = findSnap("youngest", $dataSet, $frequency, \@snapshots);
		# find the difference between $youngestSnapDate and $currentTimeDate
		my $snapDiff = $currentTimeDate - $youngestSnapDate;
		
		# check if snapshotCreationThreshold was exceeded
		if ( $snapDiff > $snapshotCreationThreshold ) {
			print "SnapshotCreation threshold was met!\n";
			system(`/sbin/zfs snapshot -r $dataSet\@bk_$frequency-$currentTimeString`);
			print "Snapshot created for $dataSet\@bk_$frequency-$currentTimeString\n";
		} else {
			# Difference was not met, snapshot not taken
			print "Only ", ceil( $snapDiff / 60 ), " Minutes since recent snapshot\n";
			print "Threshold was not met, Snapshot was not created\n";
		}
	
	} else {
		# No Snapshots exist for $frequency, so let's just generate one
		print "No Snapshots exist for $frequency, so creating one...\n";
		# take a snapshot
		system(`/sbin/zfs snapshot -r $dataSet\@bk_$frequency-$currentTimeString`);
		print "Snapshot created for $dataSet\@bk_$frequency-$currentTimeString\n";
	}
	
	# visual separator
	print "================================================================================\n";
	
	return;
}

sub rotateSnapshot {
	
	# set passed in variables
	my ($frequency, $rotateTo, $dataSet, $snapshotRetention, $retentionTime) = @_;
	# multiply retentionTime (in minutes) * 60 seconds to get retentionTime in seconds
	$retentionTime *= 60;

	# Uncomment to debug
	#print "Frequency: $frequency\n";
	#print "RotateTo: $rotateTo\n";
	#print "DateSet: $dataSet\n";
	#print "SnapshotRetention: $snapshotRetention\n";
	#print "RetentionTime: $retentionTime\n";
	
	print "Running rotateSnapshot on $dataSet: ",uc($frequency),"\n";
	
	# check to see if $snapshotRetention threshold was exceeded for $frequency snapshots
	if ( `/sbin/zfs list -r -t snapshot -H -o name $dataSet | grep -c 'bk_$frequency'` > $snapshotRetention ) {
		
		# $snapshotRetention threshold met
		print "SnapshotRetention threshold met!\n";

		# build array of existing snapshots for $frequency
		my @frequencySnapshots = `/sbin/zfs list -r -t snapshot -H -o name $dataSet | grep 'bk_$frequency'`;
		chomp @frequencySnapshots;
		
		# Set oldest date from frequency snapshots
		my ($oldestFreqSnap, $oldestFreqSnapDate) = findSnap("oldest", $dataSet, $frequency, \@frequencySnapshots);
		# calculate difference between oldest date vs current time
		my $freqDiff = $currentTimeDate - $oldestFreqSnapDate;
		
		# check if there are snapshots in rotateTo, if so compare oldest date snapshot to youngest date rotateTo
		# if no snapshots exist, then just rotate because we've already hit the maximum retention and snapshots are never generated everytime the script runs but only if threshold between current time and youngest snapshot has been met
		
		# check if there are snapshots in RotateTo
		if ( defined $rotateTo ) {
			# rotateTo is not empty
			if ( `/sbin/zfs list -r -t snapshot -H -o name $dataSet | grep -c 'bk_$rotateTo'` <= 0 ) {
				# $rotateTo snapshots are empty
				# rotate it since we've already hit the threshold here for snapshot retention and the new snapshot in rotateTo becomes our starting base
				print "$rotateTo snapshots are empty\n";
				print "rotating $frequency Snapshot into $rotateTo Snapshot\n";
				print "renaming... bk_$frequency-$oldestFreqSnap to bk_$rotateTo-$oldestFreqSnap\n";
				system(`/sbin/zfs rename $dataSet\@bk_$frequency-$oldestFreqSnap bk_$rotateTo-$oldestFreqSnap`);
			} else {
				# rotateTo snapshots are not empty
				# so perform a compare on oldest current snapshot with youngest rotateTo snapshot
				print "$rotateTo snapshots are NOT empty\n";
				# $rotateTo snapshots entries exist
				# build array of existing snapshots for $rotateTo
				my @rotateToSnapshots = `/sbin/zfs list -r -t snapshot -H -o name $dataSet | grep 'bk_$rotateTo'`;
				chomp @rotateToSnapshots;
						
				# find youngest $rotateTo snapshot
				my ($youngestRotateToSnap, $youngestRotateToSnapDate) = findSnap("youngest", $dataSet, $rotateTo, \@rotateToSnapshots);
				my $snapDiff = $oldestFreqSnapDate - $youngestRotateToSnapDate;

				# check to see if threshold between snapshots are met
				if ( $snapDiff >= $retentionTime ) {
					print "Oldest $frequency snapshot against Youngest $rotateTo snapshot threshold met!\n";
					print "renaming... bk_$frequency-$oldestFreqSnap to bk_$rotateTo-$oldestFreqSnap\n";
					system(`/sbin/zfs rename $dataSet\@bk_$frequency-$oldestFreqSnap bk_$rotateTo-$oldestFreqSnap`);
				} else {
					print "Oldest $frequency snapshot against Youngest $rotateTo snapshot threshold NOT met!\n";
					print "Deleted Snapshot $dataSet\@bk_$frequency-$oldestFreqSnap\n";
					system(`/sbin/zfs destroy $dataSet\@bk_$frequency-$oldestFreqSnap`);
				}
			}
		} else {
			# $rotateTo is null, no rotation for $frequency just delete oldest snapshot
			print "Nothing to rotate, deleting snapshot instead...\n";
			print "Deleted Snapshot $dataSet\@bk_$frequency-$oldestFreqSnap\n";
			system(`/sbin/zfs destroy $dataSet\@bk_$frequency-$oldestFreqSnap`);
		}

	} else {
		# retention not exceeded
		print "SnapshotRetention threshold not exceeded, No Snpashots will be rotated\n";
	}
	
	# visual separator
	print "================================================================================\n";
	
	return;
}

#####################################################################
# add logrotation if needed
addLogRotation;

print "\nDate: $currentTimeDate\n";
print "...Running zfs_snapshot.pl\n\n";

# start script
start;

print "\n...zfs_snapshot.pl completed\n";

close(STDOUT);
