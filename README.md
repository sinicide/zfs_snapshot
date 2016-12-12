# zfs_snapshot

Description:
A one file Perl Script to handle Periodic ZFS Snapshots. After searching for ZFS Snapshot solutions, most of which I found were either ZFS on Linux or actual Solaris, which I was not running. Since I'm using OpenIndiana, I need a solution that fit my situation, hence this one file Perl Script to generate ZFS Snapshots on a periodic basis.

This is my first real perl script, so please forgive some of the concepts of handling time and/or method of retention. Any and all improvement suggestions are welcomed!

What it does:
- Creates Periodic Snapshots for all zpools, based on the snaps you want to retain
- Delete old snapshots that are no longer part of your rentention.
- Can ignore a specified zpools
- CANNOT ignore specified datasets!
- Create Log Rotation
- Logs execution of script when ran

Instructions:
Script should be executed as ROOT. I wanted to ensure that regular users can't execute it if they don't have access to create snapshots on a zpool. 
Simply schedule a cronjob every 15 mintues to execute the script. The script will take care of checking against previous snapshots and creating a new one when the threshold is met.

# sample cronjob for Solaris/OpenIndiana
0,15,30,45 * * * * /bin/perl /location/to/zfs_snapshot.pl
