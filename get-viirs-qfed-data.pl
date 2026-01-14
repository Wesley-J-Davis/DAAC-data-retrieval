#!/usr/bin/perl
#
# PROGRAM: get_modis.pl (1) acquires the VIIRS data used by
# the aerosol assimilation (can be multiple VIIRS file types, 
# (2) stages this data in the areas defined, and then (3) 
# archives the observations.
#
# Based on an original script (get_fire.pl): 15Jul03 T. King
# Adapted by Y. Kondratyeva October/November 2018.
# 

# The setting of the options and the module lookup paths will
# be done first using the BEGIN subroutine.  This section of the
# program executes before the rest of the program is even compiled.
# This way, a new path set via the -P option can be used to locate
# the modules to include at compile time while the remainder of the
# program is compiled.

# FEB 2025 WJD - get_sci_viirs.pl was created from get_modis.pl
# Uses wget to grab SCIence quality data from a remote server
# Compares the data type against a string to detect if lvl 1 or lvl 2
# LEVEL 2 VIIRS (SNPP/NOAA20) VNP14IMG / VJ114IMG
# LEVEL 1 VIIRS (SNPP/NOAA20) VNP03IMG / VJ103IMG
#	Compares the data type against a string to detect if lvl 1 or no
#	Modifies original product by removing all data except for lat/long
#	Modifies directory permissions for output directory as per original requestor

BEGIN {

# Keep track of errors within BEGIN block.

   $die_away = 0;
# Initialize output listing location

   $opt_O = 0;
# make env vars readily available
#--------------------------------
use Env qw( FORT_CONVERT20 );


# This module contains the getopts() subroutine.

   use Getopt::Std;
   use Getopt::Long;

# Get options and arguments

   GetOptions ( 'e=s',\$opt_e,
                'E=s',\$opt_E,
                'P=s',\$opt_P,
                'R=s',\$opt_R,
                'O=s',\$opt_O,
                'L=s',\$opt_L,
                't=s',\$opt_t,
                'f',\$opt_f,
                'sched_cnfg=s',\$sched_cnfg,
                'sched_id=s',\$sched_id,
                'sched_synp=s',\$sched_synp,
                'sched_c_dt=s',\$sched_c_dt,
                'sched_dir=s',\$sched_dir,
                'sched_sts_fl=s',\$sched_sts_fl,
                'sched_hs=s',\$sched_hs );

# If get_sci_viirs.pl is initiated by the scheduler, construct table
# info. for "task_state" table of scheduler

   if ( defined( $sched_id ) )
   {
      $tab_status = 1;
      $tab_argv = "$sched_cnfg, $sched_id, $sched_synp, $sched_c_dt";
      $fl_name = "get_sci_viirs";
      $comd_wrt = "$sched_dir/utility/status";
      $args = "$fl_name COMPLETE $tab_argv $sched_dir";
   }

# Processing environment

   if ( defined( $opt_e ) ) {
       $env = $opt_e;
   } else {
       $env = "ops";
   }

# The pre-processing configuration file.

   if ( defined( $opt_E ) ) {
      $PREP_CONFIG_FILE = $opt_E;
   } else {
      $PREP_CONFIG_FILE = "DEFAULT";
   }

  print "INPUT PREP_CONFIG_FILE = $PREP_CONFIG_FILE\n";

# Lag time for real-time processing (for llk mode only)

   if ( defined( $opt_L ) ) { 
      $LAG_TIME = $opt_L;
   } else {
      $LAG_TIME = 3;
   }

# Path to directory containing other GEOS DAS programs.
# Directory $GEOSDAS_PATH/bin will be searched for these
# programs.

   if ( defined( $opt_P ) ) { 
      $GEOSDAS_PATH = $opt_P;
   } else {
      $GEOSDAS_PATH = "DEFAULT";
   }

# Location of run-time configuration file.

   if ( defined( $opt_R ) ) { 
      $RUN_CONFIG_FILE = $opt_R;
   } else {
      $RUN_CONFIG_FILE = "DEFAULT";
   }

# Load synoptic time.
# ?????
   print "opt_t=$opt_t \n";
   if ( defined( $opt_t ) ) { 
      $process_synoptic = $opt_t;
   }

# Location of run-time configuration file.

   if ( defined( $opt_t ) ) {
      $syntime = $opt_t;
   } else {
      $syntime = "hd";
   }

#####
# Flag for level 1 data to be trimmed
#   if ( defined( $opt_T ) ) {
#      $trim_data = $opt_T;
#   } else {
#      $trim_data = "0";
#   }
#####

# Satellite ID for the preprocessing run.

   $prep_ID=$ARGV[0];
   print "prep_ID = ARGV[0]\n";
   print "$prep_ID = $ARGV[0]\n";

   if ( defined( $sched_id ) ) {
      $err_ID=$sched_id;
   }
   else {
      $err_ID=$prep_ID;
   }

# Location for output listings

   if ( $opt_O ) { 
      system ("mkdir -p $opt_O");
      if ( -w "$opt_O" ) {
        $listing_file    = "$opt_O/SCI_viirs_${prep_ID}.$$.listing";
        $listing_file_gz    = "$opt_O/SCI_viirs_${prep_ID}.$$.listing_gz";
        print "Standard output redirecting to $listing_file\n";
        open (STDOUT, ">$listing_file");
        open (STDERR, ">&" . STDOUT);
      }
      else {
        print "$0: WARNING: $opt_O is not writable for listing.\n";
      }
   }else{
        $listing_file = "STDOUT"
   }

# Set usage flag.  it is expected that 
# llk will have no synoptic time in it's argument list.
   
   $u_flag = 0;
   if ( $#ARGV < 0 || $ARGV[0] eq 'help' ) {
          $u_flag = 1;
   }

   if ( $u_flag == 1 ) {
       print STDERR <<'ENDOFHELP';
Usage:

   get_sci_viirs.pl [-e] [-E Prep_Config] [-P GEOSDAS_Path] [-R Run_Config] [ -O output_location ] [-L lag_time ]  [ process_date ] [-t trim-lvl1-data ]

   Normal options and arguments:

   -e    Processing environment (default = ops)

   -f    Force flag.If no VIIRS data for a synoptic time, issue error,
         but keep on processing.If no data on remote machine(rget) for
         a synoptic time,or No input data for converter, - issue error,
         but keep on processing.

 
   -O output_location
         This is the full path to the output listings (both STDERR and STDOUT).

   -E Prep_Config
         The full path to the preprocessing configuration file.  This file contains
         parameters needed by the preprocessing control programs. If not given, a
         file named $HOME/$prep_ID/Prep_Config is used.  get_sci_viirs.pl exit with an
         error if neither of these files exist.

         The parameters set from this file are

         VIIRS_WORK_DIR
            The top level working directory in which to run the preprocessing.  A
            subdirectory with the name of the preprocessing ID is made in this 
            directory (i.e., $VIIRS_WORK_DIR/$prep_ID), and the work is done 
            there.

         VIIRS_STAGE_DIR
            The location in which to stage the VIIRS files for use by the DAS.

         VIIRS_ARCHIVE_LOC
            The location in which to archive the BUFR and binary files.  Files will be 
            stored in subdirectories according to their type and valid date.  As an 
            example, the BUFR file for March 1, 2008  will be stored under: 
            $VIIRS_ARCHIVE_LOC/ops/$prep_ID/fire/bfr/fire/Y2008/M03

         VIIRS_NO_ARCHIVE
            Flag to indicate that archiving is turned off

   	prep_ID
            Identification tag for this run of the VIIRS pre-processing.

   process_date
	/home/dao_ops/D_BOSS/local_stat
	Date in YYYYMMDD format to process.  If not given, then today's date (in terms of GMT) will be processed.

  -T trim-lvl1-data 
	1 or 0. 1 = yes, trim the data if lvl1 data present. 0 = no, do not trim the data even if lvl1 data present.
	Option to activate the trimming of lvl1 data using the utility trim_viirs.csh. This removed all data save lat/lon from the file and deposits the new file in the specified dir /css/viirs/data/Level1/data.trimmed/yyyy/ddd. 

   Options useful in development mode.  These are not necessary (and should not be
   used) when running this program in the usual operational environment.

   -P GEOSDAS_Path
         Path to directory containing other GEOS DAS programs.  The path is 
         $GEOSDAS_PATH, where $GEOSDAS_PATH/bin is the directory containing these
         programs.  If -P GEOSDAS_Path is given, then other required programs not 
         found in the directory where this program resides will be obtained from 
         subdirectories in GEOSDAS_Path - the subdirectory structure is assumed 
         to be the same as the operational subdirectory structure.  The default is 
         to use the path to the subdirectory containing this program, which is what 
         should be used in the operational environment.

   -R Run_Config
         Name of file (with path name, if necessary) to read to obtain the 
         run-time (execution) configuration parameters.  get_sci_viirs.pl reads this
         file to obtain configuration information at run time.  

         If given, get_sci_viirs.pl uses this file.  Otherwise, get_sci_viirs.pl looks for a 
         file named "Run_Config" in the user's home directory, then the 
         $GEOSDAS_PATH/bin directory.  $GEOSDAS_PATH is given by the -P option if
         set, or it is taken to be the parent directory of the directory in which this
         script resides.  It is an error if get_sci_viirs.pl does not find this file, 
         but in the GEOS DAS production environment, a default Run_Config file is always 
         present in the bin directory.

    -L   Lag Time
         This option is to be used in llk real time mode only.  This is the lag time, in 
         days, before the current date.

ENDOFHELP
      $die_away = 1;
   }


# This module locates the full path name to the location of this file.  Variable
# $FindBin::Bin will contain that value.

   use FindBin;

# This module contains the dirname() subroutine.

   use File::Basename;

# If default GEOS DAS path, set path to parent directory of directory where this
# script resides.  

   if ( $GEOSDAS_PATH eq "DEFAULT" ) {
      $GEOSDAS_PATH = dirname( $FindBin::Bin );
   }

# Set name of the bin directory to search for other programs needed by this one.

   $BIN_DIR = "$GEOSDAS_PATH/bin";

# Get the name of the directory where this script resides.  If it is different 
# than BIN_DIR, then this directory will also be included in the list of 
# directories to search for modules and programs.

   $PROGRAM_PATH = $FindBin::Bin;

# Now allow use of any modules in the bin directory, and (if not the same) the
# directory where this program resides.  (The search order is set so that
# the program's directory is searched first, then the bin directory.)

   if ( $PROGRAM_PATH ne $BIN_DIR ) {
      @SEARCH_PATH = ( $PROGRAM_PATH, $BIN_DIR );
   } else {
      @SEARCH_PATH = ( $BIN_DIR );
   }

# Set module environment for ncks system call.

   print "source g5_modules.\n";
   local @ARGV = ("$BIN_DIR");
   do "${BIN_DIR}/g5_modules_perl_wrapper";
   $rc = system("echo $BASEDIR");
   print "BASEDIR=$ENV{'BASEDIR'}\n";
   print "opt_t=$opt_t\n";

}	# End BEGIN

print "SEARCH_PATH=@SEARCH_PATH\n";
# Any reason to exit found during the BEGIN block?

if ( $die_away == 1 ) {
   exit 1;
}

# Include the directories to be searched for required modules.

use lib ( @SEARCH_PATH );

# Set the path to be searched for required programs.

$ENV{'PATH'} = join( ':', @SEARCH_PATH, $ENV{'PATH'} );

# This module contains the extract_config() subroutine.
use Extract_config;

# Archive utilities: gen_archive
use Arch_utils;

# This module contains the z_time(), dec_time() and date8() subroutines.
use Manipulate_time;

# Error logging utilities.
use Err_Log;

# This module contains the mkpath() subroutine.

use File::Path;
use File::Copy;
use File::Find;
# Record exit status to scheduler task_status file.
use Recd_State;

# This module contains the rget() routine.

use Remote_utils;

# This module contains the julian_day subroutine.

use Time::JulianDay;

#Initialize exit status

$exit_stat = 0;

# Write start message to Event Log

$err_time = "XXX";
err_log (0, "get_sci_viirs.pl", "$err_ID","$env","-1",
        {'err_desc' => "$err_ID get_sci_viirs.pl job running  has started - Standard output redirecting to $listing_file"});

# Use Prep_Config file under the preprocessing run's directory in the user's home directory
# as the default.

if ( "$PREP_CONFIG_FILE" eq "DEFAULT" ) {
   $PREP_CONFIG_FILE = "$ENV{'HOME'}/$prep_ID/Prep_Config";
}

# Does the Prep_Config file exist?  If not, die.
if ( ! -e "$PREP_CONFIG_FILE" ) {
    err_log (4, "get_sci_viirs.pl", "$err_time","$err_ID","-1",
	     {'err_desc' => "error $PREP_CONFIG_FILE not found while running  ."});
    recd_state($fl_name, "FAILED", $tab_argv, $sched_dir, $sched_sts_fl);
    die "error $PREP_CONFIG_FILE not found.";
}

# If date given, use that, 
# otherwise use today's date (GMT).

# LENA  was  $process_date = date8( $ARGV[1] );
# LENA  was  $process_date = date8( $ARGV[0] ); 

if ( $#ARGV >= 0 ) {
	$process_date = date8( $ARGV[1] );
} 
else {
	
# Get current date (YYYYMMDD) in GMT, and set the process date to be 
# $LAG_TIME days prior.  
	
	$process_date = ( z_time() )[0];
	($process_date, $process_time) = inc_time ($process_date, 0, -$LAG_TIME, 0);
}

# ++++++++++++++++++++++++++++++++++++++++++==

#  Extract list of file types.

( $VIIRS_TYPES = extract_config( "VIIRS_TYPES", $PREP_CONFIG_FILE, "NONE" ) )
   	ne "NONE" or die "(get_files) ERROR - can not set VIIRS_TYPES configuration value\n";

#  Loop over file types.  Load hash of input file locations.

foreach $key ( split(/,/, $VIIRS_TYPES) ) {

     	print "Processing $key\n";

     	( $VIIRS_REMOTE = extract_config( "VIIRS_${key}_REMOTE", $PREP_CONFIG_FILE, "NONE" ) )
       		ne "NONE" or die "(get_files) ERROR - can not set VIIRS_${key}_REMOTE configuration value\n";

     	( $VIIRS_STAGE_DIR = extract_config( "VIIRS_${key}_STAGE", $PREP_CONFIG_FILE, "NONE" ) )
       		ne "NONE" or die "(get_files) ERROR - can not set VIIRS_${key}_STAGE configuration value\n";

     	( $VIIRS_ARCHIVE_DIR = extract_config( "VIIRS_${key}_ARCHIVE", $PREP_CONFIG_FILE, "NONE" ) )
       		ne "NONE" or die "(get_files) ERROR - can not set VIIRS_${key}_ARCHIVE configuration value\n";

    	( $VIIRS_NUMLIMIT = extract_config( "VIIRS_${key}_NUMLIMIT", $PREP_CONFIG_FILE, "NONE" ) )
       		ne "NONE" or die "(get_files) ERROR - can not set VIIRS_${key}_NUMLIMIT configuration value\n";



     	$rfile{$key} = ${VIIRS_REMOTE};
     	$stage{$key} = ${VIIRS_STAGE_DIR};
     	$archive{$key} = ${VIIRS_ARCHIVE_DIR};
     	$nlimit{$key} = ${VIIRS_NUMLIMIT};
     
}

# Find the locations in which to stage and archive the BUFR files.
#( $VIIRS_STAGE_DIR = extract_config( "VIIRS_STAGE_DIR", $PREP_CONFIG_FILE, "NONE" ) ) ne "NONE"
#   or die "(get_sci_viirs.pl) ERROR - can not set VIIRS_STAGE_DIR configuration value\n";
# This is used only for listing files.
( $VIIRS_ARCHIVE_LOC = extract_config( "VIIRS_ARCHIVE_LOC", $PREP_CONFIG_FILE, "NONE" ) ) 
	ne "NONE" or die "(get_sci_viirs.pl) ERROR - can not set VIIRS_ARCHIVE_LOC configuration value\n";

$VIIRS_NO_ARCHIVE = extract_config( "VIIRS_NO_ARCHIVE", $PREP_CONFIG_FILE, "0" ); 

#Get the location of the Earth Token

( $EARTH_TOKEN_PATH = extract_config( "EARTH_TOKEN_PATH", $PREP_CONFIG_FILE, "NONE" ) ) 
	ne "NONE" or die "(get_sci_viirs.pl) ERROR - can not set EARTH_TOKEN_PATH configuration value\n";
print "Reading Earthdata token from ${EARTH_TOKEN_PATH}\n";

# Get the location, directory, and file names for the raw VIIRS binary data.

( $VIIRS_WGET = extract_config( "VIIRS_WGET", $PREP_CONFIG_FILE, "NONE" ) ) 
	ne "NONE" or die "(get_sci_viirs.pl) ERROR - can not set VIIRS_WGET configuration value\n";
if ($EARTH_TOKEN_PATH ne "NONE") {
      	open my $fh, '<', $EARTH_TOKEN_PATH or die "Cannot open file '$EARTH_TOKEN_PATH': $!\n";
      	my $EARTH_TOKEN = do { local $/; <$fh> };
      	close $fh;
      	$VIIRS_WGET =~ s/<<EARTH_TOKEN>>/$EARTH_TOKEN/;
      	$VIIRS_WGET =~ s/\n//;
 }


( $VIIRS_ACQUIRE_MACH = extract_config( "VIIRS_ACQUIRE_MACH", $PREP_CONFIG_FILE, "NONE" ) ) 
	ne "NONE" or die "(get_sci_viirs.pl) ERROR - can not set VIIRS_ACQUIRE_MACH configuration value\n";


# Get the name of the working directory for the observation preprocessing.


( $VIIRS_WORK_DIR = extract_config( "VIIRS_WORK_DIR", $PREP_CONFIG_FILE, "NONE" ) ) 
	ne "NONE" or die "(get_sci_viirs.pl) ERROR - can not set VIIRS_WORK_DIR configuration value\n";

( $NCKS_DIR = extract_config( "NCKS_DIR", $PREP_CONFIG_FILE, "NONE" ) )
	ne "NONE" or die "(get_sci_viirs.pl) ERROR - can not set NCKS_DIR configuration value\n";

##############################################


#-- SUBROUTINE GET BY SYNOPTIC ---------------------
#  This is for VIIRS   
#   Subroutine go through list of files for 1 day and sort them by synoptic time
#   Return list of files and count of files by synoptic time( 0z  - 21z)

sub count_bysyn_fire {
   	my (  $VIIRS_TYPES,$date_today,$syntime ) = @_;

#   	name_look =  MOD14.A2018290
#   	$name_look  =  'MOD14.A'.$date_today ;
        $name_look  =  ${VIIRS_TYPES}.'.A'.$date_today ;
        $name_look  = ${VIIRS_TYPES}.'*'.${date_today} ;

#  	$date_today  are in FORM YYYYDDD  ( jday)
#       return ($kday,$k00,$k06,$k12,$k18,$k21,$list_full,list00,$list06,$list12,$list18,$list21);
# Diff. from get_bysyn_mls(ozone), and mls_t_nrt  - we return $k21,$list21,$file21( current day) and separately from $k00, ...)
        $kday=  0;
        $k21=  0;
        $k00=  0;
        $k06=  0;
        $k12=  0;
        $k18=  0;
    	$list_full =' ';

    	$list00 =' ';
    	$list06 =' ';
    	$list12 =' ';
    	$list18 =' ';
    	$list21 =' ';

     	$file00 = ' ';
     	$file06 = ' ';
     	$file12 = ' ';
     	$file18 = ' ';
     	$file21 = ' ';

       	@full_list = ();
       	@full_list00 = ();
       	@full_list06 = ();
       	@full_list12 = ();
       	@full_list18 = ();
       	@full_list21 = ();

# --------------------------------------------
#  Files for VIIRS can be like
#             1         2         3         4         5
#   0123456789012345678901234567890
#   MOD14.A2018290.0035.006.NRT.hdf
# --------------------------------------------
     	if ( -e "file21") {
      		$rc=system("rm file21 ");
      		$rc=system("touch  file21 ");
     	}


     	if ( -e "file00") {
      		$rc=system("rm file00 ");
      		$rc=system("touch  file00 ");
     	}

     	if ( -e "file06") {
      		$rc=system("rm file06 ");
      		$rc=system("touch  file06 ");
     	}

     	if ( -e "file12") {
      		$rc=system("rm file12 ");
      		$rc=system("touch  file12 ");
     	}

     	if ( -e "file18") {
      		$rc=system("rm file18 ");
      		$rc=system("touch  file18 ");
     	}

#   	0123456789012345678901234567890
#   	MOD14.A2018290.0035.006.NRT.hdf

#     	while ( defined($nextname =  <MLS*>)) 
#     	while ( defined($nextname =  <MOD14.A2018290*>)) 
#    	while ( defined($nextname = <$name_look*>)) 

       	$name_look  = ${VIIRS_TYPES}.'.A'.${date_today} ;
     	while ( defined($nextname = <${name_look}.*>)) {
		print "$nextname = $nextname\n";
	     	$nextname =~s#.*/##;  # remove part before last  slash
      		$gos ="$nextname";

#  	Files for VIIRS can be like
#             1         2         3         4         5
#   0123456789012345678901234567890
#   MOD14.A2018290.0035.006.NRT.hdf
	       	print "gos = $gos\n";

      		@file_parts = split ( /\./, $gos );
      		$gap_hour=substr( @file_parts[2],0,2);
   		print "date_hour = $gap_hour\n";
	       	print "current=$date_current today=$date_today\n";
#       	if ( $date_current == $date_today) {
      			$kday = $kday +1 ; 
      			$list_full="$list_full $nextname";
      			@full_list = (@full_list,${nextname});
#       	}


 		if ( $syntime ne  'hd') {
       			if ( $gap_hour < 3) {
         			$list00="$list00 $nextname";
         			$file00[$k00]= $nextname;
# 				@full_list = (@full_list,${fire_file});
         			@full_list00 = (@full_list00,${nextname});
         			$k00= $k00 +1;
        		}

    			if ( $gap_hour < 9 && $gap_hour >= 3 ) {

         			$list06="$list06 $nextname";
         			$file06[$k06]= $nextname;
         			@full_list06 = (@full_list06,${nextname});
         			$k06= $k06 +1;
     			}
     			if ( $gap_hour < 15 && $gap_hour >= 9) {

         			$list12="$list12 $nextname";
         			$file12[$k12]= $nextname;
         			@full_list12 = (@full_list12,${nextname});
         			$k12= $k12 +1;
     			}

     			if ( $gap_hour < 21 && $gap_hour >= 15) {
         			$list18="$list18 $nextname";
         			$file18[$k18]= $nextname;
         			@full_list18 = (@full_list18,${nextname});
         			$k18= $k18 +1;
     			}
      			if  ($gap_hour >= 21  ) {

         			$list21="$list21 $nextname";
         			$file21[$k21]= $nextname;
          			@full_list21 = (@full_list21,${nextname});
        			$k21= $k21 +1;

       			}

#  end  for if syntime  ne hd 
   		}

#  END  LOOP BY inlist ( input files)
	}

	$k00= $k00 + $k21;
	return ($kday,$k00,$k06,$k12,$k18,$k21,$list_full,$list00,$list06,$list12,$list18,$list21,@full_list,@full_list00,@full_list06,@full_list12,@full_list18,@full_list21);

}
#--END OF SUBROUTINE count_bysyn_fire  GET BY SYNOPTIC ---------------------
# ---------------------------------------
#############################################################
# -----------------------------------------------
#  Assemble storage directory names 
# -----------------------------------------------
$k=0;
$file_list =' ';
#----------------------------------------------------------
#   Assign FVROOT  for  'g5_modules_perl_wrapper'
# to get all libraries for amsu_to_bufr.
#----------------------------------------------------------

#  $ENV{"G5MOD_DIR"} =  $VIIRS_G5MOD_DIR;
#  do 'source_g5modules.pl' ;

# Get the work path and

# Make it.  (mkpath default mode is 0777, which is what we want.)

if ( ! -d "$VIIRS_WORK_DIR/$prep_ID" ) { 
    mkpath( "$VIIRS_WORK_DIR/$prep_ID" ) or die "Cannot make $VIIRS_WORK_DIR/$prep_ID"; 
}


# Change into WORK directory itc

chdir "$VIIRS_WORK_DIR/$prep_ID" or die "Cannot cd to $VIIRS_WORK_DIR/$prep_ID: $!\n";

#**********************#
# Start the processing #
#**********************#

# Resolve the process_date , and break up the path name into the directory and 
# file name


# We need files from the current and the previous day to create  BUFR file 
#  for  synoptic time= 0 of VIIRS data).

#**********************#
# Start the processing #
#**********************#


print " process_date =$process_date \n" ;
$a9='[0-9]';
# CHANGE  LENA
#  $hour = $process_synoptic;
$err_time = "${process_date}.${syntime}";
#  if ( $syntime eq 'hd') {
#           $err_time = "${process_date}";
#  } else {
#          $err_time = "${process_date}.${syntime}z";
#   }


#**********************#
#  Prepare ARCHIVE  directory name for NATIVE
#**********************#

$NATIVE_PATH  = $template_archive_native;
$NATIVE_PATH =~ s/%s/$env/;
$NATIVE_PATH =~ s/%s/$prep_ID/;
$ENV{FTP_PASSIVE}=1;

###################################################################
# Loop over defined VIIRS file types.
###################################################################

while (($key, $value) = each(%rfile)) {
     	print "key=$key $value\n";
     	$missing_count=0;
     	$loop_count=0;
	$data_gap=0;
     	@full_list = ();

# break up the path name into the directory and  file name

     	$VIIRS_ACQUIRE_PATH=$value;
     	@list = split('/',${VIIRS_ACQUIRE_PATH});
     	$list_len = @list;
     	${VIIRS_ACQUIRE_FILE} = $list[$list_len-1];
     	${VIIRS_ACQUIRE_DIR} = join('/',@list[0 .. $list_len-2]);
     	$knum21_prev = 0;
###########################################################
#    	If syntime = 0 , we will get all data for previous day
###########################################################
     	if ( $syntime eq '00') {
		( $process_date_m1, $current_time ) = inc_time ($process_date, $current_time, -1, 0);
		print " process_date 2  =$process_date \n" ;
	        print " syntime = 0 , so we process first previous  day=$process_date_m1 \n" ;
		$fire_stage = token_resolve("${stage{$key}}", $process_date_m1, "${hour}${min}00");
     
     		#account for the target dir for lvl1 data to .trimmed
     		#$fire_stage =~ s/<<TYPE>>/$key/;
     		print "fire_stage=$fire_stage\n";

     		if ( ! -d "$fire_stage" ) {
        		eval { mkpath( "$fire_stage" ) }; 
        		if ($@) {
           			err_log (4, "get_sci_viirs.pl", "$err_time","$err_ID","-1",
                  			{'err_desc' => "$err_ID: Cannot make $fire_stage"});
           			recd_state($fl_name, "FAILED", $tab_argv, $sched_dir, $sched_sts_fl);
           		die "Cannot make $fire_stage";
        		}
     		}  
		eval { chdir "$fire_stage" };
     		if ($@) {
        		err_log (4, "get_sci_viirs.pl", "$err_time","$err_ID","-1",
               			{'err_desc' => "$err_ID: Cannot cd to $fire_stage"});
        		recd_state($fl_name, "FAILED", $tab_argv, $sched_dir, $sched_sts_fl);
        		die "Cannot cd to $fire_stage";
     		}

		# Resolve  for current date to the directory and  file name
     		$fire_dir  = token_resolve("${VIIRS_ACQUIRE_DIR}", $process_date_m1, "${hour}${min}00");
     		$fire_file = token_resolve("${VIIRS_ACQUIRE_FILE}", $process_date_m1, "${hour}${min}00");
     		print "fire_file=$fire_file\n";
     		$want_files = "${VIIRS_ACQUIRE_MACH}.${fire_dir}/" ;
     		$want_files = "${VIIRS_ACQUIRE_MACH}${fire_dir}/" ;
     		print "Previous    VIIRS_WGET=$VIIRS_WGET ${want_files}\n";
     		$rc= system(" ${VIIRS_WGET} ${want_files}  ");
#		012345678901234567
#   		MOD14.A2018290.1240.006.NRT.hdf
#		$date_year =substr( $filename,7,4) ;
#		$date_jday =substr( $filename,11,3) ;
#		$date_today= $date_year.$date_jday ;
#		OR
#		$date_today =substr( $fire_file,7,7) ;
#   		$fire_file =  'MOD14.A2018302.0505.006.NRT.hdf' ;
#   		$filename = substr( $fire_file,0,14) ; 
#   		$date_yesterday =substr( $filename,7,7) ;
    		@file_parts = split (/\./, $fire_file);
    		$date_yesterday = substr( $file_parts[1],1,7);
    		print   "BEFORE count_bysyn_fire:  yesterday day  =  is $process_date_m1 \n";
    		print   "BEFORE count_bysyn_fire  fire_file =    is $fire_file \n";
    		print   "BEFORE count_bysyn_fire  date_yesterday =    is $date_yesterday \n";
    		print   "YESTERDAY processing type $file_parts[0]\n";
    		count_bysyn_fire ( $file_parts[0], $date_yesterday,$syntime);
    		print "YESTERDAY AFTER count_bysyn_fire  k00=    is $k00\n";
    		print "YESTERDAY AFTER count_bysyn_fire  k06=    is $k06\n";
    		print "YESTERDAY AFTER count_bysyn_fire  k12=    is $k12\n";
    		print "YESTERDAY AFTER count_bysyn_fire  k18=    is $k18\n";
    		print "YESTERDAY AFTER count_bysyn_fire  k21=    is $k21\n";
    		print "YESTERDAY AFTER count_bysyn_fire  Number of list_full=    is $kday\n";
#   		FOR Previous DAY
    		if ( $syntime eq '00') {
          		$knum21_prev = $k21 ;
          		$knum00_prev = $k00;
          		$clist_prev = $list00;
          		@filesyn_prev = @file00;
    		}
    		print "  knum = $knum \n" ;
		die;

#   If we have data for a  previous day  - we archive them .

    		if( $kday > 0 && $VIIRS_NO_ARCHIVE != 1)  {
######################################################################
# create TAR files from raw VIIRS (real time .hdf )  files  for $process_date_m1
########################

#       		$VIIRS_ACQUIRE_PATH=$value;
#       		@list = split('/',${VIIRS_ACQUIRE_PATH});
#       		$list_len = @list;
#       		${VIIRS_ACQUIRE_FILE} = $list[$list_len-1];
#       		$fire_dir  = token_resolve("${VIIRS_ACQUIRE_DIR}", $process_date_m1, "${hour}${min}00");
#       		$fire_file = token_resolve("${VIIRS_ACQUIRE_FILE}", $process_date_m1, "${hour}${min}00");

#       		01234567890123456789012345678901234      - VIIRS
#       		MOD14.A2018297.0855.006.NRT.hdf      - VIIRS

#       		$tar_name=substr( ${fire_file},0,6) ;
			$tar_name=substr( ${VIIRS_ACQUIRE_FILE},0,6) ;
    			$tar_name  =token_resolve("${tar_name}%y4%m2%d2.hdf.tar",$process_date_m1);
        		print " iCHTORAT tar_name = $tar_name     \n";

#       		TAR  Input Raw files for previous day
#       		$rc=system(" tar -cvf ${tar_name} ${list_full} ");
#       		$rc=system(" tar -cvf ${tar_name} ${clist} ");

#        		$rc=system(" tar -cvf ${tar_name} ${list_full} ");

#       		Archive TAR file for previous day
			$fire_archive = token_resolve("${archive{$key}}", $process_date_m1, "${hour}${min}00");
   			$fire_archive =~ s/<<TYPE>>/$key/;

        		print " ARCHIVE = fire_archive = $fire_archive     \n";

        		$rc = gen_archive ( "$env", "$prep_ID", 'fire', 'hdf', "$process_date_m1", "$fire_archive", "$tar_name",  
				{  'verbose' => "$verbose", 'exp_path' => "1", 'delete'      => "1" } );

    			if ($rc != 1) {
        			err_log (4, "get_sci_viirs.pl", "$err_time","$prep_ID","-1",
                			{'err_desc' => "${err_ID}: could not archive $tar_name  while running for VIIRS. "});
        			print "WARNING: could not archive $tar_name \n";
        			$archive_err ++;
          			recd_state($fl_name, "FAILED", $tab_argv, $sched_dir, $sched_sts_fl);
            			if ( ! $opt_f ) {
        				err_log (4, "get_sci_viirs.pl", "$err_time","$prep_ID","-1",
                 				{'err_desc' => "${err_ID}: could not archive $tar_name while running for  VIIRS. "});
        				print "ERROR: could not archive $tar_name \n";
        				$archive_err ++;
          				recd_state($fl_name, "FAILED", $tab_argv, $sched_dir, $sched_sts_fl);
	        			die "Error: could not archive $tar_name  for date: $process_date_m1. This error occurred while processing  $key data.\n";
        			}
    			}

#   end If there files for  previous day
    		}
    		else {
    			print " NO FILES  to Archive for  day=  $process_date_m1 .\n";
    		}
#   end  If syntime =0
	}

###########################################################
#    	we will get all data for current day
###########################################################


     	print " process_date   =$process_date \n" ;
     	$fire_stage = token_resolve("${stage{$key}}", $process_date, "${hour}${min}00");
     	$fire_dir   = token_resolve("${VIIRS_ACQUIRE_DIR}", $process_date, "${hour}${min}00");
     	$fire_file  = token_resolve("${VIIRS_ACQUIRE_FILE}", $process_date, "${hour}${min}00");
     	print " fire_file=$fire_file\n fire_stage=$fire_stage\n fire_dir=$fire_dir\n";
        $want_files = "${VIIRS_ACQUIRE_MACH}${fire_dir}/" ;
        @file_keys = split('\/',$fire_stage);
        print "@file_keys\n";
        $rel_path = "$file_keys[-2]/$file_keys[-1]";
        #$NCKS2 = "$ENV{'BASEDIR'}/Linux/bin/ncks";
	$NCKS2 = "$NCKS_DIR/Linux/bin/ncks";
        $NCKS_ARGS = " -h -v latitude,longitude -4 --no_tmp_fl -O --baa=4 --ppc dfl=6";
        sub ncks_call {
                        print "$NCKS2 ${NCKS_ARGS} $_ $fire_stage/$_\n";
                        print "$_\n";
                        $rc = system(" $NCKS2 ${NCKS_ARGS} $_ $fire_stage/$_");
                        $rc = system(" ls $fire_stage/$_");
                        $rc = system(" rm -f $_");
                        }

	#VJ103,VNP03
        print "VIIRS_WORK_DIR/prep_ID\n";

	print "$VIIRS_WORK_DIR/$prep_ID\n";

	chdir "$VIIRS_WORK_DIR/$prep_ID";

        print "$key\n";

        if ( ( ${key} =~ /VNP03IMG_NRT/ || ${key} =~ /VJ103IMG_NRT/ || ${key} =~ /VJ203IMG_NRT/ || ${key} =~ /VJ203IMG/ || ${key} =~ /VNP03IMG/ || ${key} =~ /VJ103IMG/  ) ) {
        	print " if loop caught $key\n";
		$trim_data=1;
	}
	else {
	print " key error for $key\n";
	}
	print "${trim_data}\n";

     	if ( ( ${key} =~ /VJ103/ || ${key} =~ /VNP03/ || ${key} =~ /VNP03IMG_NRT/ || ${key} =~ /VJ103IMG_NRT/ || ${key} =~ /VJ203IMG_NRT/ || ${key} =~ /VJ203IMG/ ) && ${trim_data} == 1) {
		$fire_stage = token_resolve("${stage{$key}}", $process_date, "${hour}${min}00");
        	$fire_stage=~ s/${key}/${key}.trimmed/;
		print "$fire_stage\n";
	}

     	else {
		$fire_stage = token_resolve("${stage{$key}}", $process_date, "${hour}${min}00");
		print "non trimming loop for $key : $fire_stage\n";
     	}
        if ( ! -d "$fire_stage" ) {                        
#		$fire_stage=~ s/_NRT//;
		print "$fire_stage\n";
                eval { mkpath( "$fire_stage" ) };
                        if ($@) {
                                err_log (4, "get_sci_viirs.pl", "$err_time","$err_ID","-1",
					{'err_desc' => "$err_ID: Cannot make $fire_stage"});
                                recd_state($fl_name, "FAILED", $tab_argv, $sched_dir, $sched_sts_fl);                                      
				die "Cannot make $fire_stage";
                        }
        }
	print "$fire_stage\n";
	print "${VIIRS_WGET} ${want_files} \n";
        $rc= system(" ${VIIRS_WGET} ${want_files}  ");

	# if lvl1
        if ( ( ${key} =~ /VJ103/ || ${key} =~ /VNP03/ || ${key} =~ /VJ203IMG_NRT/ ) && ${trim_data} == 1) {
                eval { find(\&ncks_call,"$VIIRS_WORK_DIR/$prep_ID/$key/$rel_path")};
	
        	if ($@) {                        
                	err_log (4, "get_sci_viirs.pl", "$err_time","$err_ID","-1",                                
                       		{'err_desc' => "$err_ID: Cannot execute find function at $VIIRS_WORK_DIR/$prep_ID/$rel_path for data ${key}"});
                	recd_state($fl_name, "FAILED", $tab_argv, $sched_dir, $sched_sts_fl);
			die "Cannot access files at: $VIIRS_WORK_DIR/$prep_ID/$rel_path";
        	}
	}
	else {	
		$rc = system(" mv -f $VIIRS_WORK_DIR/$prep_ID/$key/$rel_path/*.nc $fire_stage");
	}

	print "$key parsed and processed \n";

# 	MOD14/MYD14/NOAA20VJ114IMG/NOAA20VJ103IMG/VNP14IMG/VNP03IMG
# 	chmod -R o+r, o+x, g+r, g+w, g+X\!*; chgrp -R modisdata\!*
#   	MOD14.A2018290.1240.006.NRT.hdf
#       $date_year =substr( $filename,7,4) ;
#       $date_jday =substr( $filename,11,3) ;
#       $date_today= $date_year.$date_jday ;
# 	OR
#       $date_today =substr( $fire_file,7,7) ;
#     	$fire_file =  'MOD14.A2018302.0505.006.NRT.hdf' ;
#      	$filename = substr( $fire_file,0,14) ; 

#      	print "$fire_file\n";

       	@file_parts = split (/\./, $fire_file); 
       	$date_today = substr( $file_parts[1],1,7);

        print   "BEFORE count_bysyn_fire:  current day  =  is $process_date \n";
        print   "BEFORE count_bysyn_fire : fire_file =    is $fire_file \n";
        print   "BEFORE count_bysyn_fire : date_today =    is $date_today \n";
        print   "BEFORE count_bysyn_fire : PROCESSING TYPE $file_parts[0]\n";
        eval { chdir "$fire_stage" };
        if ($@) {
		err_log (4, "get_sci_viirs.pl", "$err_time","$err_ID","-1",
                	{'err_desc' => "$err_ID: Cannot cd to $fire_stage"});
                recd_state($fl_name, "FAILED", $tab_argv, $sched_dir, $sched_sts_fl);
                die "Cannot cd to $fire_stage";
        }
       	count_bysyn_fire ( $file_parts[0], $date_today,$syntime);
	#return ($kday,$k00,$k06,$k12,$k18,$k21,$list_full,$list00,$list06,$list12,$list18,$list21); #11-21-25wjd


	print "AFTER count_bysyn_fire  k00=    is $k00\n";
       	print "AFTER count_bysyn_fire  k06=    is $k06\n";
       	print "AFTER count_bysyn_fire  k12=    is $k12\n";
       	print "AFTER count_bysyn_fire  k18=    is $k18\n";
       	print "AFTER count_bysyn_fire  k21=    is $k21\n";
    	print "AFTER count_bysyn_fire  Number of list_full=    is $kday\n";

        $i =0 ;
#   	FOR CURRENT DAY
    	if ( $syntime eq '00') {
          	$knum21 = $k21 ;
          	$knum00 = $k00;
          	$clist = $list00;
          	@filesyn = @file00;
          	$knum = $k00 + $knum21_prev ;
    	}

    	if ( $syntime eq '06') {
          	$knum = $k06;
          	$clist =$list06;
          	@filesyn = @file06;
    	}

    	if ( $syntime eq '12') {
        	$knum = $k12;
          	$clist =$list12;
          	@filesyn = @file12;

    	}

    	if ( $syntime eq '18') {
        	$knum = $k18;
        	$clist =$list18;
        	@filesyn = @file18;
    	}
    	if ( $syntime eq '21') {
          	$knum = $k21;
          	$clist =$list21;
          	@filesyn = @file21;
     	}
 
   	if ( $syntime eq 'hd') {
          	$knum = $kday;
          	$clist =$list_full;
          	@filesyn = @file00;
     	}



      	print "  syntime  = $syntime , knum = $knum \n" ;
#HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH

 	if ( $syntime eq 'hd') {

   		print "SYNTIME CASE is  hd :  Limit   of  ${key} data files is $nlimit{$key} .\n";
    		print "SYNTIME CASE is  hd :  Number of  ${key} data files is $kday .\n";

     		if ( $kday  < $nlimit{$key} ) {
        		print "WARNING: Less than $nlimit{$key} of ${key} data files are found. \n";
         		if (  ! defined($opt_f) ) {
      				print "STOP : NO OPTION_f \n";
  				print "ERROR:   Less than $nlimit{$key} of ${key} data files are found.. \n";

            			recd_state($fl_name, "FAILED", $tab_argv, $sched_dir, $sched_sts_fl);
            				err_log (4, "get_sci_viirs.pl", "$err_time","$err_ID","-1",
                     				{'err_desc' => "ERROR:  Less than $nlimit{$key} of ${key} data files found. Check $VIIRS_ACQUIRE_MACH:${fire_dir}"}); 
            			die "Error:  Less than   $nlimit{$key} of ${key} data files are found.";
         		}
     		}
#   end if syntime = hd
   	}


# HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH

 	if ( $syntime ne '21' &&  $syntime ne 'hd' ) {

      		print "SYNTIME CASE is syntime = $syntime :  Number of  ${key} data files is $knum .\n";
#############################################################
#   Check for missing data
#############################################################

# Make sure we have at least $nlimit{$key} of files downloaded for
# this data type.

     		$missing_count =  $nlimit{$key} - $knum ;
     		print "Minimum number of $key is $nlimit{$key}.  We found $knum.\n";
     		if (  $nlimit{$key} > $knum ) {
         		if ( defined($opt_f) ) {
             			print "WARNING: Only ${knum} ${key} data files were found.  Option -f set, so processing continues.";
             			err_log (4, "get_sci_viirs.pl", "$err_time","$err_ID","-1",
                  		{'err_desc' => "WARNING: Only ${knum} ${key} data files were found. Check $VIIRS_ACQUIRE_MACH:${fire_dir}.  Option -f set, so processing continues."});
          		}
          		else {
              			recd_state($fl_name, "FAILED", $tab_argv, $sched_dir, $sched_sts_fl);
              			err_log (4, "get_sci_viirs.pl", "$err_time","$err_ID","-1",
                  			{'err_desc' => "ERROR: Only ${knum} ${key} data files were found. Check $VIIRS_ACQUIRE_MACH:${fire_dir}."});
              			die "ERROR: Only ${knum} ${key} data files were found.";
         		}
     		}

############################################### 
#   		End  If  if ( $syntime ne '21' and $syntime ne 'hd') 

 	}


#   	If we have data for a day  - we archive them .

   	if( $kday > 0 && $VIIRS_NO_ARCHIVE != 1)  {

  		print "TIME CASE is syntime = $syntime:Number of ${key} data files to archive is $kday .\n";
######################################################################
# 		create TAR files from raw VIIRS (real time .hdf )  files  for $process date 
########################

#  		$VIIRS_ACQUIRE_PATH=$value;
#    		@list = split('/',${VIIRS_ACQUIRE_PATH});
#    		$list_len = @list;
#    		${VIIRS_ACQUIRE_FILE} = $list[$list_len-1];
#       	$fire_dir  = token_resolve("${VIIRS_ACQUIRE_DIR}", $process_date, "${hour}${min}00");
#       	$fire_file = token_resolve("${VIIRS_ACQUIRE_FILE}", $process_date, "${hour}${min}00");


#  		01234567890123456789012345678901234      - VIIRS
#  		MOD14.A2018297.0855.006.NRT.hdf      - VIIRS

#   		$tar_name=substr( ${fire_file},0,6) ;

    		$tar_name=substr( ${VIIRS_ACQUIRE_FILE},0,6) ;
    		$tar_name  =token_resolve("${tar_name}%y4%m2%d2.hdf.tar",$process_date);
                print " iCHTORAT tar_name = $tar_name     \n";

#  		TAR  Input Raw files for current day
#     		$rc=system(" tar -cvf ${tar_name} ${list_full} ");
#         	$rc=system(" tar -cvf ${tar_name} ${clist} ");

         	$rc=system(" tar -cvf ${tar_name} ${list_full} ");

#  		Archive TAR file for current day
     		print "  BEFORE ARCHIVE:   key = $key     \n";

      		print " BEFORE ARCHIVE:  VIIRS_ARCHIVE_LOC = $VIIRS_ARCHIVE_LOC     \n";
     
   		$fire_archive = token_resolve("${archive{$key}}", $process_date, "${hour}${min}00");
   		$fire_archive =~ s/<<TYPE>>/$key/;

       		print " BEFORE ARCHIVE : fire_archive = $fire_archive     \n";

 		$rc = gen_archive ( "$env", "$prep_ID", 'fire', 'nc', "$process_date", "$fire_archive", "$tar_name",  {  'verbose' => "$verbose", 'exp_path' => "1", 'delete'      => "1" } );
		print "$rc";
    		if ($rc != 1) {
        		err_log (4, "get_sci_viirs.pl", "$err_time","$prep_ID","-1",
                 		{'err_desc' => "${err_ID}: could not archive $tar_name  while running for VIIRS. "});
        		print "WARNING: could not archive $tar_name \n";
        		$archive_err ++;
          		recd_state($fl_name, "FAILED", $tab_argv, $sched_dir, $sched_sts_fl);
            		if ( ! $opt_f ) {
        			err_log (4, "get_sci_viirs.pl", "$err_time","$prep_ID","-1",
                			{'err_desc' => "${err_ID}: could not archive $tar_name while running for  VIIRS. "});
        			print "ERROR: could not archive $tar_name \n";
	        		$archive_err ++;
        	  		recd_state($fl_name, "FAILED", $tab_argv, $sched_dir, $sched_sts_fl);
			        die "Error: could not archive $tar_name  for date: $process_date .  This error occurred while processing VIIRS data .";
                 	}
          	}

   		end #If there files for today
      	}
        else {
            print " NO FILES  to Archive for today - day=  $process_date .\n";
      	}

#  end of while (($key, $value) = each(%rfile))
}

########################
# Rename output listings
########################

if ( $opt_O ) {
   	( $listing_archive = $VIIRS_ARCHIVE_LOC ) =~ s/<<TYPE>>/modis/;
   	unlink<"$listing_file_gz">;
   	system ( "gzip -c $listing_file > $listing_file_gz" );
   	$rc=gen_archive ( "$env","$prep_ID",'viirs-sci','listings', "$process_date",
        	"$listing_archive", $listing_file_gz,
                { 'remote_name' => "viirs-sci.${err_time}.listing.gz", 'delete'      => "1", 'verbose'     => "1" } );
   	if ( $rc != 1 ) {
       		err_log (4, "get_sci_viirs.pl", "$err_time","$err_ID","-1",
              		{'err_desc' => "${err_ID}: could not archive listing file $listing_file_gz"});
       		print "WARNING: could not archive listing file.\n";
   	}
   	system ("mv $listing_file $opt_O/SCI.${err_time}.listing");
}
err_log (0, "get_sci_viirs.pl", "$err_time","$err_ID","-1",{'err_desc' => "get_sci_viirs.pl: ${err_ID}: exiting normally"});
recd_state( $fl_name, "COMPLETE", $tab_argv, $sched_dir, $sched_sts_fl );
exit 0;
