#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      mgrue
#
# Created:     28/07/2017
# Copyright:   (c) mgrue 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------

# TODO: Document script
# TODO: Format print statements

import arcpy, os, operator

arcpy.env.overwriteOutput = True

def main():
    FGDB_to_backup = r"P:\DPW_ScienceAndMonitoring\Scripts\DEV\Data\DPW_Science_and_Monitoring_prod.gdb"
    backup_folder  = r'P:\DPW_ScienceAndMonitoring\Scripts\DEV\Data\Backups'
    log_file        = r'P:\DPW_ScienceAndMonitoring\Scripts\DEV\Data\Logs\Backup_FGDB'
    max_num_backups = 2


    # Set print to logging statement
##    orig_stdout = Write_Print_To_Log(log_file)

    # Copy FGDB to Backup folder
    date_time = Get_DT_To_Append()
    out_FGDB = os.path.join(backup_folder, 'DPW_Sci_and_Mon_prod_BAK__{}.gdb'.format(date_time.split('__')[0]))
    print 'Copying FGDB: "{}"\n          To: "{}"'.format(FGDB_to_backup, out_FGDB)
##    arcpy.Copy_management(FGDB_to_backup, out_FGDB)
    print '   Copied'


    # Test to determine if a backup needs to be deleted
    arcpy.env.workspace = backup_folder
    workspaces = arcpy.ListWorkspaces('', 'FileGDB')
    print 'There are {} existing workspaces, {} are allowed.'.format(len(workspaces), max_num_backups)

    if len(workspaces) <= max_num_backups:
        num_wkspaces_to_del = 0
        print 'No need to delete any workspaces'

    else:
        num_wkspaces_to_del = len(workspaces)-max_num_backups

        print 'Deleting {} workspace(s)'.format(str(num_wkspaces_to_del))

        # TODO: Need to have a loop to delete num_wkspaces_to_del
        print 'Deleting "{}"'.format(workspaces[num_wkspaces_to_del-1])


#-------------------------------------------------------------------------------
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#                           START DEFINING FUNCTIONS
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          FUNCTION Write_Print_To_Log()
def Write_Print_To_Log(log_file):
    """
    PARAMETERS:
      log_file (str): Path to log file.  The part after the last "\" will be the
        name of the .log file after the date, time, and ".log" is appended to it.

    RETURNS:
      orig_stdout (os object): The original stdout is saved in this variable so
        that the script can access it and return stdout back to its orig settings.

    FUNCTION:
      To turn all the 'print' statements into a log-writing object.  A new log
        file will be created based on log_file with the date, time, ".log"
        appended to it.  And any print statements after the command
        "sys.stdout = write_to_log" will be written to this log.
      It is a good idea to use the returned orig_stdout variable to return sys.stdout
        back to its original setting.
      NOTE: This function needs the function Get_DT_To_Append() to run

    """
    print 'Starting Write_Print_To_Log()...'

    # Get the original sys.stdout so it can be returned to normal at the
    #    end of the script.
    orig_stdout = sys.stdout

    # Get DateTime to append
    dt_to_append = Get_DT_To_Append()

    # Create the log file with the datetime appended to the file name
    log_file_date = '{}_{}.log'.format(log_file,dt_to_append)
    write_to_log = open(log_file_date, 'w')

    # Make the 'print' statement write to the log file
    print '  Setting "print" command to write to a log file found at:\n    {}'.format(log_file_date)
    sys.stdout = write_to_log

    # Header for log file
    start_time = datetime.datetime.now()
    start_time_str = [start_time.strftime('%m/%d/%Y  %I:%M:%S %p')][0]
    print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
    print '                  {}'.format(start_time_str)
    print '             START Update_DPW_w_MasterData.py'
    print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n'

    return orig_stdout

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          FUNCTION Get_dt_to_append
def Get_DT_To_Append():
    """
    PARAMETERS:
      none

    RETURNS:
      dt_to_append (str): Which is in the format 'YYYY_M_D__H_M_S'

    FUNCTION:
      To get a formatted datetime string that can be used to append to files
      to keep them unique.
    """
    print '  Starting Get_DT_To_Append()...'

    start_time = datetime.datetime.now()

    date = start_time.strftime('%Y_%m_%d')
    time = start_time.strftime('%H_%M_%S')

    dt_to_append = '%s__%s' % (date, time)

    print '    DateTime to append: "{}"'.format(dt_to_append)

    print '  Finished Get_DT_To_Append()'
    return dt_to_append

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
if __name__ == '__main__':
    main()
