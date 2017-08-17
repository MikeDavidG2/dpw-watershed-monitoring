#-------------------------------------------------------------------------------
# Name:        DPW_Update_sde_load.py
# Purpose:
"""
To take the list of items (items_to_export) from an export FGDB (fgdb_to_export)
and put the items into a FGDB to be updated (fgdb_to_be_updated).

NOTE: the Dataset Type of the items can be either a Table or a Feature Class.
"""
#
# Author:      mgrue
#
# Created:     07/07/2017
# Copyright:   (c) mgrue 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import arcpy, os

arcpy.env.overwriteOutput = True

def main():

    print 'Starting to run DPW_Update_sde_load.py\n'
    #---------------------------------------------------------------------------
    #                              Set variables
    #---------------------------------------------------------------------------

    fgdb_to_export = r"P:\DPW_ScienceAndMonitoring\Scripts\DEV\Data\DPW_Science_and_Monitoring_prod.gdb"
    items_to_export = ['DPW_WP_FIELD_DATA', 'DPW_WP_SITES']

    fgdb_to_be_updated = r'V:\sde_load.gdb'

    # Set to "True" to have 'print' statements be written to the log_file
    # Set to "False" to have 'print' statements print to screen
    run_Write_Print_To_Log = True
    log_file = r'P:\DPW_ScienceAndMonitoring\Scripts\DEV\Data\Logs\DPW_Update_sde_load'

    # Set email recipients
    email_recipients = ['michael.grue@sdcounty.ca.gov']


    # Flag that is changed to "False" if there are errors
    success = True

    #---------------------------------------------------------------------------
    #                         Start calling Functions()
    #---------------------------------------------------------------------------

    # Turn the 'print' statement into a logging object
    if (run_Write_Print_To_Log):
        orig_stdout, log_file_date = Write_Print_To_Log(log_file)

    # Process items
    try:
        for item in items_to_export:
            print 'Processing item: {}\n'.format(item)

            # Set path to export item
            item_path = os.path.join(fgdb_to_export, item)

            # Get the dataset type of item (i.e. 'Table' or 'FeatureClass') to
            # determine if script should copy ROWS or copy FEATURES.
            dataset_type = Get_Dataset_Type(item_path)

            if (dataset_type == 'Table'):
                # Set path to new item
                new_item  = os.path.join(fgdb_to_be_updated, item)

                Copy_Rows(item_path, new_item)  # Copy the Table

            elif (dataset_type == 'FeatureClass'):
                # Set path to new item INSIDE the 'workspace' Feature Dataset
                new_item  = os.path.join(fgdb_to_be_updated, 'workspace' ,item)

                Copy_Features(item_path, new_item)  # Copy the FC

            else:
                print 'ERROR! Item "{}" is not a Table or Feature Class'.format(item)
                success = False

            if success == True:
                print 'Success updating "{}"'.format(new_item)
                print '------------------------------------------------------------\n'

    except Exception as e:
        print '*** ERROR with Processing Items! ***'
        print '{}\n'.format(str(e))
        success = False

    #---------------------------------------------------------------------------
    #                         End of script reporting
    #---------------------------------------------------------------------------
    if success == True:
        email_subject = 'SUCCESS running DPW_Update_sde_load.py'
        print email_subject
    else:
        email_subject = 'ERROR running DPW_Update_sde_load.py'
        print email_subject

    # Return sys.stdout back to its original setting
    if (run_Write_Print_To_Log):
        try:
            # Footer for log file
            finish_time_str = [datetime.datetime.now().strftime('%m/%d/%Y  %I:%M:%S %p')][0]
            print '\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
            print '                    {}'.format(finish_time_str)
            print '              Finished DPW_Update_sde_load.py'
            print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'

            sys.stdout = orig_stdout

            print '\nDone with script.  Success = {}.'.format(str(success))
            print '  Please find log file location above for more info.'
        except Exception as e:
            print 'ERROR with end of script reporting'
            print str(e)

    # Send email as either SUCCESS or ERROR with the log file attached
    Email(email_subject, email_recipients, log_file_date)

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
      log_file_date (str): Path to the log file with the date and time appeneded.

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
    print '             START DPW_Update_sde_load.py'
    print '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n'

    return orig_stdout, log_file_date

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

    date = '%s_%s_%s' % (start_time.year, start_time.month, start_time.day)
    time = '%s_%s_%s' % (start_time.hour, start_time.minute, start_time.second)

    dt_to_append = '%s__%s' % (date, time)

    print '    DateTime to append: "{}"'.format(dt_to_append)

    print '  Finished Get_DT_To_Append()'
    return dt_to_append

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
def Get_Dataset_Type(in_item):
    """
    PARAMETERS:
      in_item (str): Full path to an item.

    RETURNS:
      dataset_type (str): The dataset type of the item.  Common results include:
        'FeatureClass'
        'Table'
        'GeometricNetwork'
        'RasterDataset'

    FUNCTION:
      To get the dataset type of the 'in_item' and return a string describing
      the type of dataset.  Used when the main() may want to treat the item
      differently based on the dataset type.

      For example:
        A 'Table' may require an        'arcpy.CopyRows_management()' while,
        A 'FeatureClass' may require an 'arcpy.CopyFeatures_management()'
    """

    print 'Starting Get_Dataset_Type()...'
    print '  Getting Dataset Type of: "{}"'.format(in_item)

    desc = arcpy.Describe(in_item)
    dataset_type = desc.datasetType

    print '    "{}"'.format(dataset_type)
    print 'Finished Get_Dataset_Type\n'

    return dataset_type

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                                 FUNCTION Copy_Rows()
def Copy_Rows(in_table, out_table):
    """
    PARAMETERS:
      in_table (str): Full path to an input table.
      out_table (str): Full path to an existing output table.

    RETURNS:
      None

    FUNCTION:
      To copy the rows from one table to another table.
    """

    print 'Starting Copy_Rows()...'

    print '  Copying Rows from: "{}"'.format(in_table)
    print '                 To: "{}"'.format(out_table)

    arcpy.CopyRows_management(in_table, out_table)

    print 'Finished Copy_Rows()\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          FUNCTION Copy_Features()
def Copy_Features(in_FC, out_FC):
    """
    PARAMETERS:
      in_FC (str): Full path to an input feature class.
      out_FC (str): Full path to an existing output feature class.

    RETURNS:
      None

    FUNCTION:
      To copy the features from one feature class to another existing
      feature class.
    """

    print 'Starting Copy_Features()...'

    print '  Copying Features from: "{}"'.format(in_FC)
    print '                     To: "{}"'.format(out_FC)

    arcpy.CopyFeatures_management(in_FC, out_FC)

    print 'Finished Copy_Features()\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                       FUNCTION Email()
def Email(email_subject, email_recipients, log_file=None):
    """
    PARAMETERS:
      email_subject (str): The subject line for the email

      email_recipients (list): List (of strings) of email addresses

      log_file {str}: Path to a log file to be included in the body of the
        email. Optional.


    RETURNS:
      None


    FUNCTION:
      To send an email to the listed recipients.  May provide a log file to
      include in the body of the email.

      NOTE: set the 'email_config_file' variable if needed. The format of the
        config file should be as below with <username> and <password> completed:
          [email]
          usr: <username>
          pwd: <password>

    """
    import smtplib, ConfigParser
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    #---------------------------------------------------------------------------
    #                              Set variables
    # Set config file to get username and password
    email_config_file = r"M:\scripts\configFiles\accounts.txt"

    #---------------------------------------------------------------------------
    # Set log file into body of email if provided
    if log_file != None:
        # Get the log file to add to email body
        fp = open(log_file,"rb")
        msg = MIMEText(fp.read())
        fp.close()
    else:
        msg = MIMEMultipart()

    # Get username and pwd from the config file
    try:
        config = ConfigParser.ConfigParser()
        config.read(email_config_file)
        email_usr = config.get("email","usr")
        email_pwd = config.get("email","pwd")
    except:
        print 'ERROR!  Could not read config file.  May not exist at location, or key may be incorrect.  Email not sent.'
        return

    # Set from and to addresses
    fromaddr = "dplugis@gmail.com"
    toaddr = email_recipients
    email_recipients_str = ', '.join(email_recipients)  # Join each item in list with a ', '

    # Set visible info in email
    msg['Subject'] = email_subject
    msg['From']    = "Python Script"
    msg['To']      = email_recipients_str

    # Email
    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.ehlo()
    s.starttls()
    s.ehlo()
    s.login(email_usr,email_pwd)
    s.sendmail(fromaddr,toaddr,msg.as_string())
    s.quit()

    print 'Sent email with subject "{}"'.format(email_subject)
    print 'To: {}'.format(email_recipients_str)

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

if __name__ == '__main__':
    main()