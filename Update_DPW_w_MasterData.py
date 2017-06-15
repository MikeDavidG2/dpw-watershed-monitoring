#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      mgrue
#
# Created:     09/06/2017
# Copyright:   (c) mgrue 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import arcpy
arcpy.env.overwriteOutput = True

def main():

    print 'Starting to run script.\n'

    # Set variables
    master_table = r'U:\grue\Scripts\GitHub\DPW-Sci-Monitoring\DEV\Data\DPW_Science_and_Monitoring_prod.gdb\Field_Data'    # Table used to update
    to_update_table = r'X:\day\Testing.mdb\Field_Data'                                                                     # Table to be updated

    # TODO: set the log file location, get the print statements to write to the log file
    log_file = r''
    #---------------------------------------------------------------------------
    #                         Start calling Functions()

    # Test to make sure there is no existing schema lock
    no_schema_lock = Test_Schema_Lock(to_update_table)

    # If there is no schema lock, delete the rows in the table then copy new data to it
    if no_schema_lock:
        Delete_Rows(to_update_table)

        Copy_Rows(master_table, to_update_table)

    else:
        print 'ERROR!  There was a schema lock on "{}".  \n  Not able to update database.'.format(to_update_table)
        print '  Please have everyone disconnect from database and rerun this script.\n'

    #---------------------------------------------------------------------------
    # End of script reporting
    print 'Finished running script.'


#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                          FUNCTION Test_Schema_Lock()
def Test_Schema_Lock(dataset):
    """
    PARAMETERS:
      dataset (str): Full path to a dataset to be tested if there is a schema lock

    RETURNS:
      no_schema_lock (Boolean): "True" or "False" if there is no schema lock

    FUNCTION:
      To perform a test on a dataset and return "True" if there is no schema
      lock, and "False" if a schema lock already exists.
    """

    print 'Starting Test_Schema_Lock()...'

    print '  Testing dataset: {}'.format(dataset)

    no_schema_lock = arcpy.TestSchemaLock(dataset)
    print '  Dataset available to have a schema lock applied to it = "{}"'.format(no_schema_lock)

    print 'Finished Test_Schema_Lock()\n'

    return no_schema_lock

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                            FUNCTION Delete_Rows()
def Delete_Rows(in_table):
    """
    PARAMETERS:
      in_table (str): Full path to a table.

    RETURNS:
      None

    FUNCTION:
      To delete the rows from one table.
    """

    print 'Starting Delete_Rows()...'

    print '  Deleting Rows from: "{}"'.format(in_table)

    arcpy.DeleteRows_management(in_table)

    print 'Finished Delete_Rows()\n'

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

# Run the main()
if __name__ == '__main__':
    main()
