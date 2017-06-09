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

    # Set variables
    master_table = r'X:\month\Test_FGDB.gdb\Field_Data'    # Table used to update
    to_update_table = r'X:\month\Test_PGDB.mdb\Field_Data' # Table to be updated

    #---------------------------------------------------------------------------
    #                         Start calling Functions()
    Delete_Rows(to_update_table)

    Copy_Rows(master_table, to_update_table)


    #---------------------------------------------------------------------------
    # End of script reporting
    print 'Successfully Finished.'


#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#                                 FUNCTION Delete_Rows()
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

    print '  Copying Rows from: "{}" to: "{}"'.format(in_table, out_table)

    arcpy.CopyRows_management(in_table, out_table)

    print 'Finished Copy_Rows()\n'

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

if __name__ == '__main__':
    main()
