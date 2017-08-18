::This Batch file should be run weekly to update sde_load.gdb (Script 1)
::As well as to create a backup FGDB (Script 2)
set jobdir=P:\DPW_ScienceAndMonitoring\Scripts\PROD
set script_1=%jobdir%\PROD_branch\DPW_Update_sde_load.py
set script_2=%jobdir%\PROD_branch\Backup_FGDB.py
set log=%jobdir%\Data\Logs\DPW_Update_sde_load_batch.log

echo -----------------------[START %date% %time%]------------------->>%log%

echo Running %script_1%>>%log%
Start /wait  %script_1%>>%log%

timeout /T 5

echo Running %script_2%>>%log%
Start /wait  %script_2%>>%log%

echo -----------------------[END %date% %time%]--------------------->>%log%
echo ---------------------------------------------------------------------------->>%log%
echo ---------------------------------------------------------------------------->>%log%
echo ---------------------------------------------------------------------------->>%log%