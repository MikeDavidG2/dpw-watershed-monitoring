set jobdir=P:\DPW_ScienceAndMonitoring\Scripts\PROD
set script_1=%jobdir%\PROD_branch\DPW_Science_and_Monitoring.py
::set script_2=%jobdir%\DEV_branch\Update_DPW_w_MasterData.py
set log=%jobdir%\Data\Logs\DPW_Science_and_Monitoring_batch.log

echo -----------------------[START %date% %time%]------------------->>%log%

echo Running %script_1%>>%log%
Start /wait  %script_1%>>%log%

::timeout /T 5

::echo Running %script_2%>>%log%
::Start /wait  %script_2%>>%log%

echo -----------------------[END %date% %time%]--------------------->>%log%
echo ---------------------------------------------------------------------------->>%log%
echo ---------------------------------------------------------------------------->>%log%
echo ---------------------------------------------------------------------------->>%log%