setlocal
call env.bat
BigQuery-Logs.exe -sys=AVBTEST4 -dir=test_data_light >BigQuery-Logs.out 2>BigQuery-Logs.err
endlocal