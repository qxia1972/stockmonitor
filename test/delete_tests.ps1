# PowerShell批量删除脚本
# 运行方式：powershell -ExecutionPolicy Bypass -File .\test\delete_tests.ps1

Remove-Item demo.py -ErrorAction SilentlyContinue
Remove-Item error_prevention_guide.py -ErrorAction SilentlyContinue
Remove-Item field_optimization_summary.py -ErrorAction SilentlyContinue
Remove-Item file_migration_report.py -ErrorAction SilentlyContinue
Remove-Item indentation_test.py -ErrorAction SilentlyContinue
Remove-Item no_interaction_template.py -ErrorAction SilentlyContinue
Remove-Item requirements_validation.py -ErrorAction SilentlyContinue
Remove-Item requirements_verification_report.py -ErrorAction SilentlyContinue
Remove-Item smart_insertion_test.py -ErrorAction SilentlyContinue
Remove-Item test_error_prevention.py -ErrorAction SilentlyContinue
Remove-Item test_fields_windows.py -ErrorAction SilentlyContinue
Remove-Item test_insertion.py -ErrorAction SilentlyContinue
Remove-Item test_ohlcv_api.py -ErrorAction SilentlyContinue
Remove-Item test_result_1.py -ErrorAction SilentlyContinue
Remove-Item test_result_2.py -ErrorAction SilentlyContinue
Remove-Item test_result_3.py -ErrorAction SilentlyContinue
Remove-Item test_rqsdk_api.py -ErrorAction SilentlyContinue
Remove-Item verify_fields.py -ErrorAction SilentlyContinue

Write-Host "test目录下所有过期测试Python程序已批量删除！"
