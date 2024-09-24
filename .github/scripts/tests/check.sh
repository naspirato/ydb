get_file_diff_script=/home/kirrysin/fork/ydb/.github/scripts/tests/get_file_diff.py
file_to_check=/home/kirrysin/fork/ydb/.github/config/muted_ya.txt
check_result=`$get_file_diff_script $file_to_check`
 if [[ ${check_result} == *"not changed" ]];then
    echo file ${file_to_check} NOT changed

else
    echo file ${file_to_check} changed
    /home/kirrysin/fork/ydb/.github/scripts/tests/get_muted_tests.py --output_folder "/home/kirrysin/fork/ydb/mute_info/" get_mute_details  --job-id "10958550704" --branch "main"

fi
