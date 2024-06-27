export branch_to_compare="main" 
export yellow_treshold=102400
export red_treshold=2097152 
export commit_git_sha="a3d73ea71a1eeb34d7f692f37c67c8e003d450a1"
export CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS="~/.ydb/my-robot-key.json"
export build_preset='relwithdebinfo'

get_sizes_comment_script=./get_build_diff.py
comment_raw=`$get_sizes_comment_script`

echo $comment_raw