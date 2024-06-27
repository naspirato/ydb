script=/home/kirrysin/fork/ydb/.github/scripts/get_main_build_size.py
script2=/home/kirrysin/fork/ydb/.github/scripts/get_current_build_size.py
bytesToHumanIEC() {
    b=${1:-0}; d=''; s=0; S=(Bytes {K,M,G,T,E,P,Z,Y}iB)
    while ((b > 1024)); do
        d="$(printf ".%02d" $((b % 1024 * 100 / 1024)))"
        b=$((b / 1024))
        let s++
    done
    echo "$b$d ${S[$s]}"
}
printf "
new line
again
"


 orange_treshold=10240
red_treshold=1048576
result=`$script`
result2=`$script2`
IFS=':'
read -ra main_sizes_result <<< "$result"
read -ra current_sizes_result <<< "$result2"
echo ${main_sizes_result[0]} ${current_sizes_result[0]}
#if [[ ${main_sizes_result[0]} == "sizes" && ${current_sizes_result[0]} == "sizes" ]]; then
    if [ ${main_sizes_result[0]} == "sizes" ]; then

        main_github_sha=${main_sizes_result[1]} 
        main_git_commit_time=${main_sizes_result[2]} 
        main_size_bytes=${main_sizes_result[3]} 
        main_size_stripped_bytes=${main_sizes_result[4]} 
        echo $main_github_sha
        echo $main_git_commit_time
        echo $main_size_bytes
        echo $main_size_stripped_bytes
    fi 
    echo ${current_sizes_result[0]}
    if [ ${current_sizes_result[0]} == "sizes" ]; then
        current_size_bytes=${current_sizes_result[1]}
        current_size_stripped_bytes=${current_sizes_result[2]}
        echo  $current_size_bytes
        echo  $current_size_stripped_bytes
    fi
    main_size_bytes=9189230864
    current_size_bytes=9179985368
    current_pr_commit_sha=currshashashasha
    main_github_sha=mainshashashasha
    main_size_stripped_bytes=709181968
    current_size_stripped_bytes=709181968

    bytes_diff=$((current_size_bytes-main_size_bytes))
    diff_perc=$(echo "scale=3; $bytes_diff*100/$main_size_bytes" | bc | sed -r 's/^(-?)\./\10./')

    perc=$(($bytes_diff*100/$main_size_bytes))

    echo $diff_perc%
    human_readable_size=`bytesToHumanIEC $current_size_bytes`
    echo $human_readable_size
    main_url="https://github.com/ydb-platform/ydb/commit/"$main_github_sha
    
    format_number() {
        echo "$1" | rev | sed 's/\(...\)/\1 /g' | rev | xargs
    }
    formatted_number=$(format_number $current_size_bytes)

    bytes_diff=$((current_size_bytes-main_size_bytes))
    diff_perc=$(echo "scale=3; $bytes_diff*100/$main_size_bytes" | bc | sed -r 's/^(-?)\./\10./')

    main_url="https://github.com/ydb-platform/ydb/commit/"$main_github_sha
    current_url="https://github.com/ydb-platform/ydb/commit/"$current_pr_commit_sha

    human_readable_size=`bytesToHumanIEC $current_size_bytes`
    human_readable_size_diff=`bytesToHumanIEC $bytes_diff`
    
      if [ "$bytes_diff" -ge "0" ]; then
            sign="+"
            if [ "$bytes_diff" -ge "$red_treshold" ]; then
              color="red"
            elif ["$bytes_diff" -ge "$orange_treshold" ]; then
              color="orange"
            else
              color="green"
            fi
          else
            sign=""
            color="green"
          fi

          comment="[Current:${current_pr_commit_sha}]($current_url) ydbd size $human_readable_size **$sign$human_readable_size_diff $diff_perc %%**  vs build [main:${main_github_sha:0:8}]($main_url)

<details><summary>YDBD size compare details</summary>
[main:${main_github_sha:0:8}]($main_url) ydbd build size:
  - binary size $(format_number $main_size_bytes) Bytes 
  - stripped binary size $(format_number $main_size_stripped_bytes) Bytes
[Current:${current_pr_commit_sha:0:8}]($current_url) ydbd build size:
  - binary size $(format_number $current_size_bytes) Bytes 
  - stripped binary size $(format_number $current_size_stripped_bytes) Bytes

[ydbd size dashboard](https://datalens.yandex/cu6hzmpaki700)  

</details>"
       printf "${comment}" > ~/txt.txt
        #echo $comment
        

#fi

