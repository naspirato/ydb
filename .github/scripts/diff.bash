COMMIT1="285cc51f9451ee3981e98da5e70972f2f8873870"
COMMIT2="2bf0ef34f58ca978715e91c92eacdaf18d529c44"

COMMIT_COUNT=$(git rev-list --count ${COMMIT2}..${COMMIT1})
echo "(git rev-list --count ${COMMIT2}..${COMMIT1})"
# Вывод результата
echo "Количество коммитов между ${COMMIT1} и ${COMMIT2}: ${COMMIT_COUNT}"

