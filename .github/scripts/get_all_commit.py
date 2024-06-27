
#!/usr/bin/env python3
import git
import datetime

def get_commits_for_today(repo, branch_name):
    today = datetime.datetime.now().date()
    commits = []
    for commit in repo.iter_commits(branch_name, since=today.isoformat(), until=(today + datetime.timedelta(days=1)).isoformat()):
        # Фильтрация коммитов, чтобы они попали в диапазон только сегодняшнего дня
        if commit.committed_datetime.date() == today:
            commits.append(commit)
    return commits

def main():
    repo_path = '/home/kirrysin/ydbwork/ydb/'  # Замените на путь к вашему репозиторию
    branch_name = 'main'  # Замените на имя вашей ветки, если оно отличается

    # Открытие репозитория
    try:
        repo = git.Repo(repo_path)
    except git.exc.InvalidGitRepositoryError:
        print(f"Некорректный репозиторий: {repo_path}")
        return
    
    # Получение коммитов за сегодняшний день
    commits = get_commits_for_today(repo, branch_name)
    
    if commits:
        for commit in commits:
            print(f"Коммит: {commit.hexsha} - Автор: {commit.author.name} - Дата: {commit.committed_datetime} - Сообщение: {commit.message.strip()}")
    else:
        print("За сегодняшний день коммиты не найдены.")

if __name__ == "main":
    main()