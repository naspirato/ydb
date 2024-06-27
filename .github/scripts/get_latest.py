#!/usr/bin/env python3
import git
import datetime
from dateutil.rrule import rrule, DAILY,HOURLY,MINUTELY

def get_last_commit(repo, date):
    commits = list(repo.iter_commits(until=date))
    if commits:
        return commits[0]
    return None
# Основная логика
def main():
    repo_path = '/home/kirrysin/ydbwork/ydb/'  # Замените на путь к вашему репозиторию
    # Открытие репозитория
    repo = git.Repo(repo_path)
    start_date = datetime.datetime.strptime('2024-05-20', '%Y-%m-%d')
    # Генерация дат: каждый третий день начиная с start_date
    dates = list(rrule(DAILY, interval=3,count=40, dtstart=start_date))
    

    for date in dates:
        last_commit = get_last_commit(repo, date)
        if last_commit:
            print(f"Дата: {date.strftime('%Y-%m-%d %H:%M')} - Последний коммит: {last_commit.hexsha}")
        else:
            print(f"Дата: {date.strftime('%Y-%m-%d %H:%M')} - Коммиты не найдены")
        if (date.strftime('%Y-%m-%d')>"2024-06-06"):
            return 1


main()