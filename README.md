# Airflow 101

## Challenge 4-5

### Challenge 4

Задание представляет собой апдейт [задания 2](https://github.com/dimk00z/airflow_2)
Было необходимо переписать ДАГ с добавлением ветвления.

Получилась следующая структура:
![alt text](Dag_structure.png)

Скрипты разбиты логически по файлам:

1. [data_collector_hw4_dag.py](https://github.com/dimk00z/airflow_4/blob/master/data_collector_hw4_dag.py) - собственно сам ДАГ и пременные-операторы

2. [data_collector_hw4_operators.py](https://github.com/dimk00z/airflow_4/blob/master/data_collector_hw4_operators.py) содержит операторы:

- `LoadOrdersOperator` - загрузка данных о заказах;
- `LoadTransactionsOperator` - загрузка данных о транзакциях;
- `LoadGoodsCustomersOperator` - загрузка данных о товарах и покупателях из PostgreSQL.

все загрузчики чистят дубли, надеюсь...

- `FinalSaveDataOperator` - сохранение в PostgreSQL;
- `TelegramErrorSendOperator` - отправка ошибки в телеграм ботом. Для корректной работы необходимо наличие .env файла, содержащего себе

`TELEGRAM_PROXY=socks5://login:pass@host (использование необязательно)`
`TELEGRAM_BOT_TOKEN='токен телеграма'`
`TELEGRAM_CHAT_ID='чат телеграма'`

3. [data_collector_hw4_check_operators.py](https://github.com/dimk00z/airflow_4/blob/master/data_collector_hw4_check_operators.py) содержит операторы:

- `PostgreCheckOperator` - проверка соединения с БД, срабатывает, когда нет соединения, не подходят логин/пароль, отсутствуют прописанные соединения в Airflow;
- `DataCheckOperator` - проверка и сбор финального дата сета.
  Выкидывает, когда на выходе получается записей меньше 70% от таблицы транзакций или в целом меньше 10 записей.
  Запись принимается некорректной, когда
  1. ФИО после чистки содержит меньше 2х слов
  2. Отстутсвует название товара
  3. Количество товаров меньше 1

4. [data_collector_hw4_utils.py](https://github.com/dimk00z/airflow_4/blob/master/data_collector_hw4_utils.py) содержат дополнительные функции сохранения/чтения данных

### Challenge 5

Для задания был добавлен класс [TelegramEventer](https://github.com/dimk00z/airflow_4/blob/master/telegram_eventer.py), который в зависимости от состояния "таски" отправляет в телеграм сообщение о состоянии.
Уведомления приходят при корректном завершении таска "all_success", а так же при перезапуске и неудачах выполнения.

Так же был добавлен ДАГ [hw_5_yandex_checker](https://github.com/dimk00z/airflow_4/blob/master/yandex_check.py), который рандомно собирает ya.ru с вероятностью ошибки приверно 33% для проверки работоспособности отправки состояний в телеграм.
Для дага прописаны коллбэки на статусы и sla, но вызов через `sla_miss_callback` отловить так и не получилось, хотя в вэбе видны срабатывания :(

На [сервере](http://34.90.21.114/grafana) подняты Grafana и Prometheus.

### Цель проекта

Код написан в образовательных целях на онлайн-курсе [Airflow 101](https://airflow101.python-jitsu.club/).
