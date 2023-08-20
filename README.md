# Big_data_sixth_lab

#### Ссылка на алгоритм k-средних с pyspark — https://github.com/stpic270/Big_data_fifth_lab. 

В этом репо интегрирована cassandra с pyspark с помощью docker. 

Сначала table и keyspace создаются в cassandra с помощью cassandra-driver (библиотека python), это реализовано в create_table.py. Затем запускается алгоритм k-means и получает метки для набора данных (вы можете проверить файл use_cassandra.py). Наконец, метки с начальными features передаются в cassandra. Вы также можете проверить последовательность действий в scripts/cassandra.sh

1) После клонирования репо cначала используйте эту команду, чтобы скачать проект:
### docker compose build && docker compose up —no-start
2) Затем запустите контейнер с cassandra:
### docker start big_data_sixth_lab-cassandra-1
3) Передать ip cassandra в volume проекта:
### docker exec -t big_data_sixth_lab-cassandra-1 bash -c "echo '\n' >> config/cassandra_ip.txt && ip -4 -o address >> config/cassandra_ip.txt"
4) Запустить контейнер с моделью:
### sudo docker start big_data_sixth_lab-model-1
5) Запустите файл cassandra.sh (Cоздает keyspace и table. Также передает данные в cassandra):
### docker exec -t big_data_sixth_lab-model-1 bash -c "scripts/cassandra.sh"